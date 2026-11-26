// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/frostbyte73/core"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"
	protoutils "github.com/livekit/protocol/utils"
	"github.com/livekit/psrpc"

	"github.com/livekit/ingress/pkg/errors"
	"github.com/livekit/ingress/pkg/ipc"
	"github.com/livekit/ingress/pkg/params"
	"github.com/livekit/ingress/pkg/utils"
)

const (
	maxRetries = 3

	// KillIngress waits for the pipeline to finish, so a healthy handler is long
	// gone by then. One still running is hung and would otherwise keep its
	// session open until the pod dies.
	killGracePeriod = 30 * time.Second
)

type process struct {
	sessionCloser

	params           *params.Params
	retryCount       int
	grpcClient       atomic.Pointer[ipc.IngressHandlerClient]
	ipcHandlerClient *ipc.IngressHandlerClientWrapper
	ipcServiceServer *grpc.Server
	rpcServer        rpc.IngressHandlerServer
	closed           core.Fuse
	proc             atomic.Pointer[os.Process]
	processMu        sync.Mutex
	stopRequested    bool
	stopSignal       os.Signal
	killArmed        core.Fuse
	forceKilled      atomic.Bool
	lastInfo         atomic.Pointer[livekit.IngressInfo]
}

type ProcessManager struct {
	ipc.UnimplementedIngressServiceServer

	sm            *SessionManager
	stateNotifier utils.StateNotifier
	bus           psrpc.MessageBus
	newCmd        func(ctx context.Context, p *params.Params) (*exec.Cmd, error)

	mu             sync.RWMutex
	activeHandlers map[string]*process
	starting       int
	stopping       bool
	onFatal        func(p *params.Params, err error)
}

func NewProcessManager(sm *SessionManager, stateNotifier utils.StateNotifier, bus psrpc.MessageBus, newCmd func(ctx context.Context, p *params.Params) (*exec.Cmd, error)) (*ProcessManager, error) {
	pm := &ProcessManager{
		sm:             sm,
		bus:            bus,
		stateNotifier:  stateNotifier,
		newCmd:         newCmd,
		activeHandlers: make(map[string]*process),
	}

	return pm, nil
}

func (s *ProcessManager) onFatalError(f func(p *params.Params, err error)) {
	s.onFatal = f
}

func (s *ProcessManager) startIngress(ctx context.Context, p *params.Params, closeSession func(ctx context.Context)) error {
	// TODO send update on failure
	_, span := tracer.Start(ctx, "Service.startIngress")
	defer span.End()

	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return errors.ErrServerShuttingDown
	}
	s.starting++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.starting--
		s.mu.Unlock()
	}()

	h := &process{
		params: p,
		sessionCloser: func(ctx context.Context) {
			if closeSession != nil {
				closeSession(ctx)
			}

			go func() {
				time.Sleep(time.Second)

				s.mu.Lock()
				h := s.activeHandlers[p.State.ResourceId]
				s.mu.Unlock()

				if h != nil && !h.closed.IsBroken() {
					logger.Infow("killing handler process still present after termination was requested", "ingressID", h.params.IngressId, "resourceID", p.State.ResourceId, "startedAt", p.State.StartedAt)
					if err := h.requestStop(syscall.SIGKILL); err != nil {
						logger.Infow("failed to kill process", "error", err, "ingressID", h.params.IngressId)
					}
				}
			}()
		},
	}

	if p.TmpDir != "" {
		err := os.MkdirAll(p.TmpDir, 0755)
		if err != nil {
			logger.Errorw("failed creating handler temp directory", err, "path", p.TmpDir)
			return err
		}
	}

	// on any startup failure below, remove the temp directory and the sockets
	// it contains; on success runHandler owns the cleanup
	started := false
	defer func() {
		if !started && p.TmpDir != "" {
			os.RemoveAll(p.TmpDir)
		}
	}()

	// create the IPC endpoints before advertising the RPC topics so that
	// requests dispatched during handler startup can always resolve a client
	ipcServiceServer, err := ipc.StartServiceServer(p.TmpDir, s)
	if err != nil {
		return err
	}
	h.ipcServiceServer = ipcServiceServer

	ipcHandlerClient, err := ipc.GetHandlerClient(p.TmpDir)
	if err != nil {
		ipcServiceServer.Stop()
		return err
	}
	h.ipcHandlerClient = ipcHandlerClient
	h.grpcClient.Store(&ipcHandlerClient.IngressHandlerClient)

	rpcServer, err := rpc.NewIngressHandlerServer(h, s.bus, psrpc.WithServerSkipClaim(p.SkipClaimEnabled))
	if err != nil {
		ipcHandlerClient.Close()
		ipcServiceServer.Stop()
		return err
	}
	h.rpcServer = rpcServer

	if err := utils.RegisterIngressRpcHandlers(rpcServer, p.IngressInfo); err != nil {
		utils.DeregisterIngressRpcHandlers(rpcServer, p.IngressInfo)
		ipcHandlerClient.Close()
		ipcServiceServer.Stop()
		return err
	}

	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		utils.DeregisterIngressRpcHandlers(h.rpcServer, p.IngressInfo)
		h.ipcHandlerClient.Close()
		h.ipcServiceServer.Stop()
		return errors.ErrServerShuttingDown
	}
	s.activeHandlers[p.State.ResourceId] = h
	s.mu.Unlock()

	s.sm.IngressStarted(p.IngressInfo, h)
	s.stateNotifier.SessionStarted(ctx, p.ProjectID, p.IngressInfo)

	go s.runHandler(ctx, h, p)
	started = true

	return nil
}

func (s *ProcessManager) runHandler(ctx context.Context, h *process, p *params.Params) {
	_, span := tracer.Start(ctx, "Service.runHandler")
	defer span.End()

	defer func() {
		h.processMu.Lock()
		h.closed.Break()
		h.proc.Store(nil)
		h.processMu.Unlock()

		if h.forceKilled.Load() {
			s.publishKilledState(context.WithoutCancel(ctx), h)
		}
		// Release before the session leaves the session manager. Shutdown waits
		// on IsIdle, which IngressEnded satisfies, and these two calls are not
		// atomic -- releasing afterwards lets the process exit in between.
		s.stateNotifier.SessionEnded(context.WithoutCancel(ctx), h.params.State.ResourceId)
		s.sm.IngressEnded(h.params.State.ResourceId)

		utils.DeregisterIngressRpcHandlers(h.rpcServer, p.IngressInfo)
		h.ipcHandlerClient.Close()
		h.ipcServiceServer.Stop()

		if p.TmpDir != "" {
			os.RemoveAll(p.TmpDir)
		}

		s.mu.Lock()
		delete(s.activeHandlers, h.params.State.ResourceId)
		s.mu.Unlock()
	}()

	for h.retryCount = 0; h.retryCount < maxRetries; h.retryCount++ {
		retry, _ := s.runHandlerTry(ctx, h, p)
		if !retry {
			return
		}

	}
}

// publishKilledState sends the final update that a killed handler could not.
func (s *ProcessManager) publishKilledState(ctx context.Context, h *process) {
	info := h.lastInfo.Load()
	if info == nil {
		info = h.params.CopyInfo()
	}

	now := time.Now().UnixNano()
	info.State.Status = livekit.IngressState_ENDPOINT_ERROR
	info.State.Error = errors.ErrHandlerKilled.Error()
	if info.State.EndedAt == 0 {
		info.State.EndedAt = now
	}
	info.State.UpdatedAt = now

	if err := s.stateNotifier.UpdateIngressState(ctx, h.params.ProjectID, info); err != nil {
		logger.Errorw("failed to send update", err, "ingressID", h.params.IngressId)
	}
}

func (s *ProcessManager) runHandlerTry(ctx context.Context, h *process, p *params.Params) (bool, error) {
	_, span := tracer.Start(ctx, "Service.runHandlerTry")
	defer span.End()

	cmd, err := s.newCmd(ctx, p)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	var exitErr *exec.ExitError

	err = cmd.Start()
	if err == nil {
		pid := cmd.Process.Pid

		// Shutdown may race cmd.Start. Publish the process while holding the
		// lifecycle lock so a concurrent stop either sees it or records a
		// pending signal that is delivered here.
		h.processMu.Lock()
		h.proc.Store(cmd.Process)
		var signalErr error
		if h.stopRequested && !h.closed.IsBroken() {
			signalErr = cmd.Process.Signal(h.stopSignal)
			if signalErr == nil && h.stopSignal == syscall.SIGKILL {
				h.forceKilled.Store(true)
			}
		}
		h.processMu.Unlock()
		if signalErr != nil {
			logger.Infow("failed to stop handler after startup", "error", signalErr, "ingressID", h.params.IngressId)
		}

		s.sm.HandlerStarted(h.params.IngressInfo, pid)
		err = cmd.Wait()
		h.processMu.Lock()
		h.proc.CompareAndSwap(cmd.Process, nil)
		h.processMu.Unlock()
		// Wait returns only after the output copy has finished.
		for _, w := range []io.Writer{cmd.Stdout, cmd.Stderr} {
			if l, ok := w.(*utils.HandlerLogger); ok {
				_ = l.Close()
			}
		}
		s.sm.HandlerExited(pid, cmd.ProcessState)
	}
	switch {
	case err == nil:
		// success
		return false, nil
	case errors.As(err, &exitErr):
		if exitErr.ExitCode() == 1 {
			logger.Infow("relaunching handler process after retryable failure")
			return true, err
		}
		if err.Error() == "signal: killed" {
			logger.Infow("handler killed")
			return false, err
		}
		logger.Errorw("unknown handler exit code", err)
		return false, err

	default:
		logger.Errorw("could not launch handler", err)
		if s.onFatal != nil {
			s.onFatal(h.params, err)
		}
		return false, err
	}
}

func (s *ProcessManager) shutdown(kill bool) {
	s.mu.Lock()
	s.stopping = true
	handlers := make([]*process, 0, len(s.activeHandlers))
	if kill {
		for _, h := range s.activeHandlers {
			handlers = append(handlers, h)
		}
	}
	s.mu.Unlock()

	for _, h := range handlers {
		h.killAfter(killGracePeriod)
		if err := h.requestStop(syscall.SIGINT); err != nil {
			logger.Errorw("failed to kill process", err, "ingressID", h.params.IngressId)
		}
	}
}

func (s *ProcessManager) IsIdle() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.activeHandlers) == 0 && s.starting == 0
}

func (p *process) requestStop(sig os.Signal) error {
	p.processMu.Lock()
	defer p.processMu.Unlock()

	if p.closed.IsBroken() {
		return nil
	}
	if !p.stopRequested || sig == syscall.SIGKILL {
		p.stopSignal = sig
	}
	p.stopRequested = true

	proc := p.proc.Load()
	if proc == nil {
		return nil
	}
	err := proc.Signal(p.stopSignal)
	if err == nil && p.stopSignal == syscall.SIGKILL {
		p.forceKilled.Store(true)
	}
	return err
}

func (s *ProcessManager) UpdateIngressState(ctx context.Context, req *ipc.UpdateIngressStateRequest) (*emptypb.Empty, error) {
	s.sm.HandlerStateUpdated(req.Info)

	s.mu.RLock()
	h := s.activeHandlers[req.Info.GetState().GetResourceId()]
	s.mu.RUnlock()
	if h != nil {
		h.lastInfo.Store(protoutils.CloneProto(req.Info))
	}

	return &emptypb.Empty{}, s.stateNotifier.UpdateIngressState(ctx, req.ProjectId, req.Info)
}

// killAfter SIGKILLs the handler if it is still running after termination was
// requested. Only the first request arms it.
func (p *process) killAfter(d time.Duration) {
	if !p.killArmed.Break() {
		return
	}

	time.AfterFunc(d, func() {
		p.processMu.Lock()
		if p.closed.IsBroken() {
			p.processMu.Unlock()
			return
		}
		proc := p.proc.Load()
		if proc == nil {
			// The escalation deadline can expire while a handler is being
			// constructed or between retries. Preserve SIGKILL for the next
			// runHandlerTry instead of losing escalation when no process is published.
			p.stopRequested = true
			p.stopSignal = syscall.SIGKILL
			p.processMu.Unlock()
			return
		}
		logger.Warnw("killing handler still running after termination was requested", nil,
			"ingressID", p.params.IngressId, "resourceID", p.params.State.ResourceId, "gracePeriod", d)
		err := proc.Signal(syscall.SIGKILL)
		if err == nil {
			p.forceKilled.Store(true)
		}
		p.processMu.Unlock()
		if err != nil {
			logger.Infow("failed to kill process", "error", err, "ingressID", p.params.IngressId)
		}
	})
}

func (p *process) UpdateIngress(ctx context.Context, _ *livekit.UpdateIngressRequest) (*livekit.IngressState, error) {
	return p.killIngress(ctx)
}

func (p *process) DeleteIngress(ctx context.Context, _ *livekit.DeleteIngressRequest) (*livekit.IngressState, error) {
	return p.killIngress(ctx)
}

func (p *process) DeleteWHIPResource(ctx context.Context, _ *rpc.DeleteWHIPResourceRequest) (*emptypb.Empty, error) {
	if _, err := p.killIngress(ctx); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (p *process) killIngress(ctx context.Context) (*livekit.IngressState, error) {
	p.killAfter(killGracePeriod)

	grpcClient := p.grpcClient.Load()
	if grpcClient == nil {
		return nil, errors.ErrIngressNotFound
	}

	resp, err := (*grpcClient).KillIngress(ctx, &ipc.KillIngressRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return resp.State, nil
}

func (p *process) ICERestartWHIPResource(_ context.Context, _ *rpc.ICERestartWHIPResourceRequest) (*rpc.ICERestartWHIPResourceResponse, error) {
	return &rpc.ICERestartWHIPResourceResponse{}, psrpc.NewErrorf(psrpc.UnprocessableEntity, "Trickle-ICE and ICE restart not supported")
}

func (p *process) WHIPRTCConnectionNotify(_ context.Context, _ *rpc.WHIPRTCConnectionNotifyRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (p *process) GetProfileData(ctx context.Context, profileName string, timeout int32, debug int32) (b []byte, err error) {
	grpcClient := p.grpcClient.Load()
	if grpcClient == nil {
		return nil, errors.ErrIngressNotFound
	}

	req := &ipc.PProfRequest{
		ProfileName: profileName,
		Timeout:     int32(timeout),
		Debug:       int32(debug),
	}

	resp, err := (*grpcClient).GetPProf(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.PprofFile, nil
}

func (p *process) GetPipelineDot(ctx context.Context) (string, error) {
	grpcClient := p.grpcClient.Load()
	if grpcClient == nil {
		return "", errors.ErrIngressNotFound
	}

	req := &ipc.GstPipelineDebugDotRequest{}

	resp, err := (*grpcClient).GetPipelineDot(ctx, req)
	if err != nil {
		return "", err
	}

	return resp.DotFile, nil
}

func (p *process) UpdateMediaStats(ctx context.Context, s *ipc.MediaStats) error {
	grpcClient := p.grpcClient.Load()
	if grpcClient == nil {
		return errors.ErrIngressNotFound
	}

	req := &ipc.UpdateMediaStatsRequest{
		Stats: s,
	}

	_, err := (*grpcClient).UpdateMediaStats(ctx, req)

	return err
}

func (p *process) GatherStats(ctx context.Context) (*ipc.MediaStats, error) {
	grpcClient := p.grpcClient.Load()
	if grpcClient == nil {
		return nil, errors.ErrIngressNotFound
	}

	req := &ipc.GatherMediaStatsRequest{}

	s, err := (*grpcClient).GatherMediaStats(ctx, req)
	if err != nil {
		return nil, err
	}

	return s.Stats, nil
}

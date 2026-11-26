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

package whip

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/livekit/mediatransportutil/pkg/rtcconfig"

	"github.com/livekit/ingress/pkg/config"
	"github.com/livekit/ingress/pkg/errors"
	"github.com/livekit/ingress/pkg/params"
	"github.com/livekit/ingress/pkg/stats"
	"github.com/livekit/ingress/pkg/types"

	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/utils"
	"github.com/livekit/psrpc"
)

const (
	maxWHIPRequestBytes = 1 << 20
	sdpResponseTimeout  = 5 * time.Second
	sessionStartTimeout = 10 * time.Second
	rpcTimeout          = 5 * time.Second
)

type HealthHandlers map[string]http.HandlerFunc

type WHIPServer struct {
	ctx    context.Context
	cancel context.CancelFunc

	conf         *config.Config
	webRTCConfig *rtcconfig.WebRTCConfig
	onPublish    func(streamKey, resourceId string) (*params.Params, func(mimeTypes map[types.StreamKind]string, err error) *stats.LocalMediaStatsGatherer, func(error), error)
	bus          psrpc.MessageBus
	rpcClient    rpc.IngressHandlerClient

	handlersLock sync.Mutex
	handlers     map[string]WHIPHandler

	// newHandler builds the handler for a new session. Tests replace it.
	newHandler func(p *params.Params, streamKey, resourceId, ua string) (WHIPHandler, error)

	monitor *stats.Monitor
}

func (s *WHIPServer) SetMonitor(monitor *stats.Monitor) {
	s.monitor = monitor
}

type WHIPInitResponse struct {
	SDPAnswer   string
	LinkHeaders []string
	ETag        string
}

type WHIPHandler interface {
	Init(ctx context.Context, sdpOffer string) (*WHIPInitResponse, error)
	SetMediaStatsGatherer(st *stats.LocalMediaStatsGatherer)
	Start(ctx context.Context) (map[types.StreamKind]string, error)
	WaitForSessionEnd(ctx context.Context) error
	Close()
	AssociateRelay(kind types.StreamKind, token string, w io.WriteCloser) error
	DissociateRelay(kind types.StreamKind)
}

type readyRunner interface {
	runReady(func()) bool
}

func invokeReady(
	h WHIPHandler,
	ready func(map[types.StreamKind]string, error) *stats.LocalMediaStatsGatherer,
	mimeTypes map[types.StreamKind]string,
	err error,
) (*stats.LocalMediaStatsGatherer, bool) {
	if ready == nil {
		return nil, true
	}
	var result *stats.LocalMediaStatsGatherer
	call := func() { result = ready(mimeTypes, err) }
	if runner, ok := h.(readyRunner); ok {
		return result, runner.runReady(call)
	}
	call()
	return result, true
}

func NewWHIPServer(bus psrpc.MessageBus) (*WHIPServer, error) {
	psrpcWHIPClient, err := rpc.NewIngressHandlerClient(bus)
	if err != nil {
		return nil, err
	}

	s := &WHIPServer{
		bus:       bus,
		rpcClient: psrpcWHIPClient,
		handlers:  make(map[string]WHIPHandler),
	}
	s.newHandler = s.createHandler

	return s, nil
}

func (s *WHIPServer) Start(
	conf *config.Config,
	onPublish func(streamKey, resourceId string) (*params.Params, func(mimeTypes map[types.StreamKind]string, err error) *stats.LocalMediaStatsGatherer, func(error), error),
	healthHandlers HealthHandlers,
) error {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	logger.Infow("starting WHIP server")

	if onPublish == nil {
		return psrpc.NewErrorf(psrpc.Internal, "no onPublish callback provided")
	}

	s.onPublish = onPublish
	s.conf = conf

	var err error
	s.webRTCConfig, err = rtcconfig.NewWebRTCConfig(&conf.RTCConfig, conf.Development)
	if err != nil {
		return err
	}

	r := mux.NewRouter()

	r.HandleFunc("/{app}", func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer func() {
			s.handleError(err, w)
		}()

		bearer := r.Header.Get("Authorization")
		// OBS adds the 'Bearer' prefix as expected, but some other clients do not
		streamKey := strings.TrimPrefix(bearer, "Bearer ")

		err = s.handleNewWhipClient(w, r, streamKey)
	}).Methods("POST")

	r.HandleFunc("/{app}/{stream_key}", func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer func() {
			s.handleError(err, w)
		}()

		streamKey := mux.Vars(r)["stream_key"]

		err = s.handleNewWhipClient(w, r, streamKey)
	}).Methods("POST")

	r.HandleFunc("/{app}", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, r, false)
		w.WriteHeader(http.StatusNoContent)
	}).Methods("OPTIONS")

	r.HandleFunc("/{app}/{stream_key}", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, r, false)
		w.WriteHeader(http.StatusNoContent)
	}).Methods("OPTIONS")

	// End
	r.HandleFunc("/{app}/{stream_key}/{resource_id}", func(w http.ResponseWriter, r *http.Request) {
		var err error
		defer func() {
			s.handleError(err, w)
		}()

		vars := mux.Vars(r)
		streamKey := vars["stream_key"]
		resourceID := vars["resource_id"]

		logger.Infow("handling WHIP delete request", "resourceID", resourceID)

		req := &rpc.DeleteWHIPResourceRequest{
			ResourceId: resourceID,
			StreamKey:  streamKey,
		}

		w.Header().Set("Access-Control-Allow-Origin", "*")

		_, err = s.rpcClient.DeleteWHIPResource(s.ctx, resourceID, req, psrpc.WithRequestTimeout(5*time.Second))
		if err == psrpc.ErrNoResponse {
			err = errors.ErrIngressNotFound
		}
	}).Methods("DELETE")

	// Trickle, ICE Restart unimplemented for now
	r.HandleFunc("/{app}/{stream_key}/{resource_id}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		streamKey := vars["stream_key"]
		resourceID := vars["resource_id"]

		logger.Infow("handling ICE Restart request", "resourceID", resourceID)
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Expose-Headers", "ETag")

		body, err := readWHIPRequestBody(w, r)
		if err != nil {
			logger.Infow("WHIP ICE Restart failed to read body", "error", err, "streamKey", streamKey, "resourceID", resourceID)
			var committedErr *responseCommittedHTTPError
			if !errors.As(err, &committedErr) {
				s.handleError(errors.ErrInvalidWHIPRestartRequest, w)
			}
			return
		}

		// Only extract the ufrag/pwd and candidates from the request
		// "WHIP does not support renegotiation of non-ICE related SDP information"
		//
		// https://www.ietf.org/archive/id/draft-ietf-wish-whip-14.html#name-ice-restarts
		userFragment, password, err := extractICEDetails(body)
		if err != nil {
			logger.Infow("WHIP ICE Restart failed to unmarshal SDP", "error", err, "streamKey", streamKey, "resourceID", resourceID)
			s.handleError(errors.ErrInvalidWHIPRestartRequest, w)
			return
		}

		if userFragment == "" || password == "" {
			logger.Infow("WHIP ICE Restart failed to extract ice-ufrag/ice-pwd", "error", err, "streamKey", streamKey, "resourceID", resourceID)
			s.handleError(errors.ErrInvalidWHIPRestartRequest, w)
			return
		}

		resp, err := s.rpcClient.ICERestartWHIPResource(s.ctx, resourceID, &rpc.ICERestartWHIPResourceRequest{
			UserFragment:         userFragment,
			Password:             password,
			ResourceId:           resourceID,
			StreamKey:            streamKey,
			RawTrickleIceSdpfrag: string(body),
			IfMatch:              r.Header.Get("If-Match"),
		}, psrpc.WithRequestTimeout(5*time.Second))
		if err != nil {
			s.handleError(err, w)
			logger.Infow("WHIP ICE restart failed", "error", err, "streamKey", streamKey, "resourceID", resourceID)
			return
		}

		w.Header().Set("Content-Type", "application/trickle-ice-sdpfrag")
		if resp.Etag != "" {
			w.Header().Set("ETag", resp.Etag)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(resp.TrickleIceSdpfrag))

	}).Methods("PATCH")

	r.HandleFunc("/{app}/{stream_key}/{resource_id}", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, r, true)
	}).Methods("OPTIONS")

	// Expose the health endpoints on the WHIP server as well to make
	// deployment as a k8s ingress more straightforward
	for path, handler := range healthHandlers {
		r.HandleFunc(path, handler).Methods("GET")
	}

	hs := &http.Server{
		Addr:         fmt.Sprintf(":%d", conf.WHIPPort),
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		err := hs.ListenAndServe()
		if err != http.ErrServerClosed {
			logger.Errorw("WHIP server start failed", err)
		}
	}()

	return nil
}

func (s *WHIPServer) CloseHandler(resourceId string) {
	s.handlersLock.Lock()
	h, ok := s.handlers[resourceId]
	s.handlersLock.Unlock()

	if ok && h != nil {
		h.Close()
	}
}

func (s *WHIPServer) Stop() {
	s.cancel()
}

func (s *WHIPServer) AssociateRelay(resourceId string, kind types.StreamKind, token string, w io.WriteCloser) error {
	s.handlersLock.Lock()
	h, ok := s.handlers[resourceId]
	s.handlersLock.Unlock()
	if !ok || h == nil {
		return errors.ErrIngressNotFound
	}

	return h.AssociateRelay(kind, token, w)
}

func (s *WHIPServer) DissociateRelay(resourceId string, kind types.StreamKind) {
	s.handlersLock.Lock()
	h, ok := s.handlers[resourceId]
	s.handlersLock.Unlock()
	if ok && h != nil {
		h.DissociateRelay(kind)
	}
}

func (s *WHIPServer) IsIdle() bool {
	s.handlersLock.Lock()
	defer s.handlersLock.Unlock()

	return len(s.handlers) == 0
}

func (s *WHIPServer) handleError(err error, w http.ResponseWriter) {
	var psrpcErr psrpc.Error
	var committedErr *responseCommittedHTTPError
	var canceledErr *requestCanceledHTTPError
	switch {
	case errors.As(err, &psrpcErr):
		w.WriteHeader(psrpcErr.ToHttp())
		_, _ = w.Write([]byte(psrpcErr.Error()))
	case errors.As(err, &committedErr):
		logger.Debugw("WHIP response was already committed", "error", err)
	case errors.As(err, &canceledErr):
		logger.Debugw("WHIP request was canceled before its response", "error", err)
	case err == nil:
		// Nothing, we already responded
	default:
		logger.Debugw("whip request failed", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

type responseCommittedHTTPError struct {
	cause error
}

type requestCanceledHTTPError struct {
	cause error
}

func (e *requestCanceledHTTPError) Error() string {
	return fmt.Sprintf("WHIP request canceled before response: %v", e.cause)
}

func (e *requestCanceledHTTPError) Unwrap() error {
	return e.cause
}

func readWHIPRequestBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWHIPRequestBytes))
	if err == nil {
		return body, nil
	}

	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		http.Error(w, "WHIP request body exceeds the 1 MiB limit", http.StatusRequestEntityTooLarge)
		return nil, &responseCommittedHTTPError{cause: err}
	}
	return nil, err
}

func (e *responseCommittedHTTPError) Error() string {
	return fmt.Sprintf("WHIP response already committed: %v", e.cause)
}

func (e *responseCommittedHTTPError) Unwrap() error {
	return e.cause
}

func (s *WHIPServer) handleNewWhipClient(w http.ResponseWriter, r *http.Request, streamKey string) (err error) {
	// TODO return ETAG header

	defer func() {
		s.monitor.RecordPublicationResult("whip", err)
	}()

	vars := mux.Vars(r)
	app := vars["app"]

	sdpOffer := bytes.Buffer{}

	body, err := readWHIPRequestBody(w, r)
	if err != nil {
		return err
	}
	_, _ = sdpOffer.Write(body)

	logger.Debugw("new whip request", "streamKey", streamKey, "sdpOffer", sdpOffer.String(), "userAgent", r.Header.Get("User-Agent"))

	resourceId, res, abortSession, err := s.createStream(r.Context(), streamKey, sdpOffer.String(), r.Header.Get("User-Agent"))
	if err != nil {
		return err
	}
	responseComplete := make(chan struct{})
	var responseMu sync.Mutex
	responseDelivered := false
	responseAborted := false
	go func() {
		select {
		case <-r.Context().Done():
			responseMu.Lock()
			shouldAbort := !responseDelivered && !responseAborted
			if shouldAbort {
				responseAborted = true
			}
			responseMu.Unlock()
			if shouldAbort {
				abortSession()
			}
		case <-responseComplete:
		}
	}()
	defer close(responseComplete)

	responseMu.Lock()
	if responseAborted || r.Context().Err() != nil {
		cancelErr := r.Context().Err()
		if cancelErr == nil {
			cancelErr = context.Canceled
		}
		responseAborted = true
		responseMu.Unlock()
		abortSession()
		return &requestCanceledHTTPError{cause: cancelErr}
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "Location, Link, ETag")
	w.Header().Set("Content-Type", "application/sdp")
	w.Header().Set("Location", fmt.Sprintf("/%s/%s/%s", app, streamKey, resourceId))
	etag := res.ETag
	if etag == "" {
		etag = fmt.Sprintf("\"%s\"", resourceId)
	}
	w.Header().Set("ETag", etag)
	for _, l := range res.LinkHeaders {
		w.Header().Add("Link", l)
	}
	w.WriteHeader(http.StatusCreated)
	written, writeErr := io.WriteString(w, res.SDPAnswer)
	if writeErr == nil && written != len(res.SDPAnswer) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		if flushErr := http.NewResponseController(w).Flush(); flushErr != nil && !errors.Is(flushErr, http.ErrNotSupported) {
			writeErr = flushErr
		}
	}
	if writeErr == nil {
		// Commit successful delivery before the request context is released. The
		// server cancels that context when this handler returns, which must not
		// tear down a WHIP session whose 201 response was already flushed.
		responseDelivered = true
	} else {
		responseAborted = true
	}
	responseMu.Unlock()
	if writeErr != nil {
		abortSession()
		return &responseCommittedHTTPError{cause: writeErr}
	}

	return nil
}

func (s *WHIPServer) createStream(requestCtx context.Context, streamKey string, sdpOffer string, ua string) (string, *WHIPInitResponse, func(), error) {
	ctx, done := context.WithTimeout(s.ctx, sdpResponseTimeout)
	stopRequestCancel := context.AfterFunc(requestCtx, done)
	defer stopRequestCancel()
	defer done()

	resourceId := utils.NewGuid(utils.WHIPResourcePrefix)

	p, ready, ended, err := s.onPublish(streamKey, resourceId)
	if err != nil {
		return "", nil, nil, err
	}

	h, err := s.newHandler(p, streamKey, resourceId, ua)
	if err != nil {
		// The caller is handed ready and ended together and expects one of
		// them; returning without either strands whatever it set up.
		ready(nil, err)
		return "", nil, nil, err
	}

	sdpResponse, err := h.Init(ctx, sdpOffer)
	if err != nil {
		invokeReady(h, ready, nil, err)

		h.Close()
		return "", nil, nil, err
	}

	sessionCtx, cancelSession := context.WithTimeout(s.ctx, sessionStartTimeout)
	go func() {
		defer cancelSession()

		s.handlersLock.Lock()
		s.handlers[resourceId] = h
		s.handlersLock.Unlock()

		mimeTypes, err := h.Start(sessionCtx)

		// Call ready here rather than on the way out. The goroutine below
		// calls ended, and a session whose done channel is already broken
		// reaches ended without blocking, so deferring ready lets ended
		// overtake it -- ready sends a state update, and the round trip is
		// long enough for the whole of ended to run in the middle of it.
		readyStats, readyInvoked := invokeReady(h, ready, mimeTypes, err)
		if !readyInvoked {
			h.Close()

			s.handlersLock.Lock()
			delete(s.handlers, resourceId)
			s.handlersLock.Unlock()

			return
		}
		if readyStats != nil {
			h.SetMediaStatsGatherer(readyStats)
		}

		if err != nil {
			h.Close()

			s.handlersLock.Lock()
			delete(s.handlers, resourceId)
			s.handlersLock.Unlock()

			return
		}

		logger.Infow("all tracks ready")

		go func() {
			var err error
			defer func() {
				h.Close()
				s.handlersLock.Lock()
				delete(s.handlers, resourceId)
				s.handlersLock.Unlock()

				if err != nil {
					logger.Warnw("WHIP session failed", err, "streamKey", streamKey, "resourceID", resourceId)
				}

				if ended != nil {
					ended(err)
				}
			}()

			err = h.WaitForSessionEnd(s.ctx)
		}()
	}()

	var abortOnce sync.Once
	abortSession := func() {
		abortOnce.Do(func() {
			cancelSession()
			h.Close()
		})
	}
	return resourceId, sdpResponse, abortSession, nil
}

func (s *WHIPServer) createHandler(p *params.Params, streamKey, resourceId, ua string) (WHIPHandler, error) {
	if *p.EnableTranscoding {
		logger.Infow("Using native WHIP handler", "ingressID", p.IngressId, "resourceID", resourceId, "streamKey", streamKey)
		return newWHIPHandler(p, s.webRTCConfig), nil
	}

	logger.Infow("Using proxied WHIP handler", "ingressID", p.IngressId, "resourceID", resourceId, "streamKey", streamKey)
	return NewProxyWHIPHandler(p, s.bus, ua)
}

func setCORSHeaders(w http.ResponseWriter, _ *http.Request, resourceEndpoint bool) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	if resourceEndpoint {
		w.Header().Set("Access-Control-Allow-Methods", "PATCH, OPTIONS, DELETE")
	} else {
		w.Header().Set("Accept-Post", "application/sdp")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Expose-Headers", "Location, Link, ETag")
	}
}

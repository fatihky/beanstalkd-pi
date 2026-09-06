package main

import (
	"context"
	"net/http"
	"net/http/pprof"
	"time"
)

// serveAdmin starts the observability HTTP server (/metrics, /healthz,
// and optionally /debug/pprof/*) and blocks until it shuts down. It is
// meant to be run in its own goroutine. Call s.shutdownAdmin during
// server shutdown to stop it gracefully.
func (s *Server) serveAdmin(addr string, enablePprof bool) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/healthz", s.handleHealthz)

	if enablePprof {
		// Registered explicitly (rather than via the usual
		// `import _ "net/http/pprof"`) so these endpoints only exist
		// on our own mux, and only when the flag is set - pprof
		// exposes stack traces, goroutine dumps, and profiling that
		// shouldn't be reachable by default in production.
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		logger.Warn("pprof debug endpoints enabled", "addr", addr)
	}

	httpSrv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 30 * time.Second, // pprof profile/trace can run up to 30s
	}

	s.mu.Lock()
	s.adminServer = httpSrv
	s.mu.Unlock()

	logger.Info("observability http server listening", "addr", addr, "pprof", enablePprof)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("observability http server error", "err", err)
	}
}

// shutdownAdmin gracefully stops the observability HTTP server, if one
// is running.
func (s *Server) shutdownAdmin() {
	s.mu.Lock()
	httpSrv := s.adminServer
	s.mu.Unlock()

	if httpSrv == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		logger.Error("observability http server shutdown error", "err", err)
	}
}

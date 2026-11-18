package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func newAPIServer(h http.Handler) *http.Server {
	// request contexts derive from baseCtx, so cancelling it on shutdown ends open SSE streams;
	// otherwise Shutdown would wait for every stream to finish on its own
	baseCtx, cancelBase := context.WithCancel(context.Background())

	api := &http.Server{
		Addr:              ":8080",
		Handler:           h,
		ReadHeaderTimeout: 2 * time.Second,
		IdleTimeout:       60 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
	}
	api.RegisterOnShutdown(cancelBase)
	return api
}

// expose :9090 only to Prometheus (firewall, k8s policy, etc)
func newMetricsServer() *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	return &http.Server{
		Addr:              ":9090",
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
	}
}

// pprof is bound to loopback only: profiles leak internals and /profile burns CPU,
// so reach it from outside via an SSH tunnel (ssh -L 6060:localhost:6060 host)
func newPprofServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
	}
}

// serve runs all servers until ctx is cancelled (then shuts them down) or one of them fails to start
func serve(ctx context.Context, servers []*http.Server) error {
	errc := make(chan error, len(servers))
	for _, srv := range servers {
		go func() {
			log.Printf("listening on %s", srv.Addr)
			errc <- srv.ListenAndServe()
		}()
	}

	select {
	case err := <-errc:
		// a server failed to start, e.g. the port is taken
		return err
	case <-ctx.Done():
	}

	log.Print("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// if API shuts down first metrics still can be collected
	for _, srv := range servers {
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown %s: %v", srv.Addr, err)
		}
	}
	for range servers {
		if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
			log.Print(err)
		}
	}
	return nil
}

package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/KYS3K/tensorgate/internal/gateway"
	"github.com/KYS3K/tensorgate/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	pprofAddr := flag.String("pprof", "127.0.0.1:6060", `pprof listen address, "" disables it`)
	flushInterval := flag.Duration("flush-interval", 100*time.Millisecond,
		"batch each stream's tokens into at most one write per interval, 0 writes every token at once")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	m := metrics.New(prometheus.DefaultRegisterer)

	mux := http.NewServeMux()
	mux.Handle("GET /events", gateway.NewStreamer())
	completions := gateway.NewCompletionsHandler(m)
	completions.FlushInterval = *flushInterval
	mux.Handle("POST /v1/chat/completions", completions)

	// request contexts derive from baseCtx, so cancelling it on shutdown ends open SSE streams;
	// otherwise Shutdown would wait for every stream to finish on its own
	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	api := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		IdleTimeout:       60 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
	}
	api.RegisterOnShutdown(cancelBase)

	// expose :9090 only to Prometheus (firewall, k8s policy, etc)
	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", promhttp.Handler())
	metricsSrv := &http.Server{
		Addr:              ":9090",
		Handler:           metricsMux,
		ReadHeaderTimeout: 2 * time.Second,
	}

	// pprof is bound to loopback only: profiles leak internals and /profile burns CPU,
	// so reach it from outside via an SSH tunnel (ssh -L 6060:localhost:6060 host)
	pprofMux := http.NewServeMux()
	pprofMux.HandleFunc("/debug/pprof/", pprof.Index)
	pprofMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	pprofMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	pprofMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	pprofMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	servers := []*http.Server{api, metricsSrv}
	if *pprofAddr != "" {
		servers = append(servers, &http.Server{
			Addr:              *pprofAddr,
			Handler:           pprofMux,
			ReadHeaderTimeout: 2 * time.Second,
		})
	}
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
		log.Fatal(err)
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
}

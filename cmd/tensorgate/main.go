package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/KYS3K/tensorgate/internal/admission"
	"github.com/KYS3K/tensorgate/internal/gateway"
	"github.com/KYS3K/tensorgate/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	cfg := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// run returns instead of exiting, so its deferred cleanups always run
	if err := run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, cfg config) error {
	m := metrics.New(prometheus.DefaultRegisterer)

	completions, stopCompletions, err := newCompletionsHandler(cfg, m)
	if err != nil {
		return err
	}
	defer stopCompletions()

	// the first middleware in the list is the outermost one: it sees the request first
	var pipeline []func(http.Handler) http.Handler
	if cfg.admissionOn {
		adm := admission.New(cfg.admission, m)
		defer adm.Close()
		pipeline = append(pipeline, adm.Middleware())
	}

	mux := http.NewServeMux()
	mux.Handle("GET /events", gateway.NewStreamer())
	mux.Handle("POST /v1/chat/completions", chain(completions, pipeline...))

	servers := []*http.Server{newAPIServer(mux), newMetricsServer()}
	if cfg.pprofAddr != "" {
		servers = append(servers, newPprofServer(cfg.pprofAddr))
	}
	return serve(ctx, servers)
}

// chain wraps h in the middlewares so that the first one runs first:
// chain(h, a, b) is a(b(h))
func chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

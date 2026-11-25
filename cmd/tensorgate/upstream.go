package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/KYS3K/tensorgate/internal/gateway"
	"github.com/KYS3K/tensorgate/internal/lb"
	"github.com/KYS3K/tensorgate/internal/metrics"
)

// newCompletionsHandler returns what answers POST /v1/chat/completions: the
// built-in mock when no upstreams are configured, otherwise the load balancer.
// The returned stop function releases whatever the handler started (health check
// goroutines, idle upstream connections); call it once, after the servers are down.
func newCompletionsHandler(cfg config, m *metrics.Metrics) (h http.Handler, stop func(), err error) {
	upstreams, err := lb.ParseUpstreams(cfg.upstreams)
	if err != nil {
		return nil, nil, err
	}
	if len(upstreams) == 0 {
		mock := gateway.NewCompletionsHandler(m)
		mock.FlushInterval = cfg.flushInterval
		return mock, func() {}, nil
	}

	lbCfg := cfg.lb
	lbCfg.Upstreams = upstreams
	pool, err := lb.NewPool(lbCfg)
	if err != nil {
		return nil, nil, err
	}
	handler, err := lb.NewHandler(pool, cfg.lbRetryAfter, cfg.lbRetryAfterJitter, m)
	if err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("lb: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		lb.RunHealthChecks(ctx, pool, lbCfg)
	}()

	log.Printf("balancing over %d upstreams: %s", len(upstreams), cfg.upstreams)
	return handler, func() {
		cancel()
		<-done
		pool.Close()
	}, nil
}

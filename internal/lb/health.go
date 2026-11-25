package lb

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"
)

func RunHealthChecks(ctx context.Context, p *Pool, cfg Config) {
	var wg sync.WaitGroup
	dcfg := cfg.withDefaults()
	for _, b := range p.backends {
		wg.Go(func() {
			checkLoop(ctx, b, dcfg)
		})
	}
	wg.Wait()
}

func checkLoop(ctx context.Context, b *Backend, cfg Config) {
	client := &http.Client{
		Transport: b.transport,
	}

	successes := 0
	failed := 0
	ticker := time.NewTicker(cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok := probe(ctx, client, b.HealthPathURL(), cfg)
			if ctx.Err() != nil {
				return // probe was cut by shutdown
			}
			if ok {
				successes++
				failed = 0
			} else {
				failed++
				successes = 0
			}
			if failed >= cfg.FailThreshold && b.Alive() {
				b.kill()
			}
			if successes >= cfg.RiseThreshold && !b.Alive() {
				b.resurrect()
			}
		}
	}
}

func probe(ctx context.Context, client *http.Client, url string, cfg Config) bool {
	ctxwt, cancel := context.WithTimeout(ctx, cfg.CheckTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctxwt, "GET", url, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

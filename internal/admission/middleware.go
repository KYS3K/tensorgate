// Package admission gates requests and rate limits by auth key
package admission

import (
	"errors"
	"net/http"
	"time"

	"github.com/KYS3K/tensorgate/internal/metrics"
)

type Controller struct {
	keys    *KeyLimiter
	gate    *Gate
	metrics *metrics.Metrics
	cfg     Config
}

func New(cfg Config, m *metrics.Metrics) *Controller {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	c := &Controller{
		gate:    NewGate(cfg.MaxStreams, cfg.MaxQueueWait),
		keys:    NewKeyLimiter(cfg),
		cfg:     cfg,
		metrics: m,
	}
	c.metrics.TrackKeys(c.keys.Len)
	return c
}
func (c *Controller) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key, ok := KeyFromRequest(r)
			if !ok {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			ok, retryAfter := c.keys.Allow(key, c.cfg.Now())
			if !ok {
				w.Header().Set("Retry-After", RetryAfter(retryAfter, c.cfg.RateLimitJitter))
				http.Error(w, "Too many requests", http.StatusTooManyRequests)
				c.metrics.AdmissionDone(metrics.AdmissionRejected429)
				return
			}
			waited, err := c.gate.Acquire(r.Context())
			if errors.Is(err, ErrOverloaded) {
				w.Header().Set("Retry-After", RetryAfter(c.cfg.MaxQueueWait, c.cfg.OverloadJitter))
				http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
				c.metrics.AdmissionDone(metrics.AdmissionRejected503)
				c.metrics.ObserveQueueWait(waited)
				return
			}
			if err != nil {
				c.metrics.AdmissionDone(metrics.AdmissionClientGone)
				return
			}
			defer c.gate.Release()
			c.metrics.ObserveQueueWait(waited)
			c.metrics.AdmissionDone(metrics.AdmissionAccepted)
			next.ServeHTTP(w, r)
		})
	}
}
func (c *Controller) Close() {
	c.keys.Close()
}

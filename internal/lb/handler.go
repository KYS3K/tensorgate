// Package lb balances multiple backends by LUFO, run health checks, kills failing backends
package lb

import (
	"errors"
	"net/http"
	"time"

	"github.com/KYS3K/tensorgate/internal/retryafter"
)

type Handler struct {
	p          *Pool
	retryAfter time.Duration
	jitter     time.Duration
}

var _ http.Handler = (*Handler)(nil)

func NewHandler(p *Pool, retryAfter, jitter time.Duration) (*Handler, error) {
	if retryAfter <= 0 {
		return nil, errors.New("lb: retryAfter must be > 0")
	}
	if jitter < 0 {
		return nil, errors.New("lb: jitter must be >= 0")
	}
	if p == nil {
		return nil, errors.New("lb: pool cannot be nil")
	}

	return &Handler{
		p:          p,
		retryAfter: retryAfter,
		jitter:     jitter,
	}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b := h.p.Pick()
	if b == nil {
		w.Header().Set("Retry-After", retryafter.Format(h.retryAfter, h.jitter))
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	defer b.Release()
	b.proxy.ServeHTTP(w, r)
}

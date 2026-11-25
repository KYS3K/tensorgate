// Package lb balances multiple backends by LUFO, run health checks, kills failing backends
package lb

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/KYS3K/tensorgate/internal/metrics"
	"github.com/KYS3K/tensorgate/internal/retryafter"
)

const noBackend = metrics.LBNoBackend

type Handler struct {
	p          *Pool
	retryAfter time.Duration
	jitter     time.Duration
	metrics    *metrics.Metrics // nil disables metrics
}

var _ http.Handler = (*Handler)(nil)

func NewHandler(p *Pool, retryAfter, jitter time.Duration, m *metrics.Metrics) (*Handler, error) {
	if retryAfter <= 0 {
		return nil, errors.New("lb: retryAfter must be > 0")
	}
	if jitter < 0 {
		return nil, errors.New("lb: jitter must be >= 0")
	}
	if p == nil {
		return nil, errors.New("lb: pool cannot be nil")
	}
	for _, b := range p.backends {
		m.TrackBackend(b.Name, b.Alive, b.InFlight)
	}

	return &Handler{
		p:          p,
		retryAfter: retryAfter,
		jitter:     jitter,
		metrics:    m,
	}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	body, status := readBody(w, r, h.p.maxBody)
	if status != 0 {
		http.Error(w, http.StatusText(status), status)
		h.metrics.LBRequestDone(noBackend, "4xx")
		return
	}

	h.p.budget.deposit()

	b := h.p.Pick()
	if b == nil {
		h.unavailable(w)
		h.metrics.LBRequestDone(noBackend, "no_backend")
		return
	}

	var rec *recorder
	finished := false
	// deferred: ReverseProxy panics with ErrAbortHandler when a stream is cut mid-response
	defer func() {
		h.metrics.LBRequestDone(b.Name, class(rec.status, finished))
		if rec.wrote && rec.status/100 == 2 {
			h.metrics.ObserveLBTTFT(b.URL.Host, rec.ttft)
		}
	}()

	var buf [4]*Backend
	tried := append(buf[:0], b)
	for {
		a := &attempt{eligible: len(tried) < h.p.maxAttempts && h.p.otherAlive(tried)}
		rec = newRecorder(w, start)
		h.try(b, rec, r, body, a)

		if a.denied {
			h.metrics.LBRetryBudgetExhausted()
		}
		if !a.retry || r.Context().Err() != nil {
			break
		}
		next := h.p.pick(tried)
		if next == nil {
			h.unavailable(rec)
			break
		}
		b = next
		tried = append(tried, b)
		h.metrics.LBRetry()
	}
	finished = true
}

func (h *Handler) try(b *Backend, w http.ResponseWriter, r *http.Request, body []byte, a *attempt) {
	defer b.Release()
	r = r.WithContext(withAttempt(r.Context(), a))
	if body != nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
	}
	b.proxy.ServeHTTP(w, r)
}

func (h *Handler) unavailable(w http.ResponseWriter) {
	w.Header().Set("Retry-After", retryafter.Format(h.retryAfter, h.jitter))
	http.Error(w, "service unavailable", http.StatusServiceUnavailable)
}

func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, int) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, 0
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return nil, http.StatusRequestEntityTooLarge
		}
		return nil, http.StatusBadRequest
	}
	return body, 0
}

package lb

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
)

type Backend struct {
	URL           *url.URL
	Name          string // metrics label: the host, with the scheme when two upstreams share a host
	inFlight      atomic.Int64
	alive         atomic.Bool
	passiveFails  atomic.Int32
	transport     *http.Transport
	proxy         *httputil.ReverseProxy
	failThreshold int
	pool          *Pool // nil for a backend built alone, which may then always be ejected
	HealthPath    string
}

func newBackend(u *url.URL, cfg Config) *Backend {
	dcfg := cfg.withDefaults()
	netDialer := &net.Dialer{
		Timeout: dcfg.DialTimeout,
	}

	b := &Backend{
		URL:  u,
		Name: u.Host,
		transport: &http.Transport{
			MaxIdleConnsPerHost:   dcfg.MaxIdleConnsPerHost,
			IdleConnTimeout:       dcfg.IdleConnTimeout,
			ResponseHeaderTimeout: dcfg.ResponseHeaderTimeout,
			DialContext:           netDialer.DialContext,
		},
		failThreshold: dcfg.PassiveThreshold,
		HealthPath:    dcfg.HealthPath,
	}
	b.alive.Store(true)
	b.proxy = &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(u)
			r.SetXForwarded()
		},
		Transport:      b.transport,
		FlushInterval:  -1, // prevent buffering for sse
		ErrorHandler:   b.errorHandler,
		ModifyResponse: b.modifyResponse,
	}

	return b
}

func (b *Backend) modifyResponse(r *http.Response) error {
	if shedding(r.StatusCode, r.Header) {
		return nil
	}
	if !failedBeforeClient(r.StatusCode, r.Header) {
		b.onSuccess()
		return nil
	}
	b.onFail()
	if retryableStatus(r.StatusCode, r.Header) && takeRetry(r.Request.Context(), b.budget()) {
		return errRetry
	}
	return nil
}

func (b *Backend) errorHandler(rw http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, context.Canceled) || r.Context().Err() != nil || errors.Is(err, errRetry) {
		return
	}
	b.onFail()
	if retryableError(err) && takeRetry(r.Context(), b.budget()) {
		return
	}
	http.Error(rw, "lb: bad gateway", http.StatusBadGateway)
}

func (b *Backend) budget() *retryBudget {
	if b.pool == nil {
		return nil
	}
	return b.pool.budget
}

func (b *Backend) Release() {
	r := b.inFlight.Add(-1)
	if r < 0 {
		b.inFlight.Add(+1)
		panic("lb: negative backend streams: " + b.URL.Host)
	}
}

func (b *Backend) acquire() {
	b.inFlight.Add(+1)
}

func (b *Backend) InFlight() int64 {
	return b.inFlight.Load()
}

func (b *Backend) Alive() bool {
	return b.alive.Load()
}

func (b *Backend) kill() {
	b.alive.Store(false)
	log.Printf("lb: upstream %s is dead: health probes keep failing", b.URL.Host)
}

func (b *Backend) resurrect() {
	b.alive.Store(true)
	b.passiveFails.Store(0)
	log.Printf("lb: upstream %s is back: health probes pass again", b.URL.Host)
}

func (b *Backend) onSuccess() {
	b.passiveFails.Store(0)
}

func (b *Backend) onFail() {
	fails := b.passiveFails.Add(+1)
	if fails < int32(b.failThreshold) {
		return
	}
	if b.pool == nil {
		b.alive.Store(false)
		return
	}
	b.pool.eject(b)
}

func (b *Backend) HealthPathURL() string {
	return b.URL.JoinPath(b.HealthPath).String()
}

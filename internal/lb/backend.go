package lb

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
)

type Backend struct {
	URL           *url.URL
	inFlight      atomic.Int64
	alive         atomic.Bool
	passiveFails  atomic.Int32
	transport     *http.Transport
	proxy         *httputil.ReverseProxy
	failThreshold int
	HealthPath    string
}

func newBackend(u *url.URL, cfg Config) *Backend {
	dcfg := cfg.withDefaults()
	netDialer := &net.Dialer{
		Timeout: dcfg.DialTimeout,
	}

	b := &Backend{
		URL: u,
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
	if r.StatusCode == http.StatusServiceUnavailable && r.Header.Get("Retry-After") != "" {
		return nil
	}
	if r.StatusCode == http.StatusServiceUnavailable ||
		r.StatusCode == http.StatusBadGateway ||
		r.StatusCode == http.StatusGatewayTimeout {
		b.onFail()
	} else {
		b.onSuccess()
	}
	return nil
}

func (b *Backend) errorHandler(rw http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	b.onFail()
	http.Error(rw, "lb: service unavailable", http.StatusBadGateway)
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
}

func (b *Backend) resurrect() {
	b.alive.Store(true)
	b.passiveFails.Store(0)
}

func (b *Backend) onSuccess() {
	b.passiveFails.Store(0)
}

func (b *Backend) onFail() {
	fails := b.passiveFails.Add(+1)
	if fails >= int32(b.failThreshold) {
		b.alive.Store(false)
	}
}

func (b *Backend) HealthPathURL() string {
	return b.URL.JoinPath(b.HealthPath).String()
}

package lb

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// startGateway builds a pool over the given upstreams and serves a Handler on a
// real httptest server (a real server is needed: ResponseRecorder cannot flush).
func startGateway(t *testing.T, retryAfter, jitter time.Duration, upstreams ...*httptest.Server) (*httptest.Server, *Pool) {
	t.Helper()
	return startGatewayCfg(t, Config{}, retryAfter, jitter, upstreams...)
}

// startGatewayCfg is startGateway with a caller-provided Config; Upstreams are filled in here.
func startGatewayCfg(t *testing.T, cfg Config, retryAfter, jitter time.Duration, upstreams ...*httptest.Server) (*httptest.Server, *Pool) {
	t.Helper()
	ups := make([]*url.URL, 0, len(upstreams))
	for _, s := range upstreams {
		u, err := url.Parse(s.URL)
		if err != nil {
			t.Fatal(err)
		}
		ups = append(ups, u)
	}
	cfg.Upstreams = ups
	p, err := NewPool(cfg)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(p.Close)

	h, err := NewHandler(p, retryAfter, jitter, nil)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	gw := httptest.NewServer(h)
	t.Cleanup(gw.Close)
	return gw, p
}

func newUpstream(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return s
}

// backendFor finds the pool backend that points at the given upstream.
func backendFor(t *testing.T, p *Pool, s *httptest.Server) *Backend {
	t.Helper()
	u, _ := url.Parse(s.URL)
	for _, b := range p.Backends() {
		if b.URL.Host == u.Host {
			return b
		}
	}
	t.Fatalf("no backend for %s", s.URL)
	return nil
}

// waitInFlight waits until the backend counter reaches want. inFlight is
// decremented by a defer in the handler goroutine, which may run a moment after
// the client already got its response, so an instant check would be flaky.
func waitInFlight(t *testing.T, b *Backend, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.InFlight() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s inFlight = %d, want %d", b.URL.Host, b.InFlight(), want)
}

func TestNewHandler_Errors(t *testing.T) {
	p := testPool(t, 1)

	tests := []struct {
		name       string
		p          *Pool
		retryAfter time.Duration
		jitter     time.Duration
	}{
		{"nil pool", nil, time.Second, 0},
		{"zero retryAfter", p, 0, 0},
		{"negative retryAfter", p, -time.Second, 0},
		{"negative jitter", p, time.Second, -time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, err := NewHandler(tt.p, tt.retryAfter, tt.jitter, nil)
			if err == nil {
				t.Fatalf("NewHandler() = %v, want error", h)
			}
			if h != nil {
				t.Error("NewHandler returned a handler together with an error")
			}
		})
	}
}

func TestHandler_ProxiesRequest(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Seen-Path", r.URL.Path)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("echo:" + string(body)))
	})
	gw, _ := startGateway(t, time.Second, 0, up)

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json", strings.NewReader("ping"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("status = %d, want 201 (the upstream status must pass through)", resp.StatusCode)
	}
	if string(got) != "echo:ping" {
		t.Errorf("body = %q, want %q", got, "echo:ping")
	}
	if p := resp.Header.Get("X-Seen-Path"); p != "/v1/chat/completions" {
		t.Errorf("upstream saw path %q, want /v1/chat/completions", p)
	}
}

func TestHandler_ReleasesInFlightAfterResponse(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	gw, p := startGateway(t, time.Second, 0, up)

	for range 20 {
		resp, err := http.Get(gw.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	waitInFlight(t, p.Backends()[0], 0)
}

// If the upstream is down the proxy answers 502, and the counter must still go back to zero.
func TestHandler_ReleasesInFlightWhenUpstreamIsDown(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {})
	gw, p := startGateway(t, time.Second, 0, up)
	up.Close()

	resp, err := http.Get(gw.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502 for a dead upstream", resp.StatusCode)
	}
	waitInFlight(t, p.Backends()[0], 0)
}

func TestHandler_AllDeadReturns503WithRetryAfter(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("a dead backend must not receive traffic")
	})
	gw, p := startGateway(t, 2*time.Second, 0, up)
	p.Backends()[0].alive.Store(false)

	resp, err := http.Get(gw.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "2" {
		t.Errorf("Retry-After = %q, want %q (integer seconds, not a Duration string)", got, "2")
	}
	if n := p.Backends()[0].InFlight(); n != 0 {
		t.Errorf("inFlight = %d after a 503 without a backend, want 0", n)
	}
}

// The jitter is the thing that spreads the retries of the rejected clients,
// a Handler that drops it would pass every other test.
func TestHandler_RetryAfterHasJitter(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {})
	gw, p := startGateway(t, time.Second, 5*time.Second, up)
	p.Backends()[0].alive.Store(false)

	seen := make(map[string]bool)
	for range 200 {
		resp, err := http.Get(gw.URL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		v := resp.Header.Get("Retry-After")
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 6 {
			t.Fatalf("Retry-After = %q, want an integer in [1, 6]", v)
		}
		seen[v] = true
	}
	if len(seen) < 2 {
		t.Errorf("200 responses gave only %v, jitter is not applied", seen)
	}
}

// SSE: the first event must reach the client while the upstream is still busy.
// Without FlushInterval = -1 the proxy buffers and the client sees nothing until the end.
func TestHandler_StreamsWithoutBuffering(t *testing.T) {
	release := make(chan struct{})
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
		_, _ = io.WriteString(w, "data: last\n\n")
	})
	gw, p := startGateway(t, time.Second, 0, up)
	defer close(release)

	resp, err := http.Get(gw.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	type result struct {
		line string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		line, err := bufio.NewReader(resp.Body).ReadString('\n')
		ch <- result{line, err}
	}()

	select {
	case r := <-ch:
		if r.err != nil || r.line != "data: first\n" {
			t.Fatalf("first event = %q, err %v", r.line, r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first event did not arrive while the stream is open: the proxy buffers (FlushInterval)")
	}

	// the stream is still open, so the backend must still be counted as busy
	if n := p.Backends()[0].InFlight(); n != 1 {
		t.Errorf("inFlight during an open stream = %d, want 1", n)
	}
}

// A client that goes away mid-stream must not leave the counter raised:
// otherwise the backend looks busier and busier until it gets no traffic at all.
func TestHandler_ClientDisconnectReleasesInFlight(t *testing.T) {
	started := make(chan struct{})
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done() // ends when the proxy drops the upstream connection
	})
	gw, p := startGateway(t, time.Second, 0, up)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, gw.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream handler did not start")
	}
	b := p.Backends()[0]
	waitInFlight(t, b, 1)

	cancel()
	waitInFlight(t, b, 0)
}

// While one backend holds a long request, new requests must go to the idle one.
func TestHandler_PrefersIdleBackend(t *testing.T) {
	block := make(chan struct{})
	slowStarted := make(chan struct{}, 1)
	slow := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		slowStarted <- struct{}{}
		select {
		case <-block:
		case <-r.Context().Done():
		}
	})
	hitsFast := make(chan struct{}, 100)
	fast := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		hitsFast <- struct{}{}
	})
	gw, p := startGateway(t, time.Second, 0, slow, fast)
	defer close(block)

	// pin a request on the slow backend: make it the only live one for a moment
	fastB := backendFor(t, p, fast)
	fastB.alive.Store(false)
	go func() {
		resp, err := http.Get(gw.URL)
		if err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-slowStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("slow upstream did not get the request")
	}
	fastB.alive.Store(true)
	slowB := backendFor(t, p, slow)
	waitInFlight(t, slowB, 1)

	const n = 20
	for range n {
		resp, err := http.Get(gw.URL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if got := len(hitsFast); got != n {
		t.Errorf("idle backend got %d of %d requests, want all of them: LOR must avoid the busy one", got, n)
	}
}

// A single Backend must really use its own Transport, otherwise the dial and
// header timeouts configured in newBackend do nothing.
func TestBackend_ProxyUsesOwnTransport(t *testing.T) {
	b := newBackend(testURL(t), Config{})
	if b.proxy.Transport != http.RoundTripper(b.transport) {
		t.Error("proxy.Transport is not the backend's transport: the proxy goes through http.DefaultTransport")
	}
	if b.proxy.FlushInterval >= 0 {
		t.Errorf("FlushInterval = %v, want negative (flush every write) for SSE", b.proxy.FlushInterval)
	}
}

// The upstream must see its OWN host, not the one the client used for the
// gateway: virtual hosts and TLS SNI on a real GPU host depend on it.
func TestHandler_RewritesHostAndSetsForwardedHeaders(t *testing.T) {
	var host, xff, xfProto, xfHost string
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		host = r.Host
		xff = r.Header.Get("X-Forwarded-For")
		xfProto = r.Header.Get("X-Forwarded-Proto")
		xfHost = r.Header.Get("X-Forwarded-Host")
	})
	gw, _ := startGateway(t, time.Second, 0, up)

	req, _ := http.NewRequest(http.MethodGet, gw.URL+"/x", nil)
	req.Host = "public.example.com"
	req.Header.Set("X-Forwarded-For", "6.6.6.6") // a client must not be able to forge this
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	upURL, _ := url.Parse(up.URL)
	if host != upURL.Host {
		t.Errorf("upstream saw Host %q, want its own %q", host, upURL.Host)
	}
	if xff != "127.0.0.1" {
		t.Errorf("X-Forwarded-For = %q, want only the real peer 127.0.0.1 (client value must be dropped)", xff)
	}
	if xfProto != "http" {
		t.Errorf("X-Forwarded-Proto = %q, want http", xfProto)
	}
	if xfHost != "public.example.com" {
		t.Errorf("X-Forwarded-Host = %q, want the host the client asked for", xfHost)
	}
}

// An upstream that dies in the middle of a response must not leak inFlight: the
// reverse proxy aborts the handler by panic, and the deferred Release must still run.
func TestHandler_UpstreamDyingMidStreamReleasesInFlight(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: one\n\n")
		w.(http.Flusher).Flush()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		conn.Close() // the connection is cut: the chunked body never finishes
	})
	gw, p := startGateway(t, time.Second, 0, up)
	b := p.Backends()[0]

	resp, err := http.Get(gw.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr == nil {
		t.Error("the client saw a clean end of stream although the upstream was cut")
	}
	waitInFlight(t, b, 0)
}

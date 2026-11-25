package lb

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type counted struct {
	*httptest.Server
	hits atomic.Int64
}

func newCounted(t *testing.T, h http.HandlerFunc) *counted {
	t.Helper()
	c := &counted{}
	c.Server = newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		c.hits.Add(1)
		h(w, r)
	})
	return c
}

func gateway(t *testing.T, cs ...*counted) (*httptest.Server, *Pool) {
	t.Helper()
	servers := make([]*httptest.Server, len(cs))
	for i, c := range cs {
		servers[i] = c.Server
	}
	return startGatewayCfg(t, retryCfg(), time.Second, 0, servers...)
}

func echo(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	w.Header().Set("X-Served-By", "good")
	_, _ = io.WriteString(w, "echo:"+string(body))
}

func bad502(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Bad", "1")
	w.Header().Set("Content-Type", "text/x-bad")
	w.WriteHeader(http.StatusBadGateway)
	_, _ = io.WriteString(w, "bad gateway body")
}

// retryCfg keeps every backend alive however many times it fails, so only the
// retry logic decides where the request goes.
func retryCfg() Config { return Config{PassiveThreshold: 1_000_000} }

// busy makes LOR pick s second: it looks like it already serves a request.
func busy(t *testing.T, p *Pool, s *httptest.Server) {
	t.Helper()
	b := backendFor(t, p, s)
	b.acquire()
	t.Cleanup(b.Release)
}

func post(t *testing.T, url, body string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Post(url, "text/plain", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func TestRetry_DeadBackendFailsOverAndBodyIsReplayed(t *testing.T) {
	dead := newCounted(t, echo)
	good := newCounted(t, echo)
	gw, p := gateway(t, dead, good)
	dead.Close()
	busy(t, p, good.Server)

	for i := range 10 {
		resp, body := post(t, gw.URL, "payload")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status %d, want 200 from the second backend", i, resp.StatusCode)
		}
		if body != "echo:payload" {
			t.Fatalf("request %d: body %q, want the request body to reach the second backend intact", i, body)
		}
	}
}

func TestRetry_Upstream502FailsOverWithoutLeakingHeaders(t *testing.T) {
	bad := newCounted(t, bad502)
	good := newCounted(t, echo)
	gw, p := gateway(t, bad, good)
	busy(t, p, good.Server)

	resp, body := post(t, gw.URL, "x")

	if resp.StatusCode != http.StatusOK || body != "echo:x" {
		t.Fatalf("got %d %q, want 200 echo:x", resp.StatusCode, body)
	}
	if resp.Header.Get("X-Bad") != "" || resp.Header.Get("Content-Type") == "text/x-bad" {
		t.Errorf("headers of the failed attempt leaked to the client: %v", resp.Header)
	}
	if bad.hits.Load() != 1 {
		t.Errorf("bad backend was hit %d times, want exactly 1 (the retry must go elsewhere)", bad.hits.Load())
	}
}

// If every attempt fails the client must see the failure, not an empty 200.
func TestRetry_AllFailReturnsTheFailure(t *testing.T) {
	a := newCounted(t, bad502)
	b := newCounted(t, bad502)
	gw, _ := gateway(t, a, b)

	resp, body := post(t, gw.URL, "x")

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	if body != "bad gateway body" {
		t.Errorf("body = %q, want the last upstream's answer", body)
	}
}

func TestRetry_AllDeadReturns502(t *testing.T) {
	a := newCounted(t, echo)
	b := newCounted(t, echo)
	gw, _ := gateway(t, a, b)
	a.Close()
	b.Close()

	resp, _ := post(t, gw.URL, "x")

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
}

func TestRetry_SingleBackendHasNothingToRetryOn(t *testing.T) {
	a := newCounted(t, bad502)
	gw, _ := gateway(t, a)

	resp, body := post(t, gw.URL, "x")

	if resp.StatusCode != http.StatusBadGateway || body != "bad gateway body" {
		t.Fatalf("got %d %q, want the upstream's 502", resp.StatusCode, body)
	}
	if a.hits.Load() != 1 {
		t.Errorf("single backend hit %d times, want 1", a.hits.Load())
	}
}

func TestRetry_AttemptsAreBounded(t *testing.T) {
	var ups []*counted
	for range 4 {
		c := newCounted(t, bad502)
		ups = append(ups, c)
	}
	gw, _ := gateway(t, ups...)

	post(t, gw.URL, "x")

	var total int64
	for _, u := range ups {
		total += u.hits.Load()
	}
	if total > 2 {
		t.Errorf("one request caused %d upstream attempts, want at most 2", total)
	}
}

// 503 + Retry-After is an upstream shedding load on purpose: pass it on, do not shop around.
func TestRetry_ShedLoadIsNotRetried(t *testing.T) {
	shed := newCounted(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	good := newCounted(t, echo)
	gw, p := gateway(t, shed, good)
	busy(t, p, good.Server)

	resp, _ := post(t, gw.URL, "x")

	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "7" {
		t.Fatalf("got %d / Retry-After=%q, want the upstream's own 503 / 7", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if good.hits.Load() != 0 {
		t.Errorf("a load-shedding answer was retried on another backend")
	}
}

func TestRetry_ClientErrorsAndServerErrorsPassThrough(t *testing.T) {
	for _, code := range []int{400, 404, 429, 500} {
		a := newCounted(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) })
		good := newCounted(t, echo)
		gw, p := gateway(t, a, good)
		busy(t, p, good.Server)

		resp, _ := post(t, gw.URL, "x")

		if resp.StatusCode != code {
			t.Errorf("status %d was changed to %d", code, resp.StatusCode)
		}
		if good.hits.Load() != 0 {
			t.Errorf("status %d was retried", code)
		}
	}
}

// After the first byte reached the client the request is committed: no retry.
func TestRetry_NoRetryAfterFirstByte(t *testing.T) {
	cut := newCounted(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: one\n\n")
		w.(http.Flusher).Flush()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	})
	good := newCounted(t, echo)
	gw, p := gateway(t, cut, good)
	busy(t, p, good.Server)

	resp, err := http.Get(gw.URL)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if !strings.Contains(string(b), "data: one") {
		t.Errorf("client lost the first chunk: %q", b)
	}
	if good.hits.Load() != 0 {
		t.Error("the stream was retried after the first byte was sent: the client would see two generations")
	}
}

func TestRetry_ClientCancelIsNotRetried(t *testing.T) {
	started := make(chan struct{}, 1)
	slow := newCounted(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	})
	good := newCounted(t, echo)
	m := startMetered(t, retryCfg(), slow.Server, good.Server)
	busy(t, m.p, good.Server)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, m.gw.URL, nil)
	go func() {
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	<-started
	cancel()

	m.waitValue(t, requests, map[string]string{"backend": backendFor(t, m.p, slow.Server).URL.Host, "class": "canceled"}, 1)
	if good.hits.Load() != 0 {
		t.Error("a request canceled by the client was retried")
	}
}

func TestRetry_InFlightIsReleasedOnEveryAttempt(t *testing.T) {
	bad := newCounted(t, bad502)
	good := newCounted(t, echo)
	gw, p := gateway(t, bad, good)

	for range 20 {
		post(t, gw.URL, "x")
	}

	for _, b := range p.Backends() {
		waitInFlight(t, b, 0)
	}
}

func TestRetry_BigBodyIsRejectedNotTruncated(t *testing.T) {
	good := newCounted(t, echo)
	gw, _ := gateway(t, good)

	resp, _ := post(t, gw.URL, strings.Repeat("a", 4<<20))

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d for a 4 MiB body, want 413", resp.StatusCode)
	}
}

// A 504 is not here on purpose: see TestRetry_504IsNotRetried.
func TestRetry_Bare503FailsOver(t *testing.T) {
	for _, code := range []int{http.StatusServiceUnavailable} {
		bad := newCounted(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) })
		good := newCounted(t, echo)
		gw, p := gateway(t, bad, good)
		busy(t, p, good.Server)

		resp, body := post(t, gw.URL, "x")

		if resp.StatusCode != http.StatusOK || body != "echo:x" {
			t.Errorf("%d: got %d %q, want a retry to the good backend", code, resp.StatusCode, body)
		}
	}
}

// The failed answer must not be read: its body may be cut, and that used to abort the request.
func TestRetry_CutErrorBodyStillFailsOver(t *testing.T) {
	bad := newCounted(t, func(w http.ResponseWriter, r *http.Request) {
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		_, _ = buf.WriteString("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 1000\r\n\r\npartial")
		_ = buf.Flush()
		conn.Close()
	})
	good := newCounted(t, echo)
	gw, p := gateway(t, bad, good)
	busy(t, p, good.Server)

	resp, body := post(t, gw.URL, "x")

	if resp.StatusCode != http.StatusOK || body != "echo:x" {
		t.Fatalf("got %d %q, want 200 echo:x", resp.StatusCode, body)
	}
}

// The other backend dies while the first attempt runs: the client gets the same 503 as with no backends.
func TestRetry_NobodyLeftIs503WithRetryAfter(t *testing.T) {
	var p *Pool
	var good *counted
	bad := newCounted(t, func(w http.ResponseWriter, r *http.Request) {
		backendFor(t, p, good.Server).kill()
		w.WriteHeader(http.StatusBadGateway)
	})
	good = newCounted(t, echo)
	var gw *httptest.Server
	gw, p = gateway(t, bad, good)
	busy(t, p, good.Server)

	resp, _ := post(t, gw.URL, "x")

	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("got %d / Retry-After=%q, want 503 with Retry-After", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if good.hits.Load() != 0 {
		t.Error("a dead backend got the retry")
	}
}

func TestRetry_HeadersSetBeforeLBSurvive(t *testing.T) {
	bad := newCounted(t, bad502)
	good := newCounted(t, echo)
	var cfg = retryCfg()
	for _, s := range []*httptest.Server{bad.Server, good.Server} {
		u, _ := url.Parse(s.URL)
		cfg.Upstreams = append(cfg.Upstreams, u)
	}
	p, err := NewPool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	h, err := NewHandler(p, time.Second, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-Id", "42")
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(gw.Close)
	busy(t, p, good.Server)

	resp, _ := post(t, gw.URL, "x")

	if resp.Header.Get("X-Request-Id") != "42" {
		t.Errorf("header set before the LB was lost on retry: %v", resp.Header)
	}
	if resp.Header.Get("X-Bad") != "" {
		t.Errorf("headers of the failed attempt leaked: %v", resp.Header)
	}
}

func TestRetry_BodyLimitIsConfigurable(t *testing.T) {
	good := newCounted(t, echo)
	gw, _ := startGatewayCfg(t, Config{MaxBodyBytes: 10}, time.Second, 0, good.Server)

	resp, _ := post(t, gw.URL, strings.Repeat("a", 11))

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d for 11 bytes with a 10 byte limit, want 413", resp.StatusCode)
	}
}

func TestRetry_504IsNotRetried(t *testing.T) {
	slow := newCounted(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusGatewayTimeout) })
	good := newCounted(t, echo)
	gw, p := gateway(t, slow, good)
	busy(t, p, good.Server)

	resp, _ := post(t, gw.URL, "x")

	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want the upstream's 504", resp.StatusCode)
	}
	if good.hits.Load() != 0 {
		t.Error("a 504 was sent to another backend")
	}
}

func TestRetry_504StillCountsAgainstTheBackend(t *testing.T) {
	slow := newCounted(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusGatewayTimeout) })
	spare := newCounted(t, echo)
	gw, p := startGatewayCfg(t, Config{PassiveThreshold: 2, MaxAttempts: 1}, time.Second, 0, slow.Server, spare.Server)
	busy(t, p, spare.Server)

	post(t, gw.URL, "x")
	post(t, gw.URL, "x")

	if backendFor(t, p, slow.Server).Alive() {
		t.Error("two 504 did not mark the backend dead")
	}
}

// The upstream may be generating already: a header timeout must not start a second generation.
func TestRetry_HeaderTimeoutIsNotRetried(t *testing.T) {
	hang := newCounted(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
	})
	good := newCounted(t, echo)
	cfg := retryCfg()
	cfg.ResponseHeaderTimeout = 100 * time.Millisecond
	gw, p := startGatewayCfg(t, cfg, time.Second, 0, hang.Server, good.Server)
	busy(t, p, good.Server)

	resp, _ := post(t, gw.URL, "x")

	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", resp.StatusCode)
	}
	if good.hits.Load() != 0 {
		t.Error("a request that timed out waiting for headers was sent to another backend")
	}
}

func TestRetryableError(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL
	dead.Close()
	_, dialErr := http.Get(url)
	if dialErr == nil {
		t.Fatal("expected a dial error")
	}

	if !retryableError(dialErr) {
		t.Error("a refused connection must be retryable")
	}
	if !retryableError(io.ErrUnexpectedEOF) {
		t.Error("a connection cut before the response must be retryable")
	}
	if retryableError(context.DeadlineExceeded) {
		t.Error("a deadline is not retryable")
	}
}

func TestRetry_BudgetLimitsRetries(t *testing.T) {
	bad := newCounted(t, bad502)
	good := newCounted(t, echo)
	gw, p := gateway(t, bad, good)
	p.budget = newRetryBudget(10, 10*time.Second, 0) // no floor: only 10% of the requests
	busy(t, p, good.Server)

	const n = 100
	var ok, failed int
	for range n {
		resp, _ := post(t, gw.URL, "x")
		if resp.StatusCode == http.StatusOK {
			ok++
		} else {
			failed++
		}
	}

	// every request goes to the bad backend first and needs a retry
	if ok < 8 || ok > 12 {
		t.Errorf("%d of %d requests were saved by a retry, want about 10 (10%% budget)", ok, n)
	}
	if failed != n-ok {
		t.Errorf("failed = %d, want %d", failed, n-ok)
	}
}

// With an empty budget the failure is not retried and the client gets the upstream's own answer.
func TestRetry_EmptyBudgetPassesTheFailureThrough(t *testing.T) {
	bad := newCounted(t, bad502)
	good := newCounted(t, echo)
	gw, p := gateway(t, bad, good)
	p.budget = newRetryBudget(1, 10*time.Second, 0)
	busy(t, p, good.Server)

	resp, body := post(t, gw.URL, "x")

	if resp.StatusCode != http.StatusBadGateway || body != "bad gateway body" {
		t.Errorf("got %d %q, want the upstream's 502", resp.StatusCode, body)
	}
	if good.hits.Load() != 0 {
		t.Error("a retry was made with an empty budget")
	}
}

func TestRetry_BudgetIsNotSpentOnSuccessfulRequests(t *testing.T) {
	good := newCounted(t, echo)
	other := newCounted(t, echo)
	gw, p := gateway(t, good, other)
	p.budget = newRetryBudget(100, 10*time.Second, 0)

	for range 50 {
		post(t, gw.URL, "x")
	}

	if _, retries := p.budget.totals(); retries != 0 {
		t.Errorf("%d retries were taken from the budget by requests that never needed one", retries)
	}
}

func TestRetry_MaxAttemptsOneTurnsRetriesOff(t *testing.T) {
	bad := newCounted(t, bad502)
	good := newCounted(t, echo)
	cfg := retryCfg()
	cfg.MaxAttempts = 1
	gw, p := startGatewayCfg(t, cfg, time.Second, 0, bad.Server, good.Server)
	busy(t, p, good.Server)

	resp, _ := post(t, gw.URL, "x")

	if resp.StatusCode != http.StatusBadGateway || good.hits.Load() != 0 {
		t.Errorf("got %d, good hits %d: with MaxAttempts=1 the failure must go to the client", resp.StatusCode, good.hits.Load())
	}
}

func TestRetry_NeverGoesBackToATriedBackend(t *testing.T) {
	ups := []*counted{newCounted(t, bad502), newCounted(t, bad502), newCounted(t, bad502)}
	cfg := retryCfg()
	cfg.MaxAttempts = 3
	gw, p := gateway3(t, cfg, ups...)
	p.budget = nil

	for range 20 {
		for _, u := range ups {
			u.hits.Store(0)
		}
		post(t, gw.URL, "x")
		for i, u := range ups {
			if u.hits.Load() != 1 {
				t.Fatalf("backend %d hit %d times in one request, want each of 3 exactly once", i, u.hits.Load())
			}
		}
	}
}

func gateway3(t *testing.T, cfg Config, cs ...*counted) (*httptest.Server, *Pool) {
	t.Helper()
	servers := make([]*httptest.Server, len(cs))
	for i, c := range cs {
		servers[i] = c.Server
	}
	return startGatewayCfg(t, cfg, time.Second, 0, servers...)
}

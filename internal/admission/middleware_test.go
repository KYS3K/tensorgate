package admission

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KYS3K/tensorgate/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/goleak"
	"golang.org/x/time/rate"
)

// admissionTestConfig: one stream slot, 30ms of queueing, no rate limit and a frozen clock.
// Tests that need the limiter tighten RatePerKey and BurstPerKey themselves
func admissionTestConfig() Config {
	return Config{
		RatePerKey:   rate.Inf,
		BurstPerKey:  1000,
		KeyTTL:       time.Minute,
		SweepEvery:   time.Hour,
		MaxKeys:      1000,
		MaxStreams:   1,
		MaxQueueWait: 30 * time.Millisecond,
		Now:          func() time.Time { return base },
	}
}

// every controller gets its own registry: the global one would panic on the second registration
func newController(t *testing.T, cfg Config) (*Controller, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	c := New(cfg, metrics.New(reg))
	t.Cleanup(c.Close)
	return c, reg
}

// the handler behind the middleware: counts how many requests got through
type probe struct{ calls atomic.Int64 }

func (p *probe) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.calls.Add(1)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// a stream that never ends until release is closed: it holds its slot the whole time
type blocker struct {
	entered chan struct{}
	release chan struct{}
}

func newBlocker() *blocker {
	return &blocker{entered: make(chan struct{}, 64), release: make(chan struct{})}
}

func (b *blocker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.entered <- struct{}{}
	<-b.release
}

func serveCtx(ctx context.Context, h http.Handler, key string) *httptest.ResponseRecorder {
	r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", nil)
	if key != "" {
		r.Header.Set("X-Api-Key", key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

func serve(h http.Handler, key string) *httptest.ResponseRecorder {
	return serveCtx(context.Background(), h, key)
}

// starts a request in the background and returns once it is inside the handler, i.e. holds a slot
func holdSlot(t *testing.T, h http.Handler, b *blocker, key string) (done <-chan struct{}) {
	t.Helper()
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		serve(h, key)
	}()
	select {
	case <-b.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the request did not reach the handler")
	}
	return ch
}

func admissionCount(t *testing.T, reg *prometheus.Registry, result string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "tensorgate_admission_requests_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "result" && l.GetValue() == result {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func queueWaitCount(t *testing.T, reg *prometheus.Registry) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "tensorgate_admission_queue_wait_seconds" {
			return mf.GetMetric()[0].GetHistogram().GetSampleCount()
		}
	}
	return 0
}

func trackedKeys(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "tensorgate_admission_tracked_keys" {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatal("tensorgate_admission_tracked_keys is not registered")
	return 0
}

func TestMiddlewareRejectsRequestsWithoutAKey(t *testing.T) {
	tests := []struct {
		name   string
		header string
		value  string
	}{
		{"no headers", "", ""},
		{"too long key", "X-Api-Key", strings.Repeat("k", maxKeyLength+1)},
		{"not a bearer token", "Authorization", "Basic dXNlcjpwYXNz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, reg := newController(t, admissionTestConfig())
			next := &probe{}
			h := c.Middleware()(next)

			r := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.header != "" {
				r.Header.Set(tt.header, tt.value)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401", rec.Code)
			}
			if next.calls.Load() != 0 {
				t.Error("the handler was reached without a key")
			}
			if len(c.gate.slots) != 0 {
				t.Error("a request without a key took a stream slot")
			}
			if trackedKeys(t, reg) != 0 {
				t.Error("a request without a key created a limiter entry")
			}
		})
	}
}

func TestMiddlewareAcceptsAndReleasesTheSlot(t *testing.T) {
	c, reg := newController(t, admissionTestConfig()) // a single slot
	next := &probe{}
	h := c.Middleware()(next)

	// the second request can only pass if the first one gave its slot back when it finished
	for i := range 3 {
		rec := serve(h, "a")
		if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
			t.Fatalf("request %d: %d %q, want the handler's own 200 ok", i, rec.Code, rec.Body.String())
		}
	}
	if next.calls.Load() != 3 {
		t.Errorf("handler called %d times, want 3", next.calls.Load())
	}
	if len(c.gate.slots) != 0 {
		t.Errorf("%d slots still taken after all requests finished", len(c.gate.slots))
	}
	if got := admissionCount(t, reg, "accepted"); got != 3 {
		t.Errorf("accepted = %v, want 3", got)
	}
	if got := queueWaitCount(t, reg); got != 3 {
		t.Errorf("queue wait observations = %d, want 3", got)
	}
}

func TestMiddlewareRateLimit(t *testing.T) {
	cfg := admissionTestConfig()
	cfg.RatePerKey = 1
	cfg.BurstPerKey = 2
	cfg.MaxStreams = 10
	c, reg := newController(t, cfg)
	next := &probe{}
	h := c.Middleware()(next)

	for i := range 2 {
		if rec := serve(h, "a"); rec.Code != http.StatusOK {
			t.Fatalf("request %d within the burst: status %d", i, rec.Code)
		}
	}

	rec := serve(h, "a")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over the burst: status %d, want 429", rec.Code)
	}
	// Result().Header is the snapshot taken at WriteHeader, i.e. what a real client receives.
	// rec.Header() is the live map and would show a header that was set too late
	if got := rec.Result().Header.Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want %q (it is lost if the header is set after WriteHeader)", got, "1")
	}
	if next.calls.Load() != 2 {
		t.Errorf("handler called %d times, the limited request must not reach it", next.calls.Load())
	}

	// the same key through the bearer header is the same bucket
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set("Authorization", "Bearer a")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("bearer with the same key: status %d, want 429", rec.Code)
	}

	// another key has its own bucket
	if rec := serve(h, "b"); rec.Code != http.StatusOK {
		t.Errorf("a different key was limited: status %d", rec.Code)
	}

	if got := admissionCount(t, reg, "rejected_429"); got != 2 {
		t.Errorf("rejected_429 = %v, want 2", got)
	}
	if got := admissionCount(t, reg, "accepted"); got != 3 {
		t.Errorf("accepted = %v, want 3", got)
	}
	if len(c.gate.slots) != 0 {
		t.Error("a slot leaked")
	}
}

// the clock from Config is what drives the limiter: if New replaced it with time.Now,
// no time would pass here and the key would stay limited
func TestMiddlewareUsesTheInjectedClock(t *testing.T) {
	now := base
	cfg := admissionTestConfig()
	cfg.RatePerKey = 1
	cfg.BurstPerKey = 1
	cfg.Now = func() time.Time { return now }
	c, _ := newController(t, cfg)
	h := c.Middleware()(&probe{})

	if rec := serve(h, "a"); rec.Code != http.StatusOK {
		t.Fatalf("first request: status %d", rec.Code)
	}
	if rec := serve(h, "a"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second request: status %d, want 429", rec.Code)
	}

	now = now.Add(time.Second) // a token has been refilled on the injected clock only
	if rec := serve(h, "a"); rec.Code != http.StatusOK {
		t.Fatalf("after 1s on the injected clock: status %d, want 200", rec.Code)
	}
}

func TestMiddlewareShedsWhenFull(t *testing.T) {
	c, reg := newController(t, admissionTestConfig())
	b := newBlocker()
	h := c.Middleware()(b)
	holder := holdSlot(t, h, b, "holder")

	start := time.Now()
	rec := serve(h, "k2")
	elapsed := time.Since(start)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := rec.Result().Header.Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want %q (it is lost if the header is set after WriteHeader)", got, "1")
	}
	if elapsed < c.cfg.MaxQueueWait {
		t.Errorf("shed after %v, it must wait the full %v first", elapsed, c.cfg.MaxQueueWait)
	}

	// A failed request owns no slot, so it must not give one back. If it did, the holder's slot
	// would be free again and this request would get into the handler
	if rec := serve(h, "k3"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("second request: status %d, want 503 (a rejected request freed someone else's slot)", rec.Code)
	}

	close(b.release)
	<-holder

	if rec := serve(h, "k4"); rec.Code != http.StatusOK {
		t.Fatalf("after the holder finished: status %d, want 200", rec.Code)
	}
	if got := admissionCount(t, reg, "rejected_503"); got != 2 {
		t.Errorf("rejected_503 = %v, want 2", got)
	}
	if got := admissionCount(t, reg, "accepted"); got != 2 { // the holder and k4
		t.Errorf("accepted = %v, want 2", got)
	}
	// 2 shed + 2 accepted: shed requests are in the histogram too, otherwise p99 looks better than it is
	if got := queueWaitCount(t, reg); got != 4 {
		t.Errorf("queue wait observations = %d, want 4", got)
	}
}

// the limiter runs before the gate: a limited request gets 429 at once instead of queueing for a slot
func TestMiddlewareLimiterRunsBeforeGate(t *testing.T) {
	cfg := admissionTestConfig()
	cfg.RatePerKey = 1
	cfg.BurstPerKey = 1
	cfg.MaxQueueWait = 5 * time.Second // if the gate went first this would take 5s
	c, _ := newController(t, cfg)
	b := newBlocker()
	h := c.Middleware()(b)
	holder := holdSlot(t, h, b, "a") // spends the only token of "a" and holds the only slot
	defer func() { close(b.release); <-holder }()

	start := time.Now()
	rec := serve(h, "a")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("took %v: the request queued for a slot before being rate limited", elapsed)
	}
}

// the client leaving the queue is not an overload: no response, no 503 in the numbers
func TestMiddlewareClientLeavesTheQueue(t *testing.T) {
	cfg := admissionTestConfig()
	cfg.MaxQueueWait = 5 * time.Second // only the context can end the wait
	c, reg := newController(t, cfg)
	b := newBlocker()
	h := c.Middleware()(b)
	holder := holdSlot(t, h, b, "holder")

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	defer cancel()

	start := time.Now()
	rec := serveCtx(ctx, h, "gone")
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("took %v, the cancelled context was ignored", elapsed)
	}
	if rec.Body.Len() != 0 || rec.Header().Get("Retry-After") != "" {
		t.Errorf("something was written to a client that is gone: %q %v", rec.Body.String(), rec.Header())
	}

	if got := admissionCount(t, reg, "client_gone"); got != 1 {
		t.Errorf("client_gone = %v, want 1", got)
	}
	if got := admissionCount(t, reg, "rejected_503"); got != 0 {
		t.Errorf("rejected_503 = %v, a client leaving is not an overload", got)
	}
	if got := queueWaitCount(t, reg); got != 1 { // only the holder
		t.Errorf("queue wait observations = %d, want 1 (the one who left is not counted)", got)
	}

	// the one who left did not free the holder's slot
	if rec := serve(h, "other"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503: the slot must still belong to the holder", rec.Code)
	}
	close(b.release)
	<-holder
}

// the stream may crash: the slot must come back anyway, otherwise a few panics shut the gateway
func TestMiddlewareReleasesTheSlotOnPanic(t *testing.T) {
	c, _ := newController(t, admissionTestConfig())
	h := c.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))

	func() {
		defer func() { recover() }() // net/http does this for a real server
		serve(h, "a")
	}()

	if len(c.gate.slots) != 0 {
		t.Fatal("the slot was not released after a panic in the handler")
	}
}

func TestMiddlewareTracksKeys(t *testing.T) {
	cfg := admissionTestConfig()
	cfg.MaxStreams = 10
	c, reg := newController(t, cfg)
	h := c.Middleware()(&probe{})

	serve(h, "a")
	serve(h, "b")
	serve(h, "a")

	if got := trackedKeys(t, reg); got != 2 {
		t.Errorf("tracked keys = %v, want 2", got)
	}
}

// many clients at once: the limit holds and every request is accounted for exactly once
func TestMiddlewareConcurrent(t *testing.T) {
	const (
		maxStreams = 4
		goroutines = 64
		perClient  = 10
	)
	cfg := admissionTestConfig()
	cfg.MaxStreams = maxStreams
	cfg.MaxQueueWait = 2 * time.Millisecond
	c, reg := newController(t, cfg)

	var inside, peak atomic.Int64
	h := c.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := inside.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(200 * time.Microsecond)
		inside.Add(-1)
	}))

	var ok, shed atomic.Int64
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "client-" + strconv.Itoa(g)
			for range perClient {
				switch rec := serve(h, key); rec.Code {
				case http.StatusOK:
					ok.Add(1)
				case http.StatusServiceUnavailable:
					shed.Add(1)
				default:
					t.Errorf("unexpected status %d", rec.Code)
				}
			}
		}()
	}
	wg.Wait()

	if p := peak.Load(); p > maxStreams {
		t.Fatalf("%d streams at once, the limit is %d", p, maxStreams)
	}
	if ok.Load()+shed.Load() != goroutines*perClient {
		t.Fatalf("ok %d + shed %d != %d requests", ok.Load(), shed.Load(), goroutines*perClient)
	}
	if got := admissionCount(t, reg, "accepted"); got != float64(ok.Load()) {
		t.Errorf("accepted metric = %v, responses say %d", got, ok.Load())
	}
	if got := admissionCount(t, reg, "rejected_503"); got != float64(shed.Load()) {
		t.Errorf("rejected_503 metric = %v, responses say %d", got, shed.Load())
	}
	if len(c.gate.slots) != 0 {
		t.Errorf("%d slots leaked", len(c.gate.slots))
	}
	t.Logf("ok %d, shed %d, peak %d", ok.Load(), shed.Load(), peak.Load())
}

func TestControllerCloseStopsTheJanitor(t *testing.T) {
	defer goleak.VerifyNone(t)

	for range 20 {
		c := New(admissionTestConfig(), nil) // nil metrics are allowed
		c.Close()
		c.Close() // twice is fine
	}
}

// 429 and 503 have their own jitter: swapping them would go unnoticed with the zero jitter of the other tests.
// The wide one must spread the Retry-After over many seconds, the other one must stay at the exact minimum
func TestMiddlewareJitterIsPerRejection(t *testing.T) {
	tests := []struct {
		name      string
		rateLimit time.Duration // jitter of a 429
		overload  time.Duration // jitter of a 503
		wide429   bool
		wide503   bool
	}{
		{name: "wide jitter on 503 only", overload: 10 * time.Second, wide503: true},
		{name: "wide jitter on 429 only", rateLimit: 10 * time.Second, wide429: true},
	}

	const samples = 40
	retryAfters := func(t *testing.T, recs []*httptest.ResponseRecorder, wantCode int) []int {
		t.Helper()
		out := make([]int, 0, len(recs))
		for _, rec := range recs {
			if rec.Code != wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, wantCode)
			}
			n, err := strconv.Atoi(rec.Result().Header.Get("Retry-After"))
			if err != nil {
				t.Fatalf("Retry-After is not a number: %v", err)
			}
			out = append(out, n)
		}
		return out
	}
	check := func(t *testing.T, what string, got []int, wide bool) {
		t.Helper()
		distinct := map[int]bool{}
		for _, n := range got {
			distinct[n] = true
			if n < 1 || n > 11 {
				t.Errorf("%s: Retry-After %d outside [1, 11]", what, n)
			}
		}
		switch {
		case wide && len(distinct) < 2:
			t.Errorf("%s: %d samples gave only %v, the jitter did not spread", what, samples, distinct)
		case !wide && (len(distinct) != 1 || !distinct[1]):
			t.Errorf("%s: got %v, want only 1: this rejection must not get the other one's jitter", what, distinct)
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := admissionTestConfig()
			cfg.RatePerKey = 1
			cfg.BurstPerKey = 1
			cfg.MaxQueueWait = 5 * time.Millisecond
			cfg.RateLimitJitter = tt.rateLimit
			cfg.OverloadJitter = tt.overload
			c, _ := newController(t, cfg)
			b := newBlocker()
			h := c.Middleware()(b)
			holder := holdSlot(t, h, b, "a") // spends the one token of "a" and holds the one slot
			defer func() { close(b.release); <-holder }()

			var limited, shed []*httptest.ResponseRecorder
			for i := range samples {
				limited = append(limited, serve(h, "a"))                // no token: 429
				shed = append(shed, serve(h, "fresh-"+strconv.Itoa(i))) // own token, no slot: 503
			}
			check(t, "429", retryAfters(t, limited, http.StatusTooManyRequests), tt.wide429)
			check(t, "503", retryAfters(t, shed, http.StatusServiceUnavailable), tt.wide503)
		})
	}
}

// discards everything: the benchmark must measure the middleware, not the recorder
type discardWriter struct{ h http.Header }

func (d discardWriter) Header() http.Header         { return d.h }
func (d discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (d discardWriter) WriteHeader(int)             {}

// the accepted path is the hot one: key, limiter, slot and metrics together must not allocate
func BenchmarkMiddlewareAccepted(b *testing.B) {
	cfg := admissionTestConfig()
	cfg.MaxStreams = 1 << 16
	c := New(cfg, metrics.New(prometheus.NewRegistry()))
	defer c.Close()
	h := c.Middleware()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set("X-Api-Key", "bench-key")
	w := discardWriter{h: http.Header{}}
	h.ServeHTTP(w, r) // the first call creates the limiter entry

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h.ServeHTTP(w, r)
		}
	})
}

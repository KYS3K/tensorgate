package lb

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/KYS3K/tensorgate/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type metered struct {
	gw  *httptest.Server
	p   *Pool
	reg *prometheus.Registry
}

func startMetered(t *testing.T, cfg Config, upstreams ...*httptest.Server) metered {
	t.Helper()
	for _, s := range upstreams {
		u, err := url.Parse(s.URL)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Upstreams = append(cfg.Upstreams, u)
	}
	p, err := NewPool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	h, err := NewHandler(p, time.Second, 0, m)
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(h)
	t.Cleanup(gw.Close)
	return metered{gw: gw, p: p, reg: reg}
}

// value returns the counter or gauge sample with exactly these labels, 0 if there is none.
func (m metered) value(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	fams, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
	next:
		for _, s := range f.GetMetric() {
			got := map[string]string{}
			for _, l := range s.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			if len(got) != len(labels) {
				continue
			}
			for k, v := range labels {
				if got[k] != v {
					continue next
				}
			}
			switch {
			case s.Counter != nil:
				return s.GetCounter().GetValue()
			case s.Gauge != nil:
				return s.GetGauge().GetValue()
			}
		}
	}
	return 0
}

// ttft returns how many first-byte observations were made, over all upstreams, and their sum in seconds.
func (m metered) ttft(t *testing.T) (count uint64, sum float64) {
	t.Helper()
	fams, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() == "tensorgate_lb_ttft_seconds" {
			for _, s := range f.GetMetric() {
				count += s.GetHistogram().GetSampleCount()
				sum += s.GetHistogram().GetSampleSum()
			}
		}
	}
	return count, sum
}

// has reports whether a series with exactly these labels exists, even at 0.
func (m metered) has(t *testing.T, name string, labels map[string]string) bool {
	t.Helper()
	fams, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
	next:
		for _, s := range f.GetMetric() {
			got := map[string]string{}
			for _, l := range s.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			if len(got) != len(labels) {
				continue
			}
			for k, v := range labels {
				if got[k] != v {
					continue next
				}
			}
			return true
		}
	}
	return false
}

func (m metered) waitValue(t *testing.T, name string, labels map[string]string, want float64) {
	t.Helper()
	waitFor(t, name, func() bool { return m.value(t, name, labels) == want })
}

func get(t *testing.T, url string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

const requests = "tensorgate_lb_requests_total"

func TestMetrics_CountsByBackendAndClass(t *testing.T) {
	ok := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bad" {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if r.URL.Path == "/nope" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = io.WriteString(w, "hi")
	})
	m := startMetered(t, Config{}, ok)
	host := m.p.Backends()[0].URL.Host

	for range 3 {
		get(t, m.gw.URL)
	}
	get(t, m.gw.URL+"/bad")
	get(t, m.gw.URL+"/nope")

	for class, want := range map[string]float64{"2xx": 3, "5xx": 1, "4xx": 1} {
		m.waitValue(t, requests, map[string]string{"backend": host, "class": class}, want)
	}
}

func TestMetrics_DeadUpstreamIs5xx(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {})
	m := startMetered(t, Config{}, up)
	host := m.p.Backends()[0].URL.Host
	up.Close()

	get(t, m.gw.URL)

	m.waitValue(t, requests, map[string]string{"backend": host, "class": "5xx"}, 1)
}

func TestMetrics_NoBackendIsCountedWithoutTTFT(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {})
	m := startMetered(t, Config{}, up)
	m.p.Backends()[0].alive.Store(false)

	get(t, m.gw.URL)

	m.waitValue(t, requests, map[string]string{"backend": noBackend, "class": "no_backend"}, 1)
	if n, _ := m.ttft(t); n != 0 {
		t.Errorf("ttft observed %d times for a request that never reached a backend", n)
	}
}

func TestMetrics_ClientCancelIsCanceledWithoutTTFT(t *testing.T) {
	started := make(chan struct{})
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	})
	m := startMetered(t, Config{}, up)
	host := m.p.Backends()[0].URL.Host

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, m.gw.URL, nil)
	go func() {
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	<-started
	cancel()

	m.waitValue(t, requests, map[string]string{"backend": host, "class": "canceled"}, 1)
	if n, _ := m.ttft(t); n != 0 {
		t.Errorf("ttft observed %d times, nothing was written to the client", n)
	}
}

func TestMetrics_UpstreamCutMidStreamIsAborted(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: one\n\n")
		w.(http.Flusher).Flush()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	})
	m := startMetered(t, Config{}, up)
	host := m.p.Backends()[0].URL.Host

	resp, err := http.Get(m.gw.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	m.waitValue(t, requests, map[string]string{"backend": host, "class": "aborted"}, 1)
	if n, _ := m.ttft(t); n != 1 {
		t.Errorf("ttft observed %d times, want 1: the first chunk did reach the client", n)
	}
}

// TTFT is the time to the FIRST body byte, not to the end of the stream.
func TestMetrics_TTFTIsFirstByteNotEndOfStream(t *testing.T) {
	const firstAt, endAt = 100 * time.Millisecond, 700 * time.Millisecond
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		time.Sleep(firstAt)
		_, _ = io.WriteString(w, "data: one\n\n")
		w.(http.Flusher).Flush()
		time.Sleep(endAt - firstAt)
		_, _ = io.WriteString(w, "data: two\n\n")
	})
	m := startMetered(t, Config{}, up)

	get(t, m.gw.URL)

	waitFor(t, "ttft observation", func() bool { n, _ := m.ttft(t); return n == 1 })
	_, sum := m.ttft(t)
	got := time.Duration(sum * float64(time.Second))
	if got < firstAt || got > firstAt+300*time.Millisecond {
		t.Errorf("ttft = %v, want about %v (the stream ends at %v)", got, firstAt, endAt)
	}
}

func TestMetrics_BackendGauges(t *testing.T) {
	release := make(chan struct{})
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) { <-release })
	m := startMetered(t, Config{}, up)
	b := m.p.Backends()[0]
	l := map[string]string{"backend": b.URL.Host}

	if got := m.value(t, "tensorgate_lb_backend_alive", l); got != 1 {
		t.Errorf("alive = %v, want 1", got)
	}
	b.kill()
	if got := m.value(t, "tensorgate_lb_backend_alive", l); got != 0 {
		t.Errorf("alive = %v after kill, want 0", got)
	}
	b.resurrect()

	done := make(chan struct{})
	go func() { defer close(done); get(t, m.gw.URL) }()
	waitFor(t, "inflight gauge", func() bool { return m.value(t, "tensorgate_lb_backend_inflight", l) == 1 })
	close(release)
	<-done
	waitFor(t, "inflight gauge back to 0", func() bool { return m.value(t, "tensorgate_lb_backend_inflight", l) == 0 })
}

func TestMetrics_RetryIsCounted(t *testing.T) {
	bad := newCounted(t, bad502)
	good := newCounted(t, echo)
	m := startMetered(t, retryCfg(), bad.Server, good.Server)
	busy(t, m.p, good.Server)

	get(t, m.gw.URL)

	m.waitValue(t, "tensorgate_lb_retries_total", map[string]string{}, 1)
	m.waitValue(t, requests, map[string]string{"backend": backendFor(t, m.p, good.Server).URL.Host, "class": "2xx"}, 1)
}

func TestMetrics_TTFTSkipsErrors(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	})
	m := startMetered(t, Config{}, up)

	get(t, m.gw.URL)

	m.waitValue(t, requests, map[string]string{"backend": m.p.Backends()[0].URL.Host, "class": "4xx"}, 1)
	if n, _ := m.ttft(t); n != 0 {
		t.Errorf("ttft observed %d times for an error body", n)
	}
}

func TestMetrics_DeniedRetryIsCounted(t *testing.T) {
	bad := newCounted(t, bad502)
	good := newCounted(t, echo)
	m := startMetered(t, retryCfg(), bad.Server, good.Server)
	m.p.budget = newRetryBudget(1, 10*time.Second, 0)
	busy(t, m.p, good.Server)

	get(t, m.gw.URL)

	m.waitValue(t, "tensorgate_lb_retry_budget_exhausted_total", map[string]string{}, 1)
	if got := m.value(t, "tensorgate_lb_retries_total", map[string]string{}); got != 0 {
		t.Errorf("retries_total = %v, want 0", got)
	}
}

// A header timeout is never retried, so an empty budget did not refuse anything.
func TestMetrics_TimeoutIsNotABudgetDenial(t *testing.T) {
	hang := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
	})
	spare := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {})
	cfg := retryCfg()
	cfg.ResponseHeaderTimeout = 100 * time.Millisecond
	m := startMetered(t, cfg, hang, spare)
	m.p.budget = newRetryBudget(1, 10*time.Second, 0)
	busy(t, m.p, spare)

	get(t, m.gw.URL)

	m.waitValue(t, requests, map[string]string{"backend": backendFor(t, m.p, hang).URL.Host, "class": "5xx"}, 1)
	if got := m.value(t, "tensorgate_lb_retry_budget_exhausted_total", map[string]string{}); got != 0 {
		t.Errorf("budget_exhausted = %v for a timeout, want 0", got)
	}
}

// http://a and https://a share a host: their gauges must not collide, or NewHandler panics.
func TestMetrics_SameHostDifferentSchemeHasDistinctNames(t *testing.T) {
	a, _ := url.Parse("http://127.0.0.1:8081")
	b, _ := url.Parse("https://127.0.0.1:8081")
	p, err := NewPool(Config{Upstreams: []*url.URL{a, b}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)

	if _, err := NewHandler(p, time.Second, 0, metrics.New(prometheus.NewRegistry())); err != nil {
		t.Fatal(err)
	}
	if n := p.Backends(); n[0].Name == n[1].Name {
		t.Errorf("both backends are named %q", n[0].Name)
	}
}

// A counter born by its first increment is invisible to rate() and increase(): the game day lost
// 267 aborted streams that way. Every series must exist at 0 from the start.
func TestMetrics_SeriesExistAtZeroFromTheStart(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {})
	m := startMetered(t, Config{}, up)
	host := m.p.Backends()[0].URL.Host

	for _, class := range metrics.LBClasses {
		labels := map[string]string{"backend": host, "class": class}
		if !m.has(t, requests, labels) {
			t.Errorf("no series for %v before any request", labels)
		}
		if got := m.value(t, requests, labels); got != 0 {
			t.Errorf("%v = %v before any request, want 0", labels, got)
		}
	}
	for _, class := range []string{"no_backend", "4xx"} {
		if !m.has(t, requests, map[string]string{"backend": noBackend, "class": class}) {
			t.Errorf("no series for backend=%q class=%q before any request", noBackend, class)
		}
	}
	if !m.has(t, "tensorgate_lb_ttft_seconds", map[string]string{"backend": host}) {
		t.Error("no TTFT histogram for the upstream before any request")
	}
}

func TestMetrics_TTFTIsPerUpstream(t *testing.T) {
	slow := newCounted(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		_, _ = io.WriteString(w, "x")
	})
	fast := newCounted(t, echo)
	m := startMetered(t, Config{}, slow.Server, fast.Server)
	busy(t, m.p, fast.Server)

	get(t, m.gw.URL)

	slowHost := backendFor(t, m.p, slow.Server).URL.Host
	fams, _ := m.reg.Gather()
	for _, f := range fams {
		if f.GetName() != "tensorgate_lb_ttft_seconds" {
			continue
		}
		for _, s := range f.GetMetric() {
			host := s.GetLabel()[0].GetValue()
			n, sum := s.GetHistogram().GetSampleCount(), s.GetHistogram().GetSampleSum()
			if host == slowHost && (n != 1 || sum < 0.15) {
				t.Errorf("slow upstream: %d observations, sum %vs, want 1 observation of at least 0.15s", n, sum)
			}
			if host != slowHost && n != 0 {
				t.Errorf("the other upstream got %d observations, want 0", n)
			}
		}
	}
}

package lb

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// healthCfg is a config for fast tests: a probe every 60ms, 30ms to answer,
// two probes in a row decide in either direction.
func healthCfg() Config {
	return Config{
		CheckInterval: 60 * time.Millisecond,
		CheckTimeout:  30 * time.Millisecond,
		FailThreshold: 2,
		RiseThreshold: 2,
	}
}

// healthUpstream is a mock upstream whose /healthz answer can be switched at runtime.
type healthUpstream struct {
	*httptest.Server
	healthy atomic.Bool
	hits    atomic.Int64 // number of /healthz requests that reached the server
}

func newHealthUpstream(t *testing.T, healthy bool) *healthUpstream {
	t.Helper()
	u := &healthUpstream{}
	u.healthy.Store(healthy)
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		u.hits.Add(1)
		if u.healthy.Load() {
			_, _ = w.Write([]byte("ok"))
			return
		}
		http.Error(w, "sick", http.StatusInternalServerError)
	}))
	t.Cleanup(u.Close)
	return u
}

// startChecks builds a pool over the urls and runs RunHealthChecks on it until
// the test ends. Cleanup waits for RunHealthChecks to return.
func startChecks(t *testing.T, cfg Config, urls ...string) *Pool {
	t.Helper()
	ups := make([]*url.URL, 0, len(urls))
	for _, s := range urls {
		u, err := url.Parse(s)
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

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunHealthChecks(ctx, p, cfg)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		p.Close()
	})
	return p
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// ---- probe: one check of one backend ----

func TestProbe_StatusCodes(t *testing.T) {
	tests := []struct {
		status int
		want   bool
	}{
		{http.StatusOK, true},
		{http.StatusNoContent, true},
		{http.StatusNotFound, false},
		{http.StatusInternalServerError, false},
		{http.StatusServiceUnavailable, false},
	}
	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.status), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
			}))
			defer s.Close()
			client := &http.Client{Transport: &http.Transport{}}
			defer client.CloseIdleConnections()

			if got := probe(context.Background(), client, s.URL, healthCfg()); got != tt.want {
				t.Errorf("probe() with status %d = %v, want %v", tt.status, got, tt.want)
			}
		})
	}
}

// A refused connection leaves resp == nil; touching it before checking err panics.
func TestProbe_DeadServerIsFalseNotPanic(t *testing.T) {
	s := httptest.NewServer(http.NotFoundHandler())
	addr := s.URL
	s.Close()
	client := &http.Client{Transport: &http.Transport{}}

	if probe(context.Background(), client, addr, healthCfg()) {
		t.Fatal("probe() = true for a server that is down")
	}
}

func TestProbe_SlowServerTimesOutInTime(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer s.Close()
	client := &http.Client{Transport: &http.Transport{}}
	defer client.CloseIdleConnections()

	start := time.Now()
	ok := probe(context.Background(), client, s.URL, healthCfg())
	elapsed := time.Since(start)

	if ok {
		t.Error("probe() = true for a server slower than CheckTimeout")
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("probe took %v, CheckTimeout is %v: the timeout is not applied", elapsed, healthCfg().CheckTimeout)
	}
}

func TestProbe_CanceledContext(t *testing.T) {
	s := newHealthUpstream(t, true)
	client := &http.Client{Transport: &http.Transport{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if probe(ctx, client, s.URL+"/healthz", healthCfg()) {
		t.Error("probe() = true with a canceled context")
	}
}

// A healthy probe must drain the body BEFORE closing it, otherwise the transport
// throws the connection away and every probe opens a new TCP connection.
func TestProbe_ReusesConnection(t *testing.T) {
	var conns atomic.Int64
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	s.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		if st == http.StateNew {
			conns.Add(1)
		}
	}
	s.Start()
	defer s.Close()

	client := &http.Client{Transport: &http.Transport{}}
	defer client.CloseIdleConnections()

	for range 5 {
		if !probe(context.Background(), client, s.URL, healthCfg()) {
			t.Fatal("probe() = false for a healthy server")
		}
	}
	if got := conns.Load(); got != 1 {
		t.Errorf("5 probes opened %d TCP connections, want 1 (body is not drained before Close)", got)
	}
}

// ---- RunHealthChecks: the state machine ----

func TestHealth_DeadUpstreamIsKilled(t *testing.T) {
	s := httptest.NewServer(http.NotFoundHandler())
	addr := s.URL
	s.Close() // nothing listens there anymore

	p := startChecks(t, healthCfg(), addr)
	b := p.Backends()[0]

	waitFor(t, "backend to die", func() bool { return !b.Alive() })
}

func TestHealth_UnhealthyStatusIsKilled(t *testing.T) {
	s := newHealthUpstream(t, false)
	p := startChecks(t, healthCfg(), s.URL)
	b := p.Backends()[0]

	waitFor(t, "backend to die", func() bool { return !b.Alive() })
}

func TestHealth_SlowUpstreamIsKilled(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer s.Close()

	p := startChecks(t, healthCfg(), s.URL)
	b := p.Backends()[0]

	waitFor(t, "slow backend to die", func() bool { return !b.Alive() })
}

// It must take FailThreshold failures, not fewer.
func TestHealth_KillNeedsFullThreshold(t *testing.T) {
	cfg := healthCfg()
	cfg.FailThreshold = 3
	s := newHealthUpstream(t, false)
	p := startChecks(t, cfg, s.URL)
	b := p.Backends()[0]

	waitFor(t, "backend to die", func() bool { return !b.Alive() })
	if got := s.hits.Load(); got < 3 {
		t.Errorf("backend died after %d failed probes, want at least FailThreshold=3", got)
	}
}

// With the zero config the thresholds come from the defaults. Without
// withDefaults FailThreshold = 0 and the backend dies on the first tick.
func TestHealth_ZeroConfigDoesNotKillHealthyBackend(t *testing.T) {
	s := newHealthUpstream(t, true)
	ups, _ := url.Parse(s.URL)
	p, err := NewPool(Config{Upstreams: []*url.URL{ups}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunHealthChecks(ctx, p, Config{}) // zero config on purpose
	}()
	defer func() { cancel(); <-done; p.Close() }()

	// the default interval is 2s, give it two ticks
	time.Sleep(4500 * time.Millisecond)
	if !p.Backends()[0].Alive() {
		t.Fatal("healthy backend was killed under the zero config")
	}
	if s.hits.Load() == 0 {
		t.Fatal("no probes were made")
	}
}

func TestHealth_RevivesAndResetsPassiveFails(t *testing.T) {
	s := newHealthUpstream(t, false)
	p := startChecks(t, healthCfg(), s.URL)
	b := p.Backends()[0]

	waitFor(t, "backend to die", func() bool { return !b.Alive() })

	b.passiveFails.Store(2) // leftovers of the passive streak
	s.healthy.Store(true)

	waitFor(t, "backend to come back", func() bool { return b.Alive() })
	if got := b.passiveFails.Load(); got != 0 {
		t.Errorf("passiveFails = %d after revival, want 0: the next single 502 would kill it again", got)
	}
}

// Rise needs RiseThreshold successes in a row; one lucky answer is not enough.
func TestHealth_ReviveNeedsFullThreshold(t *testing.T) {
	cfg := healthCfg()
	cfg.RiseThreshold = 3
	s := newHealthUpstream(t, false)
	p := startChecks(t, cfg, s.URL)
	b := p.Backends()[0]

	waitFor(t, "backend to die", func() bool { return !b.Alive() })
	before := s.hits.Load()
	s.healthy.Store(true)

	waitFor(t, "backend to come back", func() bool { return b.Alive() })
	if got := s.hits.Load() - before; got < 3 {
		t.Errorf("revived after %d healthy probes, want at least RiseThreshold=3", got)
	}
}

// While the backend is alive, healthy probes must not touch the passive
// counter: otherwise passive checks never get to accumulate a streak.
func TestHealth_HealthyProbesDoNotResetPassiveStreak(t *testing.T) {
	s := newHealthUpstream(t, true)
	p := startChecks(t, healthCfg(), s.URL)
	b := p.Backends()[0]
	b.passiveFails.Store(2)

	waitFor(t, "several probes", func() bool { return s.hits.Load() >= 6 })

	if got := b.passiveFails.Load(); got != 2 {
		t.Errorf("passiveFails = %d, want 2: resurrect is called on a backend that is already alive", got)
	}
}

// A healthy backend must stay alive far beyond one CheckTimeout. Catches a
// timeout context created once outside the loop: it expires and every later
// probe fails without even reaching the server.
func TestHealth_HealthyStaysAlive(t *testing.T) {
	s := newHealthUpstream(t, true)
	p := startChecks(t, healthCfg(), s.URL)
	b := p.Backends()[0]

	waitFor(t, "8 probes to reach the server", func() bool { return s.hits.Load() >= 8 })
	if !b.Alive() {
		t.Fatal("healthy backend was killed")
	}
}

// Failures that are not consecutive must never kill: F,S,F,S,... with
// FailThreshold = 2. Catches counters that are not reset by the opposite result.
func TestHealth_FlappingBelowThresholdStaysAlive(t *testing.T) {
	var hits atomic.Int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1)%2 == 1 {
			http.Error(w, "sick", http.StatusInternalServerError)
		}
	}))
	defer s.Close()

	p := startChecks(t, healthCfg(), s.URL)
	b := p.Backends()[0]

	for hits.Load() < 16 {
		if !b.Alive() {
			t.Fatalf("backend died after %d probes, but there were never 2 failures in a row", hits.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// One sick backend must not affect the other one (no shared counters).
func TestHealth_BackendsAreIndependent(t *testing.T) {
	bad := newHealthUpstream(t, false)
	good := newHealthUpstream(t, true)
	p := startChecks(t, healthCfg(), bad.URL, good.URL)

	waitFor(t, "bad backend to die", func() bool { return !p.Backends()[0].Alive() })
	waitFor(t, "good backend probed several times", func() bool { return good.hits.Load() >= 8 })

	if !p.Backends()[1].Alive() {
		t.Error("healthy backend died together with the sick one")
	}
	if p.Backends()[0].Alive() {
		t.Error("sick backend came back to life")
	}
}

// The probes go out through the pool, so a dead backend is not picked.
func TestHealth_DeadBackendIsSkippedByPick(t *testing.T) {
	bad := newHealthUpstream(t, false)
	good := newHealthUpstream(t, true)
	p := startChecks(t, healthCfg(), bad.URL, good.URL)

	waitFor(t, "bad backend to die", func() bool { return !p.Backends()[0].Alive() })
	for range 50 {
		b := p.Pick()
		if b == nil || b != p.Backends()[1] {
			t.Fatalf("Pick() = %v, want the healthy backend", b)
		}
		b.Release()
	}
}

// ---- shutdown ----

func TestHealth_StopsOnContextCancelWithoutLeaks(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	s := newHealthUpstream(t, true)
	ups, _ := url.Parse(s.URL)
	cfg := healthCfg()
	cfg.Upstreams = []*url.URL{ups, ups}
	// the same URL twice is rejected by ParseUpstreams only, the pool itself does not mind
	p, err := NewPool(cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunHealthChecks(ctx, p, cfg)
	}()

	waitFor(t, "first probes", func() bool { return s.hits.Load() >= 2 })
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunHealthChecks did not return after the context was canceled")
	}

	after := s.hits.Load()
	time.Sleep(3 * healthCfg().CheckInterval)
	if got := s.hits.Load(); got > after {
		t.Errorf("%d probes were made after the stop", got-after)
	}

	p.Close()
	s.Close()
}

// A probe that is cut by the shutdown is not a backend failure.
func TestHealth_ShutdownDoesNotKillBackends(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer s.Close()

	ups, _ := url.Parse(s.URL)
	cfg := healthCfg()
	cfg.FailThreshold = 1 // a single failed probe would be enough to kill
	cfg.CheckTimeout = 500 * time.Millisecond
	cfg.CheckInterval = 600 * time.Millisecond
	cfg.Upstreams = []*url.URL{ups}
	p, err := NewPool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunHealthChecks(ctx, p, cfg)
	}()

	time.Sleep(cfg.CheckInterval + 100*time.Millisecond) // the probe is in flight now
	cancel()
	<-done

	if !p.Backends()[0].Alive() {
		t.Error("backend was killed by a probe that was canceled by the shutdown")
	}
}

// Probes must be paced by CheckInterval. A loop without a ticker (or with a
// default branch in the select) hammers /healthz as fast as it can.
func TestHealth_ProbesArePacedByInterval(t *testing.T) {
	s := newHealthUpstream(t, true)
	startChecks(t, healthCfg(), s.URL) // 60ms interval

	time.Sleep(600 * time.Millisecond)
	got := s.hits.Load()
	// about 10 expected; allow slack for a slow CI machine, but a busy loop gives thousands
	if got < 3 || got > 15 {
		t.Errorf("%d probes in 600ms with a 60ms interval, want about 10", got)
	}
}

// Healthy probes while alive must not bank successes: a passive ejection of an
// upstream with a fine /healthz then lasts RiseThreshold probes, not one.
func TestHealth_PassiveEjectionIsNotUndoneByTheNextProbe(t *testing.T) {
	cfg := healthCfg()
	cfg.RiseThreshold = 3
	s := newHealthUpstream(t, true)
	p := startChecks(t, cfg, s.URL)
	b := p.Backends()[0]

	waitFor(t, "a few healthy probes", func() bool { return s.hits.Load() >= 5 })
	b.kill()
	before := s.hits.Load()

	waitFor(t, "backend to come back", func() bool { return b.Alive() })
	if got := s.hits.Load() - before; got < 3 {
		t.Errorf("revived after %d probes since the ejection, want at least RiseThreshold=3", got)
	}
}

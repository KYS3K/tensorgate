package lb

import (
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"
)

func testPool(t *testing.T, n int) *Pool {
	t.Helper()
	ups := make([]*url.URL, 0, n)
	for i := range n {
		u, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", 8081+i))
		if err != nil {
			t.Fatal(err)
		}
		ups = append(ups, u)
	}
	p, err := NewPool(Config{Upstreams: ups})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestNewPool_BuildsOneBackendPerUpstream(t *testing.T) {
	p := testPool(t, 3)

	bs := p.Backends()
	if len(bs) != 3 {
		t.Fatalf("len(Backends()) = %d, want 3", len(bs))
	}
	for i, b := range bs {
		if b == nil {
			t.Fatalf("backend %d is nil", i)
		}
		want := fmt.Sprintf("127.0.0.1:%d", 8081+i)
		if b.URL.Host != want {
			t.Errorf("backend %d host = %q, want %q (order must be kept)", i, b.URL.Host, want)
		}
		if !b.Alive() {
			t.Errorf("backend %d must start alive", i)
		}
	}
}

func TestNewPool_Errors(t *testing.T) {
	u, _ := url.Parse("http://127.0.0.1:8081")

	tests := []struct {
		name string
		cfg  Config
	}{
		{"no upstreams", Config{}},
		{"nil upstream", Config{Upstreams: []*url.URL{u, nil}}},
		{
			"check timeout not below interval",
			Config{Upstreams: []*url.URL{u}, CheckInterval: time.Second, CheckTimeout: time.Second},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewPool(tt.cfg)
			if err == nil {
				t.Fatalf("NewPool() = %v, want error", p)
			}
			if p != nil {
				t.Errorf("NewPool() returned a pool together with an error")
			}
		})
	}
}

func TestBackends_ReturnsCopy(t *testing.T) {
	p := testPool(t, 2)

	bs := p.Backends()
	bs[0] = nil

	if p.Backends()[0] == nil {
		t.Error("modifying the returned slice changed the pool")
	}
}

func TestPick_ChoosesLeastLoaded(t *testing.T) {
	p := testPool(t, 3)
	bs := p.Backends()
	for range 3 {
		bs[0].acquire()
	}
	bs[1].acquire()
	for range 2 {
		bs[2].acquire()
	}

	got := p.Pick()
	if got != bs[1] {
		t.Fatalf("Pick() chose %v, want the backend with inFlight=1 (%v)", got.URL.Host, bs[1].URL.Host)
	}
	if got.InFlight() != 2 {
		t.Errorf("InFlight() = %d, want 2: Pick must acquire the backend it returns", got.InFlight())
	}
}

func TestPick_SkipsDead(t *testing.T) {
	p := testPool(t, 3)
	bs := p.Backends()
	bs[0].alive.Store(false) // idle, but dead
	bs[1].acquire()
	bs[2].acquire()
	bs[2].acquire()

	if got := p.Pick(); got != bs[1] {
		t.Fatalf("Pick() = %v, want %v", got.URL.Host, bs[1].URL.Host)
	}
	if bs[0].InFlight() != 0 {
		t.Errorf("dead backend was acquired: inFlight = %d", bs[0].InFlight())
	}
}

func TestPick_AllDeadReturnsNilAndDoesNotAcquire(t *testing.T) {
	p := testPool(t, 3)
	for _, b := range p.Backends() {
		b.alive.Store(false)
	}

	if got := p.Pick(); got != nil {
		t.Fatalf("Pick() = %v, want nil when every backend is dead", got.URL.Host)
	}
	for _, b := range p.Backends() {
		if b.InFlight() != 0 {
			t.Errorf("%s inFlight = %d, want 0", b.URL.Host, b.InFlight())
		}
	}
}

// With every backend idle all of them tie. If Pick always takes the first one
// of a tie, the first backend gets all of the light traffic.
func TestPick_SpreadsTies(t *testing.T) {
	p := testPool(t, 3)

	const rounds = 3000
	hits := make(map[*Backend]int)
	for range rounds {
		b := p.Pick()
		hits[b]++
		b.Release()
	}

	for _, b := range p.Backends() {
		if hits[b] < rounds/10 {
			t.Errorf("%s was picked %d of %d times, ties are not spread: %v",
				b.URL.Host, hits[b], rounds, hits)
		}
	}
}

func TestPick_ConcurrentBalanced(t *testing.T) {
	p := testPool(t, 4)

	const workers, iters = 100, 1000
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iters {
				b := p.Pick()
				if b == nil {
					t.Error("Pick() = nil with live backends")
					return
				}
				b.Release()
			}
		}()
	}
	wg.Wait()

	for _, b := range p.Backends() {
		if b.InFlight() != 0 {
			t.Errorf("%s inFlight = %d after all releases, want 0", b.URL.Host, b.InFlight())
		}
	}
}

func TestPick_NoAllocations(t *testing.T) {
	p := testPool(t, 4)

	allocs := testing.AllocsPerRun(1000, func() {
		p.Pick().Release()
	})
	if allocs != 0 {
		t.Errorf("Pick+Release allocates %v times per call, want 0", allocs)
	}
}

// A single upstream is the backward-compatible case, it must work.
func TestPick_SingleBackend(t *testing.T) {
	p := testPool(t, 1)

	for range 100 {
		b := p.Pick()
		if b == nil {
			t.Fatal("Pick() = nil with one live backend")
		}
		b.Release()
	}
}

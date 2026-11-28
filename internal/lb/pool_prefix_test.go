package lb

import (
	"fmt"
	"math"
	"math/rand/v2"
	"net/url"
	"sync"
	"testing"

	"github.com/KYS3K/tensorgate/internal/metrics"
)

func prefixPool(t *testing.T, n int) *Pool {
	t.Helper()
	ups := make([]*url.URL, 0, n)
	for i := range n {
		u, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", 8081+i))
		if err != nil {
			t.Fatal(err)
		}
		ups = append(ups, u)
	}
	p, err := NewPool(Config{Upstreams: ups, Balancing: BalancingPrefix})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestNewPool_UnknownBalancing(t *testing.T) {
	u, _ := url.Parse("http://127.0.0.1:8081")
	if p, err := NewPool(Config{Upstreams: []*url.URL{u}, Balancing: "round-robin"}); err == nil {
		p.Close()
		t.Fatal("NewPool accepted an unknown balancing")
	}
}

func TestPickFor_LORModeHasNoRoute(t *testing.T) {
	p := testPool(t, 3)
	b, route := p.pickFor(123, true, nil)
	if b == nil || route != "" {
		t.Fatalf("pickFor = %v, %q, want a backend and no route", b, route)
	}
	if _, ok := p.routeKey([]byte(`{"messages":[]}`)); ok {
		t.Fatal("least-outstanding mode computes a prefix key")
	}
}

func TestPickFor_SameKeyGoesToTheSameBackend(t *testing.T) {
	p := prefixPool(t, 4)
	home, route := p.pickFor(42, true, nil)
	if route != metrics.LBRouteHome {
		t.Fatalf("route = %q, want home", route)
	}
	home.Release()
	for range 50 {
		b, route := p.pickFor(42, true, nil)
		if b != home || route != metrics.LBRouteHome {
			t.Fatalf("pickFor = %v, %q; want %v, home", b.URL.Host, route, home.URL.Host)
		}
		b.Release()
	}
}

func TestPickFor_NoKeyIsLeastOutstanding(t *testing.T) {
	p := prefixPool(t, 3)
	bs := p.Backends()
	bs[0].acquire()
	bs[0].acquire()
	bs[1].acquire()
	b, route := p.pickFor(0, false, nil)
	if b != bs[2] || route != metrics.LBRouteNoKey {
		t.Fatalf("pickFor = %v, %q; want the idle %v, no_key", b.URL.Host, route, bs[2].URL.Host)
	}
}

// one hot key must not bury its backend: past the cap the next ones on the ring take over
func TestPickFor_HotKeySpillsAndNobodyGoesOverTheCap(t *testing.T) {
	const picks = 200
	p := prefixPool(t, 4)
	var spills int
	for i := range picks {
		var total, alive int64
		for _, b := range p.Backends() {
			alive++
			total += b.InFlight()
		}
		limit := int64(math.Ceil(1.25 * float64(total+1) / float64(alive)))

		b, route := p.pickFor(777, true, nil)
		if b == nil {
			t.Fatalf("pick %d: no backend", i)
		}
		if b.InFlight() > limit {
			t.Fatalf("pick %d: %s has %d in flight, the cap is %d", i, b.URL.Host, b.InFlight(), limit)
		}
		if route == metrics.LBRouteSpill {
			spills++
		}
	}
	used := 0
	for _, b := range p.Backends() {
		if b.InFlight() > 0 {
			used++
		}
	}
	if used < 3 || spills == 0 {
		t.Fatalf("the hot key reached %d of 4 backends with %d spills", used, spills)
	}
}

func TestPickFor_DeadHomeIsSkippedAndComesBack(t *testing.T) {
	p := prefixPool(t, 4)
	home, _ := p.pickFor(42, true, nil)
	home.Release()

	home.alive.Store(false)
	b, route := p.pickFor(42, true, nil)
	if b == nil || b == home || route != metrics.LBRouteSpill {
		t.Fatalf("pickFor with a dead home = %v, %q", b, route)
	}
	if home.InFlight() != 0 {
		t.Errorf("the dead backend was acquired")
	}
	b.Release()

	home.alive.Store(true)
	back, route := p.pickFor(42, true, nil)
	if back != home || route != metrics.LBRouteHome {
		t.Fatalf("after the home came back pickFor = %v, %q", back.URL.Host, route)
	}
}

func TestPickFor_OnlyTheKeysOfADeadBackendMove(t *testing.T) {
	p := prefixPool(t, 4)
	rng := rand.New(rand.NewPCG(5, 6))
	keys := make([]uint64, 2000)
	before := make([]*Backend, len(keys))
	for i := range keys {
		keys[i] = rng.Uint64()
		before[i], _ = p.pickFor(keys[i], true, nil)
		before[i].Release()
	}
	dead := p.Backends()[2]
	dead.alive.Store(false)
	for i, k := range keys {
		b, _ := p.pickFor(k, true, nil)
		b.Release()
		if before[i] != dead && b != before[i] {
			t.Fatalf("key %d moved from %s to %s though %s stayed alive", k, before[i].URL.Host, b.URL.Host, before[i].URL.Host)
		}
		if b == dead {
			t.Fatalf("key %d still goes to the dead backend", k)
		}
	}
}

func TestPickFor_SkipsTriedBackends(t *testing.T) {
	p := prefixPool(t, 3)
	first, _ := p.pickFor(9, true, nil)
	first.Release()

	second, _ := p.pickFor(9, true, []*Backend{first})
	if second == nil || second == first {
		t.Fatalf("retry got %v, the one that already failed was %v", second, first.URL.Host)
	}
	second.Release()

	if b, _ := p.pickFor(9, true, p.Backends()); b != nil {
		t.Fatalf("every backend was tried, got %v", b.URL.Host)
	}
}

func TestPickFor_AllDead(t *testing.T) {
	p := prefixPool(t, 3)
	for _, b := range p.Backends() {
		b.alive.Store(false)
	}
	if b, _ := p.pickFor(1, true, nil); b != nil {
		t.Fatal("picked a dead backend")
	}
}

func TestPickFor_SpreadsDistinctKeys(t *testing.T) {
	p := prefixPool(t, 4)
	rng := rand.New(rand.NewPCG(1, 2))
	count := map[*Backend]int{}
	const n = 8000
	for range n {
		b, _ := p.pickFor(rng.Uint64(), true, nil)
		count[b]++
		b.Release()
	}
	for b, c := range count {
		if share := float64(c) / n; share < 0.15 || share > 0.35 {
			t.Errorf("%s got %.0f%% of the keys, want about 25%%", b.URL.Host, share*100)
		}
	}
	if len(count) != 4 {
		t.Errorf("only %d of 4 backends got keys", len(count))
	}
}

func TestPickFor_SingleBackendTakesEverything(t *testing.T) {
	p := prefixPool(t, 1)
	for range 100 {
		if b, route := p.pickFor(5, true, nil); b == nil || route != metrics.LBRouteHome {
			t.Fatalf("pickFor = %v, %q", b, route)
		}
	}
}

func TestPickFor_Concurrent(t *testing.T) {
	p := prefixPool(t, 4)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 1))
			for range 5000 {
				b, _ := p.pickFor(rng.Uint64()%50, true, nil)
				if b != nil {
					b.Release()
				}
			}
		}()
	}
	wg.Wait()
	for _, b := range p.Backends() {
		if b.InFlight() != 0 {
			t.Errorf("%s leaked %d in-flight requests", b.URL.Host, b.InFlight())
		}
	}
}

func TestPickFor_NoAllocations(t *testing.T) {
	p := prefixPool(t, 4)
	allocs := testing.AllocsPerRun(1000, func() {
		b, _ := p.pickFor(42, true, nil)
		b.Release()
	})
	if allocs != 0 {
		t.Errorf("pickFor allocates %.1f times per call", allocs)
	}
}

func BenchmarkPickFor(b *testing.B) {
	p := prefixPool(&testing.T{}, 8)
	rng := rand.New(rand.NewPCG(1, 2))
	keys := make([]uint64, 1<<12)
	for i := range keys {
		keys[i] = rng.Uint64()
	}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		x, _ := p.pickFor(keys[i&(len(keys)-1)], true, nil)
		x.Release()
	}
}

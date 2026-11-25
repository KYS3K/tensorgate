package lb

import (
	"math/rand/v2"
	"net/http"
	"sync"
	"testing"
)

func failUntilDead(b *Backend) {
	for range 10 {
		b.onFail()
	}
}

func aliveCount(p *Pool) int {
	n := 0
	for _, b := range p.Backends() {
		if b.Alive() {
			n++
		}
	}
	return n
}

func TestEject_NeverLeavesLessThanHalfAlive(t *testing.T) {
	for _, tt := range []struct{ n, wantAlive int }{{1, 1}, {2, 1}, {3, 2}, {4, 2}, {5, 3}} {
		p := testPool(t, tt.n)
		for _, b := range p.Backends() {
			failUntilDead(b)
		}
		if got := aliveCount(p); got != tt.wantAlive {
			t.Errorf("pool of %d: %d alive after every backend failed, want %d", tt.n, got, tt.wantAlive)
		}
	}
}

// Backends killed by the health probes count as dead: passive ejection must not go below half on top of them.
func TestEject_CountsBackendsKilledByProbes(t *testing.T) {
	p := testPool(t, 3)
	bs := p.Backends()
	bs[0].kill()

	failUntilDead(bs[1])

	if !bs[1].Alive() {
		t.Error("the second backend was ejected: only one of three would stay alive")
	}
}

// The probes tell the truth about /healthz: the limit does not apply to them.
func TestEject_ProbesMayKillTheLastBackend(t *testing.T) {
	p := testPool(t, 1)
	b := p.Backends()[0]

	failUntilDead(b)
	if !b.Alive() {
		t.Fatal("the only backend was ejected by real requests")
	}
	b.kill()
	if b.Alive() {
		t.Error("kill() did not kill the only backend")
	}
}

func TestEject_DeadBackendIsNotCountedTwice(t *testing.T) {
	p := testPool(t, 4)
	bs := p.Backends()
	failUntilDead(bs[0])
	failUntilDead(bs[0]) // already dead: must not take a second place in the limit

	failUntilDead(bs[1])

	if aliveCount(p) != 2 {
		t.Errorf("%d alive, want 2: two ejections of four are allowed", aliveCount(p))
	}
}

func TestEject_ConcurrentFailuresRespectTheLimit(t *testing.T) {
	p := testPool(t, 6)

	var wg sync.WaitGroup
	for _, b := range p.Backends() {
		for range 8 {
			wg.Go(func() {
				for range 20 {
					b.onFail()
				}
			})
		}
	}
	wg.Wait()

	if got := aliveCount(p); got != 3 {
		t.Errorf("%d of 6 alive after concurrent failures, want exactly 3", got)
	}
}

// Every backend fails a third of its requests. Ejecting all of them would answer 503 to
// everybody although two thirds of the requests would succeed.
func TestEject_PartlyFailingFleetStillServes(t *testing.T) {
	flaky := func(w http.ResponseWriter, r *http.Request) {
		if rand.IntN(10) < 3 {
			w.WriteHeader(http.StatusBadGateway)
		}
	}
	a, b, c := newUpstream(t, flaky), newUpstream(t, flaky), newUpstream(t, flaky)
	gw, p := startGatewayCfg(t, Config{PassiveThreshold: 3, MaxAttempts: 1}, 1, 0, a, b, c)

	const n = 300
	ok := 0
	for range n {
		resp, err := http.Get(gw.URL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			ok++
		}
	}

	if ok < n/2 {
		t.Errorf("only %d of %d requests succeeded, %d backends alive: the pool ejected itself into an outage", ok, n, aliveCount(p))
	}
	if aliveCount(p) < 2 {
		t.Errorf("%d backends alive, want at least 2 of 3", aliveCount(p))
	}
}

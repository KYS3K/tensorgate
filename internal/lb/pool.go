package lb

import (
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"slices"
	"sync"

	"github.com/KYS3K/tensorgate/internal/hashring"
	"github.com/KYS3K/tensorgate/internal/metrics"
)

type Pool struct {
	backends []*Backend
	maxBody  int64

	maxAttempts int
	budget      *retryBudget

	ejectMu sync.Mutex // serializes passive ejections, see eject

	// prefix routing, ring is nil in least-outstanding mode. A dead backend stays on the ring: its keys
	// go to the next ones while it is down and come back to it when it is, so the ring never changes.
	ring         *hashring.Ring
	byName       map[string]*Backend
	blockBytes   int
	prefixBlocks int
	slack        float64
}

func NewPool(cfg Config) (*Pool, error) {
	cfg = cfg.withDefaults()
	err := cfg.validate()
	if err != nil {
		return nil, fmt.Errorf("lb: %w", err)
	}
	if len(cfg.Upstreams) == 0 {
		return nil, errors.New("lb: at least one upstream required")
	}
	backends := make([]*Backend, 0, len(cfg.Upstreams))
	for _, u := range cfg.Upstreams {
		if u == nil {
			return nil, errors.New("lb: upstream cannot be null")
		}
		b := newBackend(u, cfg)
		backends = append(backends, b)
	}
	p := &Pool{
		backends: backends,
		maxBody:  cfg.MaxBodyBytes,

		maxAttempts: cfg.MaxAttempts,
		budget:      newRetryBudget(cfg.RetryBudgetPercent, cfg.RetryBudgetWindow, cfg.RetryMinPerSec),
	}
	hosts := map[string]int{}
	for _, b := range backends {
		hosts[b.URL.Host]++
	}
	for _, b := range backends {
		b.pool = p
		if hosts[b.URL.Host] > 1 {
			b.Name = b.URL.Scheme + "://" + b.URL.Host
		}
	}
	if cfg.Balancing == BalancingPrefix {
		p.ring = hashring.New(cfg.RingVnodes)
		p.byName = make(map[string]*Backend, len(backends))
		p.blockBytes = cfg.PrefixBlockBytes
		p.prefixBlocks = cfg.PrefixBlocks
		p.slack = cfg.PrefixSlack
		for _, b := range backends {
			p.byName[b.Name] = b
			p.ring.Add(b.Name)
		}
	}
	return p, nil
}

// eject marks b dead because real requests keep failing, unless that would leave
// less than half of the pool alive. When most backends fail at once the cause is
// usually shared (a bad model, an overload), and ejecting them all turns a partly
// working service into a total outage. The health probes are not limited by this:
// they tell the truth about /healthz.
func (p *Pool) eject(b *Backend) {
	// a refused ejection repeats on every failure, so it is first decided without the lock
	if !p.mayEject(b) {
		return
	}
	p.ejectMu.Lock()
	defer p.ejectMu.Unlock()
	if p.mayEject(b) {
		b.alive.Store(false)
		log.Printf("lb: upstream %s is dead: %d requests in a row failed", b.URL.Host, b.failThreshold)
	}
}

func (p *Pool) mayEject(b *Backend) bool {
	if !b.Alive() {
		return false
	}
	alive := 0
	for _, x := range p.backends {
		if x.Alive() {
			alive++
		}
	}
	return alive-1 >= (len(p.backends)+1)/2
}

func (p *Pool) Pick() *Backend {
	return p.pick(nil)
}

func (p *Pool) otherAlive(tried []*Backend) bool {
	for _, b := range p.backends {
		if b.Alive() && !slices.Contains(tried, b) {
			return true
		}
	}
	return false
}

func (p *Pool) pick(tried []*Backend) *Backend {
	var best *Backend
	minInFlight := int64(math.MaxInt64)
	n := len(p.backends)
	start := rand.Intn(n)

	for step := 0; step < n; step++ {
		i := (step + start) % n
		if p.backends[i].Alive() && !slices.Contains(tried, p.backends[i]) {
			inFlight := p.backends[i].InFlight()
			if inFlight < minInFlight {
				best = p.backends[i]
				minInFlight = inFlight
			}
		}
	}
	if best != nil {
		best.acquire()
	}

	return best
}

// routeKey is the ring key of a request body, false if the pool does not route by prefix
// or the request has no prefix to route by.
func (p *Pool) routeKey(body []byte) (uint64, bool) {
	if p.ring == nil || len(body) == 0 {
		return 0, false
	}
	return prefixKey(body, p.blockBytes, p.prefixBlocks)
}

// pickFor chooses a backend for a request with the given ring key, skipping the tried ones,
// and acquires it. Without a key it is least-outstanding. With one it is the backend the key belongs to,
// or, if that one carries more than (1+slack) times the average load, the next one on the ring
// that does not (bounded loads). The route says which of these happened, see metrics.LBRoute*.
func (p *Pool) pickFor(key uint64, hasKey bool, tried []*Backend) (b *Backend, route string) {
	if p.ring == nil {
		return p.pick(tried), ""
	}
	if !hasKey {
		return p.pick(tried), metrics.LBRouteNoKey
	}

	var total, alive int64
	for _, x := range p.backends {
		if x.Alive() {
			alive++
			total += x.InFlight()
		}
	}
	if alive == 0 {
		return nil, metrics.LBRouteFallback
	}
	// the request being placed counts too: with an idle pool the cap is 1, not 0
	limit := int64(math.Ceil((1 + p.slack) * float64(total+1) / float64(alive)))

	asked := 0
	name, ok := p.ring.GetBounded(key, func(name string) bool {
		asked++
		x := p.byName[name]
		return x.Alive() && !slices.Contains(tried, x) && x.InFlight()+1 <= limit
	})
	if !ok {
		// the loads moved between counting and choosing
		return p.pick(tried), metrics.LBRouteFallback
	}
	b = p.byName[name]
	b.acquire()
	if asked == 1 {
		return b, metrics.LBRouteHome
	}
	return b, metrics.LBRouteSpill
}

func (p *Pool) Backends() []*Backend {
	return slices.Clone(p.backends)
}

func (p *Pool) Close() {
	for _, b := range p.backends {
		b.transport.CloseIdleConnections()
	}
}

package lb

import (
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"slices"
	"sync"
)

type Pool struct {
	backends []*Backend
	maxBody  int64

	maxAttempts int
	budget      *retryBudget

	ejectMu sync.Mutex // serializes passive ejections, see eject
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

func (p *Pool) Backends() []*Backend {
	return slices.Clone(p.backends)
}

func (p *Pool) Close() {
	for _, b := range p.backends {
		b.transport.CloseIdleConnections()
	}
}

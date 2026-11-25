package lb

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
)

type Pool struct {
	backends []*Backend
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
	return &Pool{
		backends: backends,
	}, nil
}

func (p *Pool) Pick() *Backend {
	var best *Backend
	minInFlight := int64(math.MaxInt64)
	n := len(p.backends)
	start := rand.Intn(n)

	for step := 0; step < n; step++ {
		i := (step + start) % n
		if p.backends[i].Alive() {
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

package admission

import (
	"context"
	"errors"
	"time"
)

var ErrOverloaded = errors.New("admission: gate is full")

type Gate struct {
	slots   chan struct{}
	maxWait time.Duration
}

func NewGate(maxStreams int, maxWait time.Duration) *Gate {
	if maxStreams <= 0 {
		panic("admission: maxStreams must be > 0")
	}
	if maxWait <= 0 {
		panic("admission: maxWait must be > 0")
	}
	return &Gate{
		slots:   make(chan struct{}, maxStreams),
		maxWait: maxWait,
	}
}
func (g *Gate) Acquire(ctx context.Context) (waited time.Duration, err error) {
	select {
	case g.slots <- struct{}{}:
		return 0, nil
	default:
		start := time.Now()
		timer := time.NewTimer(g.maxWait)
		defer timer.Stop()
		select {
		case g.slots <- struct{}{}:
			return time.Since(start), nil
		case <-timer.C:
			return g.maxWait, ErrOverloaded
		case <-ctx.Done():
			return time.Since(start), ctx.Err()
		}
	}
}
func (g *Gate) Release() {
	select {
	case <-g.slots:
	default:
		panic("admission: Release without Acquire")
	}
}

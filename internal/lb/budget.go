package lb

import (
	"sync/atomic"
	"time"
)

// retryBudget caps retries as a share of recent requests, so that when the
// backends are failing the retries do not double the load on them.
//
// Time is cut into one-second slots kept in a ring as long as the window. A
// retry is allowed while
//
//	retries in the window < requests in the window * percent / 100 + minPerSec * window seconds
//
// The minPerSec part is a floor: at low traffic the percentage alone would
// allow no retries at all. All methods are safe for concurrent use and do not
// block; a nil *retryBudget allows everything.
type retryBudget struct {
	now       func() time.Time
	origin    time.Time // seconds are counted from here on the monotonic clock
	percent   int64
	minPerSec int64
	slots     []budgetSlot
}

// A counter packs the second it counts (high 32 bits) and the count (low 32 bits)
// into one word, so that starting a new second and counting in it is one atomic step.
type budgetSlot struct {
	requests atomic.Uint64
	retries  atomic.Uint64
}

func newRetryBudget(percent int, window time.Duration, minPerSec int) *retryBudget {
	return &retryBudget{
		now:       time.Now,
		origin:    time.Now(),
		percent:   int64(percent),
		minPerSec: int64(minPerSec),
		slots:     make([]budgetSlot, max(1, int(window/time.Second))),
	}
}

func (b *retryBudget) second() uint32 { return uint32(b.now().Sub(b.origin) / time.Second) }

func (b *retryBudget) slot(sec uint32) *budgetSlot { return &b.slots[int(sec)%len(b.slots)] }

// add changes a counter by delta (+1 or -1) in the given second. A counter that
// still holds an older second starts over from this one. A decrement of a second
// that is already gone does nothing.
func add(c *atomic.Uint64, sec uint32, delta int) {
	for {
		v := c.Load()
		epoch, n := uint32(v>>32), uint32(v)
		switch {
		case epoch != sec && delta > 0:
			epoch, n = sec, 1
		case epoch != sec || (delta < 0 && n == 0):
			return
		default:
			n = uint32(int(n) + delta)
		}
		if c.CompareAndSwap(v, uint64(epoch)<<32|uint64(n)) {
			return
		}
	}
}

func (b *retryBudget) count(c *atomic.Uint64, sec uint32) int64 {
	v := c.Load()
	if sec-uint32(v>>32) < uint32(len(b.slots)) {
		return int64(uint32(v))
	}
	return 0
}

func (b *retryBudget) totals() (requests, retries int64) {
	sec := b.second()
	for i := range b.slots {
		requests += b.count(&b.slots[i].requests, sec)
		retries += b.count(&b.slots[i].retries, sec)
	}
	return requests, retries
}

func (b *retryBudget) limit(requests int64) int64 {
	return requests*b.percent/100 + b.minPerSec*int64(len(b.slots))
}

// deposit counts one incoming request. Retries are not counted here.
func (b *retryBudget) deposit() {
	if b == nil {
		return
	}
	sec := b.second()
	add(&b.slot(sec).requests, sec, +1)
}

// withdraw takes one retry from the budget, or reports false if none is left.
// It adds first and checks after, so that concurrent callers cannot all pass
// the check on the same last unit; under contention it may refuse a unit that was still free.
func (b *retryBudget) withdraw() bool {
	if b == nil {
		return true
	}
	sec := b.second()
	c := &b.slot(sec).retries
	add(c, sec, +1)
	requests, retries := b.totals()
	if retries > b.limit(requests) {
		add(c, sec, -1)
		return false
	}
	return true
}

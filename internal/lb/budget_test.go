package lb

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct{ sec atomic.Int64 }

func (c *fakeClock) now() time.Time { return time.Unix(c.sec.Load(), 0) }

func testBudget(percent int, window time.Duration, minPerSec int) (*retryBudget, *fakeClock) {
	c := &fakeClock{}
	c.sec.Store(1_000_000)
	b := newRetryBudget(percent, window, minPerSec)
	b.now = c.now
	b.origin = c.now()
	return b, c
}

func drain(b *retryBudget) int {
	n := 0
	for b.withdraw() {
		n++
		if n > 1_000_000 {
			return n
		}
	}
	return n
}

func TestBudget_NilAllowsEverything(t *testing.T) {
	var b *retryBudget
	b.deposit()
	if !b.withdraw() {
		t.Error("a nil budget must allow every retry")
	}
}

func TestBudget_PercentOfRequests(t *testing.T) {
	b, _ := testBudget(20, 10*time.Second, 0)
	for range 100 {
		b.deposit()
	}

	if got := drain(b); got != 20 {
		t.Errorf("100 requests at 20%% allowed %d retries, want 20", got)
	}
}

func TestBudget_FloorLetsAQuietGatewayRetry(t *testing.T) {
	b, _ := testBudget(20, 10*time.Second, 1)

	// no requests at all: 1 per second over a 10 s window
	if got := drain(b); got != 10 {
		t.Errorf("floor allowed %d retries in a 10 s window, want 10", got)
	}
}

func TestBudget_WindowForgetsOldTraffic(t *testing.T) {
	b, clock := testBudget(100, 5*time.Second, 0)
	for range 50 {
		b.deposit()
	}

	clock.sec.Add(5) // the whole window has passed
	if b.withdraw() {
		t.Error("requests from outside the window still pay for retries")
	}
}

func TestBudget_RetriesAlsoLeaveTheWindow(t *testing.T) {
	b, clock := testBudget(10, 5*time.Second, 0)
	for range 100 {
		b.deposit()
	}
	if got := drain(b); got != 10 {
		t.Fatalf("allowed %d retries, want 10", got)
	}

	clock.sec.Add(5)
	for range 100 {
		b.deposit()
	}
	if got := drain(b); got != 10 {
		t.Errorf("after the window moved on, %d retries were allowed, want a fresh 10", got)
	}
}

// The limit holds when many goroutines race for the last units.
func TestBudget_ConcurrentWithdrawNeverExceedsTheLimit(t *testing.T) {
	b, _ := testBudget(50, 10*time.Second, 0)
	for range 1000 {
		b.deposit()
	}

	var granted atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			for b.withdraw() {
				granted.Add(1)
			}
		})
	}
	wg.Wait()

	// a unit may be refused while racing for it, never granted twice
	if got := granted.Load(); got > 500 || got <= 500-64 {
		t.Errorf("granted %d retries for 1000 requests at 50%%, want at most 500 and more than %d", got, 500-64)
	}
}

func TestBudget_ConcurrentDepositCountsEverything(t *testing.T) {
	b, _ := testBudget(100, 10*time.Second, 0)

	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 1000 {
				b.deposit()
			}
		})
	}
	wg.Wait()

	if requests, _ := b.totals(); requests != 32_000 {
		t.Errorf("counted %d requests, want 32000", requests)
	}
}

func TestBudget_NoAllocations(t *testing.T) {
	b := newRetryBudget(20, 10*time.Second, 1)

	allocs := testing.AllocsPerRun(1000, func() {
		b.deposit()
		_ = b.withdraw()
	})
	if allocs != 0 {
		t.Errorf("deposit+withdraw allocate %v times per call, want 0", allocs)
	}
}

func BenchmarkBudget_DepositWithdraw(b *testing.B) {
	rb := newRetryBudget(20, 10*time.Second, 1)
	for b.Loop() {
		rb.deposit()
		_ = rb.withdraw()
	}
}

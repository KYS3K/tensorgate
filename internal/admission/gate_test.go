package admission

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestNewGatePanicsOnBadArgs(t *testing.T) {
	tests := []struct {
		name       string
		maxStreams int
		maxWait    time.Duration
	}{
		{"zero streams", 0, time.Millisecond},
		{"negative streams", -1, time.Millisecond},
		{"zero wait", 1, 0},
		{"negative wait", 1, -time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("NewGate did not panic")
				}
			}()
			NewGate(tt.maxStreams, tt.maxWait)
		})
	}
}

// a free slot is taken at once: no waiting is reported
func TestGateFastPath(t *testing.T) {
	g := NewGate(3, time.Second)
	for i := range 3 {
		waited, err := g.Acquire(context.Background())
		if waited != 0 || err != nil {
			t.Fatalf("Acquire %d = (%v, %v), want (0, nil)", i, waited, err)
		}
	}
}

// Release must really free the slot, otherwise the gate fills up for good.
// With a single slot the second Acquire only succeeds if the first Release worked
func TestGateReleaseFreesTheSlot(t *testing.T) {
	g := NewGate(1, 20*time.Millisecond)
	for i := range 100 {
		if _, err := g.Acquire(context.Background()); err != nil {
			t.Fatalf("round %d: Acquire = %v, want nil (a slot was not freed)", i, err)
		}
		g.Release()
	}
}

func TestGateReleaseWithoutAcquirePanics(t *testing.T) {
	g := NewGate(2, time.Second)
	defer func() {
		if recover() == nil {
			t.Error("Release on an empty gate did not panic")
		}
	}()
	g.Release()
}

func TestGateTimeout(t *testing.T) {
	const maxWait = 30 * time.Millisecond
	g := NewGate(1, maxWait)
	if _, err := g.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	waited, err := g.Acquire(context.Background())
	elapsed := time.Since(start)

	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("err = %v, want ErrOverloaded", err)
	}
	if waited != maxWait {
		t.Errorf("waited = %v, want exactly maxWait (%v)", waited, maxWait)
	}
	if elapsed < maxWait {
		t.Errorf("returned after %v, before maxWait (%v)", elapsed, maxWait)
	}
	if elapsed > 10*maxWait {
		t.Errorf("returned after %v, far later than maxWait (%v)", elapsed, maxWait)
	}
}

// a slot freed during the wait goes to the waiter, and the wait is reported
func TestGateWaiterGetsFreedSlot(t *testing.T) {
	const holdFor = 30 * time.Millisecond
	g := NewGate(1, 5*time.Second)
	if _, err := g.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(holdFor, g.Release)

	start := time.Now()
	waited, err := g.Acquire(context.Background())
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Acquire = %v, want nil", err)
	}
	if waited < holdFor/2 || waited > elapsed {
		t.Errorf("waited = %v, want between %v and %v", waited, holdFor/2, elapsed)
	}
	if elapsed > time.Second {
		t.Errorf("Acquire took %v, it should have returned right after the Release", elapsed)
	}
}

// the client leaving is not an overload: the caller must be able to tell the two apart
func TestGateContextEnd(t *testing.T) {
	tests := []struct {
		name    string
		ctx     func() (context.Context, context.CancelFunc)
		wantErr error
	}{
		{
			name: "cancelled while waiting",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				time.AfterFunc(20*time.Millisecond, cancel)
				return ctx, cancel
			},
			wantErr: context.Canceled,
		},
		{
			name: "deadline while waiting",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 20*time.Millisecond)
			},
			wantErr: context.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewGate(1, 10*time.Second) // maxWait is huge: only the context can end the wait
			if _, err := g.Acquire(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := tt.ctx()
			defer cancel()

			start := time.Now()
			_, err := g.Acquire(ctx)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if errors.Is(err, ErrOverloaded) {
				t.Fatal("a client leaving was reported as overload")
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("returned after %v, the context was ignored", elapsed)
			}
		})
	}
}

// a waiter that gave up must not take a slot: after it left, all slots are still usable
func TestGateAbandonedWaiterKeepsNoSlot(t *testing.T) {
	g := NewGate(1, 10*time.Second)
	if _, err := g.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := g.Acquire(ctx)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond) // let it block in the slow path
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter err = %v, want Canceled", err)
	}

	g.Release()
	waited, err := g.Acquire(context.Background())
	if waited != 0 || err != nil {
		t.Fatalf("Acquire after the abandoned waiter = (%v, %v), want a free slot at once", waited, err)
	}
}

// waiters are served in the order they arrived
func TestGateFIFO(t *testing.T) {
	const waiters = 5
	g := NewGate(1, 10*time.Second)
	if _, err := g.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	order := make(chan int, waiters)
	var wg sync.WaitGroup
	for i := range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := g.Acquire(context.Background()); err != nil {
				t.Errorf("waiter %d: %v", i, err)
				return
			}
			order <- i
			g.Release()
		}()
		// the next one starts only after this one is parked in the queue
		time.Sleep(20 * time.Millisecond)
	}

	g.Release()
	wg.Wait()
	close(order)

	want := 0
	for got := range order {
		if got != want {
			t.Fatalf("waiter %d was served at position %d: not FIFO", got, want)
		}
		want++
	}
}

// the whole point of the gate: never more than maxStreams holders at once
func TestGateNeverExceedsCapacity(t *testing.T) {
	const (
		capacity   = 4
		goroutines = 64
		rounds     = 20
	)
	g := NewGate(capacity, 200*time.Millisecond)

	var inside, peak, served, shed atomic.Int64
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range rounds {
				if _, err := g.Acquire(context.Background()); err != nil {
					if !errors.Is(err, ErrOverloaded) {
						t.Errorf("unexpected error: %v", err)
					}
					shed.Add(1)
					continue
				}
				n := inside.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				time.Sleep(100 * time.Microsecond)
				inside.Add(-1)
				g.Release()
				served.Add(1)
			}
		}()
	}
	wg.Wait()

	if p := peak.Load(); p > capacity {
		t.Fatalf("%d holders at once, the limit is %d", p, capacity)
	}
	if served.Load() == 0 {
		t.Fatal("nobody was served")
	}
	// everything that was taken was returned: the gate is empty again
	for range capacity {
		if waited, err := g.Acquire(context.Background()); waited != 0 || err != nil {
			t.Fatalf("slot leaked: Acquire = (%v, %v)", waited, err)
		}
	}
	t.Logf("served %d, shed %d, peak %d", served.Load(), shed.Load(), peak.Load())
}

func TestGateNoGoroutineLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	g := NewGate(1, 5*time.Millisecond)
	if _, err := g.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := context.Background()
			if i%2 == 0 {
				c = ctx
			}
			g.Acquire(c) // half time out, half are cancelled
		}()
	}
	cancel()
	wg.Wait()
}

// the common case: a free slot must cost no allocations
func BenchmarkGateFastPath(b *testing.B) {
	g := NewGate(1, time.Second)
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		g.Acquire(ctx)
		g.Release()
	}
}

// the shed path under overload: how much one rejected request costs (timer included)
func BenchmarkGateShed(b *testing.B) {
	g := NewGate(1, time.Microsecond)
	g.Acquire(context.Background())
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		g.Acquire(ctx)
	}
}

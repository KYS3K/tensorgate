package lb

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"
)

func testURL(t *testing.T) *url.URL {
	t.Helper()
	u, err := url.Parse("http://127.0.0.1:8081")
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestNewBackend_ZeroConfigDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("newBackend with a zero Config panicked: %v", r)
		}
	}()
	newBackend(testURL(t), Config{})
}

func TestNewBackend_StartsAlive(t *testing.T) {
	b := newBackend(testURL(t), Config{})
	if !b.Alive() {
		t.Error("new backend must start alive")
	}
	if got := b.InFlight(); got != 0 {
		t.Errorf("InFlight() = %d, want 0", got)
	}
}

func TestNewBackend_TransportIsConfigured(t *testing.T) {
	cfg := Config{}.withDefaults()
	b := newBackend(testURL(t), Config{})
	tr := b.transport

	if tr.MaxIdleConnsPerHost != cfg.MaxIdleConnsPerHost {
		t.Errorf("MaxIdleConnsPerHost = %d, want %d", tr.MaxIdleConnsPerHost, cfg.MaxIdleConnsPerHost)
	}
	if tr.IdleConnTimeout != cfg.IdleConnTimeout {
		t.Errorf("IdleConnTimeout = %v, want %v", tr.IdleConnTimeout, cfg.IdleConnTimeout)
	}
	if tr.ResponseHeaderTimeout != cfg.ResponseHeaderTimeout {
		t.Errorf("ResponseHeaderTimeout = %v, want %v", tr.ResponseHeaderTimeout, cfg.ResponseHeaderTimeout)
	}
	if tr.DialContext == nil {
		t.Error("DialContext is nil: connecting to a dead host would hang until the OS gives up")
	}
	if tr.Proxy != nil {
		t.Error("Proxy must be nil: upstreams are reached directly")
	}
}

func TestRelease_BelowZeroPanicsAndKeepsCounterSane(t *testing.T) {
	b := newBackend(testURL(t), Config{})

	func() {
		defer func() {
			if recover() == nil {
				t.Error("Release without acquire must panic")
			}
		}()
		b.Release()
	}()

	// net/http recovers handler panics, so the process lives on: the counter
	// must not stay negative or this backend looks like the freest one forever.
	if got := b.InFlight(); got != 0 {
		t.Errorf("InFlight() after a bad Release = %d, want 0", got)
	}
}

func TestAcquireRelease_Concurrent(t *testing.T) {
	b := newBackend(testURL(t), Config{})

	const workers, iters = 64, 2000
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iters {
				b.acquire()
				_ = b.InFlight() // concurrent reads must be race free
				b.Release()
			}
		}()
	}
	wg.Wait()

	if got := b.InFlight(); got != 0 {
		t.Errorf("InFlight() = %d after balanced acquire/release, want 0", got)
	}
}

func TestRelease_ReleasesOnPanicWithDefer(t *testing.T) {
	b := newBackend(testURL(t), Config{})

	func() {
		defer func() { _ = recover() }()
		b.acquire()
		defer b.Release()
		panic("handler blew up")
	}()

	if got := b.InFlight(); got != 0 {
		t.Errorf("InFlight() = %d after a panicking handler, want 0", got)
	}
}

// A zero Config must still give a dial timeout. 10.255.255.1 is a black hole
// on most networks: the SYN is never answered, so only a dial timeout (and not
// the OS, which gives up after minutes) ends the attempt.
func TestNewBackend_DialTimeoutFromDefaults(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the default dial timeout")
	}
	b := newBackend(testURL(t), Config{})

	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout*2)
	defer cancel()

	start := time.Now()
	conn, err := b.transport.DialContext(ctx, "tcp", "10.255.255.1:81")
	elapsed := time.Since(start)
	if err == nil {
		conn.Close()
		t.Skip("black hole address answered, cannot test the dial timeout here")
	}
	if elapsed < defaultDialTimeout/2 {
		t.Skipf("dial failed immediately (%v): %v; no black hole on this network", elapsed, err)
	}
	if ctx.Err() != nil {
		t.Errorf("dial was only stopped by the test deadline after %v: Dialer.Timeout is not set from the defaults", elapsed)
	}
}

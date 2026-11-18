package admission

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
	"golang.org/x/time/rate"
)

var base = time.Unix(1_700_000_000, 0)

// testConfig: 1 token/s, burst 2. The sweeper ticks once an hour, so it never fires on its own
// and the tests call sweep() by hand with a fake clock.
func testConfig() Config {
	return Config{
		RatePerKey:  1,
		BurstPerKey: 2,
		KeyTTL:      time.Minute,
		SweepEvery:  time.Hour,
		MaxKeys:     100,
	}
}

func newTestLimiter(t *testing.T, cfg Config) *KeyLimiter {
	t.Helper()
	k := NewKeyLimiter(cfg)
	t.Cleanup(k.Close)
	return k
}

func (k *KeyLimiter) size() int {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return len(k.entries)
}

// one call to Allow: at is the offset from base
type step struct {
	at        time.Duration
	key       string
	wantOK    bool
	wantRetry time.Duration
}

func TestKeyLimiterAllow(t *testing.T) {
	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "burst is spent, then reject with the wait for one token",
			steps: []step{
				{0, "a", true, 0},
				{0, "a", true, 0},
				{0, "a", false, time.Second},
			},
		},
		{
			name: "partial refill gives a shorter wait",
			steps: []step{
				{0, "a", true, 0},
				{0, "a", true, 0},
				{500 * time.Millisecond, "a", false, 500 * time.Millisecond},
			},
		},
		{
			name: "allowed again after waiting the advertised time",
			steps: []step{
				{0, "a", true, 0},
				{0, "a", true, 0},
				{0, "a", false, time.Second},
				{time.Second, "a", true, 0},
			},
		},
		{
			// without CancelAt every rejection would push the bucket deeper into debt
			// and the client would wait 1s, 2s, 3s... instead of 1s
			name: "rejections do not accumulate debt",
			steps: []step{
				{0, "a", true, 0},
				{0, "a", true, 0},
				{0, "a", false, time.Second},
				{0, "a", false, time.Second},
				{0, "a", false, time.Second},
				{0, "a", false, time.Second},
				{time.Second, "a", true, 0},
				{time.Second, "a", false, time.Second},
			},
		},
		{
			name: "keys are independent",
			steps: []step{
				{0, "a", true, 0},
				{0, "a", true, 0},
				{0, "a", false, time.Second},
				{0, "b", true, 0},
				{0, "b", true, 0},
				{0, "b", false, time.Second},
			},
		},
		{
			name: "idle time never fills the bucket above the burst",
			steps: []step{
				{0, "a", true, 0},
				{0, "a", true, 0},
				{time.Hour, "a", true, 0},
				{time.Hour, "a", true, 0},
				{time.Hour, "a", false, time.Second},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := newTestLimiter(t, testConfig())
			for i, s := range tt.steps {
				ok, retry := k.Allow(s.key, base.Add(s.at))
				if ok != s.wantOK || retry != s.wantRetry {
					t.Fatalf("step %d (key %q at +%v): Allow = (%v, %v), want (%v, %v)",
						i, s.key, s.at, ok, retry, s.wantOK, s.wantRetry)
				}
			}
		})
	}
}

func TestKeyLimiterMaxKeys(t *testing.T) {
	cfg := testConfig()
	cfg.MaxKeys = 2
	cfg.SweepEvery = 7 * time.Second // also the Retry-After for a rejected new key
	k := newTestLimiter(t, cfg)

	for _, key := range []string{"a", "b"} {
		if ok, _ := k.Allow(key, base); !ok {
			t.Fatalf("key %q below the cap was rejected", key)
		}
	}

	ok, retry := k.Allow("c", base)
	if ok || retry != cfg.SweepEvery {
		t.Fatalf("new key over the cap: Allow = (%v, %v), want (false, %v)", ok, retry, cfg.SweepEvery)
	}
	if got := k.size(); got != 2 {
		t.Fatalf("size = %d after a rejected key, want 2 (the cap)", got)
	}

	// existing keys keep working when the map is full
	if ok, _ := k.Allow("a", base); !ok {
		t.Fatal("existing key was rejected because the map is full")
	}

	// after the sweeper frees the room, the new key gets in
	k.sweep(base.Add(cfg.KeyTTL + time.Nanosecond))
	if ok, _ := k.Allow("c", base.Add(cfg.KeyTTL+time.Second)); !ok {
		t.Fatal("new key was rejected after the sweep freed the room")
	}
}

func TestKeyLimiterSweep(t *testing.T) {
	ttl := testConfig().KeyTTL

	tests := []struct {
		name string
		// calls made before the sweep
		calls     []step
		sweepAt   time.Duration
		wantLeft  int
		wantAlive []string
	}{
		{
			name:     "idle for exactly the TTL stays",
			calls:    []step{{0, "a", true, 0}},
			sweepAt:  ttl,
			wantLeft: 1, wantAlive: []string{"a"},
		},
		{
			name:     "idle for longer than the TTL goes",
			calls:    []step{{0, "a", true, 0}},
			sweepAt:  ttl + time.Nanosecond,
			wantLeft: 0,
		},
		{
			// a later request moves lastSeen forward, otherwise an active client
			// would be swept and get a fresh full bucket
			name: "a recent request keeps the key",
			calls: []step{
				{0, "a", true, 0},
				{40 * time.Second, "a", true, 0},
			},
			sweepAt:  ttl + time.Nanosecond,
			wantLeft: 1, wantAlive: []string{"a"},
		},
		{
			// a rejected request is still activity: a flooding key must not be swept mid-attack
			name: "a rejected request keeps the key too",
			calls: []step{
				{0, "a", true, 0},
				{0, "a", true, 0},
				{40 * time.Second, "a", true, 0},
				{40 * time.Second, "a", true, 0},
				{40 * time.Second, "a", false, time.Second},
			},
			sweepAt:  ttl + time.Nanosecond,
			wantLeft: 1, wantAlive: []string{"a"},
		},
		{
			name: "only the stale keys are removed",
			calls: []step{
				{0, "old", true, 0},
				{50 * time.Second, "fresh", true, 0},
			},
			sweepAt:  ttl + time.Nanosecond,
			wantLeft: 1, wantAlive: []string{"fresh"},
		},
		{
			name:     "empty map",
			sweepAt:  ttl,
			wantLeft: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := newTestLimiter(t, testConfig())
			for _, s := range tt.calls {
				k.Allow(s.key, base.Add(s.at))
			}

			k.sweep(base.Add(tt.sweepAt))

			if got := k.size(); got != tt.wantLeft {
				t.Fatalf("size after sweep = %d, want %d", got, tt.wantLeft)
			}
			k.mu.RLock()
			defer k.mu.RUnlock()
			for _, key := range tt.wantAlive {
				if k.entries[key] == nil {
					t.Errorf("key %q was removed, want it kept", key)
				}
			}
		})
	}
}

// a swept key comes back with a full bucket: that is why KeyTTL must exceed Burst/Rate
func TestKeyLimiterSweptKeyStartsFresh(t *testing.T) {
	k := newTestLimiter(t, testConfig())
	k.Allow("a", base)
	k.Allow("a", base)
	if ok, _ := k.Allow("a", base); ok {
		t.Fatal("bucket should be empty")
	}

	later := base.Add(time.Hour)
	k.sweep(later)

	for i := range 2 {
		if ok, _ := k.Allow("a", later); !ok {
			t.Fatalf("call %d after the sweep was rejected, want a fresh bucket", i)
		}
	}
}

// the clock is frozen, so exactly Burst calls per key may pass no matter how the goroutines interleave.
// Every round uses fresh keys and releases all goroutines at once: that is when several of them find
// the key missing together and race to create it, which the double check under Lock must survive
func TestKeyLimiterConcurrent(t *testing.T) {
	const (
		rounds     = 200
		keys       = 8
		goroutines = 32
		callsEach  = 40
	)
	cfg := testConfig()
	cfg.BurstPerKey = 5
	cfg.MaxKeys = rounds * keys // every round adds fresh keys, the cap must not get in the way
	k := newTestLimiter(t, cfg)

	for round := range rounds {
		names := make([]string, keys)
		for i := range names {
			names[i] = "round-" + strconv.Itoa(round) + "-key-" + strconv.Itoa(i)
		}
		var allowed [keys]atomic.Int64

		start := make(chan struct{})
		var wg sync.WaitGroup
		for g := range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := range callsEach {
					idx := (g + i) % keys
					if ok, _ := k.Allow(names[idx], base); ok {
						allowed[idx].Add(1)
					}
				}
			}()
		}
		close(start)
		wg.Wait()

		for i := range allowed {
			if got := allowed[i].Load(); got != int64(cfg.BurstPerKey) {
				t.Fatalf("round %d, %s: %d calls passed, want exactly %d", round, names[i], got, cfg.BurstPerKey)
			}
		}
	}
}

// the real sweeper goroutine, driven by a fake clock that is already far in the future
func TestKeyLimiterJanitorSweeps(t *testing.T) {
	cfg := testConfig()
	cfg.SweepEvery = 5 * time.Millisecond
	cfg.Now = func() time.Time { return base.Add(2 * cfg.KeyTTL) }
	k := newTestLimiter(t, cfg)

	k.Allow("a", base)
	if k.size() != 1 {
		t.Fatal("key was not stored")
	}

	deadline := time.Now().Add(2 * time.Second)
	for k.size() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("janitor did not remove the stale key in 2s")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestKeyLimiterClose(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Close must return only after the janitor has exited: done is already closed on return.
	// Repeated, because a Close that does not wait loses this race only some of the time
	for range 500 {
		k := NewKeyLimiter(testConfig())
		k.Close()
		select {
		case <-k.done:
		default:
			t.Fatal("Close returned before the janitor goroutine exited")
		}
		k.Close() // a second Close must be a no-op, not a panic on a closed channel
	}
}

func TestNewKeyLimiterPanicsOnBadConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"zero SweepEvery", func(c *Config) { c.SweepEvery = 0 }},
		{"negative SweepEvery", func(c *Config) { c.SweepEvery = -time.Second }},
		{"zero KeyTTL", func(c *Config) { c.KeyTTL = 0 }},
		{"zero BurstPerKey", func(c *Config) { c.BurstPerKey = 0 }},
		{"zero MaxKeys", func(c *Config) { c.MaxKeys = 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.mutate(&cfg)
			defer func() {
				if recover() == nil {
					t.Error("NewKeyLimiter did not panic")
				}
			}()
			k := NewKeyLimiter(cfg)
			k.Close() // reached only if there was no panic; do not leak the janitor
		})
	}
}

// the hot path for a known key: a read lock, an atomic store and a reservation; it must not allocate
func BenchmarkKeyLimiterAllow(b *testing.B) {
	cfg := testConfig()
	cfg.RatePerKey = rate.Inf
	k := NewKeyLimiter(cfg)
	defer k.Close()
	k.Allow("hot", base)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k.Allow("hot", base)
		}
	})
}

package admission

import (
	"maps"
	"strings"
	"sync"
	"sync/atomic"

	"time"

	"golang.org/x/time/rate"
)

type keyEntry struct {
	lim      *rate.Limiter
	lastSeen atomic.Int64 //unix nano
}

type KeyLimiter struct {
	mu      sync.RWMutex
	entries map[string]*keyEntry
	cfg     Config
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

func NewKeyLimiter(cfg Config) *KeyLimiter {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.SweepEvery <= 0 {
		panic("admission: SweepEvery must be > 0")
	}
	if cfg.KeyTTL <= 0 {
		panic("admission: KeyTTL must be > 0")
	}
	if cfg.BurstPerKey <= 0 {
		panic("admission: BurstPerKey must be > 0")
	}
	if cfg.MaxKeys <= 0 {
		panic("admission: MaxKeys must be > 0")
	}
	k := &KeyLimiter{
		entries: make(map[string]*keyEntry),
		cfg:     cfg,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go k.janitor()
	return k
}
func (k *KeyLimiter) janitor() {
	defer close(k.done)
	ticker := time.NewTicker(k.cfg.SweepEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			k.sweep(k.cfg.Now())
		case <-k.stop:
			return
		}
	}
}
func (k *KeyLimiter) sweep(now time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()
	maps.DeleteFunc(k.entries, func(_ string, v *keyEntry) bool {
		last := v.lastSeen.Load()
		return now.Sub(time.Unix(0, last)) > k.cfg.KeyTTL
	})
}

func (k *KeyLimiter) Allow(key string, now time.Time) (ok bool, retryAfter time.Duration) {
	k.mu.RLock()
	e := k.entries[key]
	k.mu.RUnlock()
	if e == nil {
		k.mu.Lock()
		// checking, maybe since last check it already been created
		e = k.entries[key]
		if e == nil {
			if len(k.entries) >= k.cfg.MaxKeys {
				k.mu.Unlock()
				return false, k.cfg.SweepEvery
			}
			e = &keyEntry{
				lim: rate.NewLimiter(k.cfg.RatePerKey, k.cfg.BurstPerKey),
			}
			e.lastSeen.Store(now.UnixNano())
			k.entries[strings.Clone(key)] = e
		}
		k.mu.Unlock()
	}
	e.lastSeen.Store(now.UnixNano())
	r := e.lim.ReserveN(now, 1)
	d := r.DelayFrom(now)
	if d > 0 {
		r.CancelAt(now)
		return false, d
	}
	return true, 0
}

func (k *KeyLimiter) Close() {
	k.once.Do(func() {
		close(k.stop)
	})
	<-k.done
}

func (k *KeyLimiter) Len() int {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return len(k.entries)
}

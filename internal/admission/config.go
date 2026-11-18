package admission

import (
	"golang.org/x/time/rate"
	"time"
)

type Config struct {
	RatePerKey   rate.Limit
	BurstPerKey  int
	KeyTTL       time.Duration //inactive key live in memory
	SweepEvery   time.Duration
	MaxKeys      int // map size threshold
	MaxStreams   int // chan struct treshhold
	MaxQueueWait time.Duration
	// random time added to Retry-After so rejected clients do not all come back in the same second.
	// Separate for the two rejections: a rate-limited client knows when its token returns and should come back
	// close to that, while an overloaded server wants the retries spread wide to cut the retry storm
	RateLimitJitter time.Duration // 429
	OverloadJitter  time.Duration // 503
	Now             func() time.Time
}

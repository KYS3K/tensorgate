package lb

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	Upstreams []*url.URL

	MaxIdleConnsPerHost   int
	IdleConnTimeout       time.Duration
	ResponseHeaderTimeout time.Duration
	DialTimeout           time.Duration

	CheckInterval    time.Duration
	CheckTimeout     time.Duration
	FailThreshold    int
	RiseThreshold    int
	PassiveThreshold int

	HealthPath string

	MaxBodyBytes int64

	// MaxAttempts is the number of tries per request, 1 turns retries off.
	MaxAttempts int
	// Retries are limited to RetryBudgetPercent of the requests of the last
	// RetryBudgetWindow, plus RetryMinPerSec per second so that a quiet gateway can still retry.
	RetryBudgetPercent int
	RetryBudgetWindow  time.Duration
	RetryMinPerSec     int

	// Balancing is "lor" (the default): the least loaded upstream, or "prefix": requests that start with
	// the same prompt go to the same upstream, which has the KV cache of that start, unless that upstream
	// carries more than (1+PrefixSlack) times the average load, then the next one on the ring takes them.
	Balancing string
	// The routing key is the hash of the first PrefixBlocks full blocks of PrefixBlockBytes of the prompt.
	PrefixBlockBytes int
	PrefixBlocks     int
	PrefixSlack      float64 // must be > 0, a smaller one spreads the load better and reuses the cache less
	// RingVnodes is the number of points an upstream has on the hash ring. The spread of load between
	// upstreams is about 1/sqrt(RingVnodes). Changing it moves every key.
	RingVnodes int
}

const (
	BalancingLOR    = "lor"
	BalancingPrefix = "prefix"
)

const (
	defaultMaxIdleConnsPerHost   = 128
	defaultIdleConnTimeout       = 30 * time.Second
	defaultResponseHeaderTimeout = 10 * time.Second
	defaultDialTimeout           = 2 * time.Second

	defaultCheckInterval    = 2 * time.Second
	defaultCheckTimeout     = 1 * time.Second // must stay below defaultCheckInterval
	defaultFailThreshold    = 3
	defaultRiseThreshold    = 2
	defaultPassiveThreshold = 3

	defaultMaxBodyBytes = 1 << 20

	defaultMaxAttempts        = 2
	defaultRetryBudgetPercent = 20
	defaultRetryBudgetWindow  = 10 * time.Second
	defaultRetryMinPerSec     = 1

	defaultPrefixBlockBytes = 256
	defaultPrefixBlocks     = 2
	defaultPrefixSlack      = 0.25
	defaultRingVnodes       = 150
)

// withDefaults returns a copy of c where every field that is zero or negative
// is replaced by its default. Fields that are already set are left alone, and
// calling it twice gives the same result.
func (c Config) withDefaults() Config {
	if c.MaxIdleConnsPerHost <= 0 {
		c.MaxIdleConnsPerHost = defaultMaxIdleConnsPerHost
	}
	if c.IdleConnTimeout <= 0 {
		c.IdleConnTimeout = defaultIdleConnTimeout
	}
	if c.ResponseHeaderTimeout <= 0 {
		c.ResponseHeaderTimeout = defaultResponseHeaderTimeout
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = defaultDialTimeout
	}
	if c.CheckInterval <= 0 {
		c.CheckInterval = defaultCheckInterval
	}
	if c.CheckTimeout <= 0 {
		c.CheckTimeout = defaultCheckTimeout
	}
	if c.FailThreshold <= 0 {
		c.FailThreshold = defaultFailThreshold
	}
	if c.RiseThreshold <= 0 {
		c.RiseThreshold = defaultRiseThreshold
	}
	if c.PassiveThreshold <= 0 {
		c.PassiveThreshold = defaultPassiveThreshold
	}
	if len(c.HealthPath) == 0 {
		c.HealthPath = "/healthz"
	}
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = defaultMaxBodyBytes
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
	if c.RetryBudgetPercent <= 0 {
		c.RetryBudgetPercent = defaultRetryBudgetPercent
	}
	if c.RetryBudgetWindow <= 0 {
		c.RetryBudgetWindow = defaultRetryBudgetWindow
	}
	if c.RetryMinPerSec <= 0 {
		c.RetryMinPerSec = defaultRetryMinPerSec
	}
	if c.Balancing == "" {
		c.Balancing = BalancingLOR
	}
	if c.PrefixBlockBytes <= 0 {
		c.PrefixBlockBytes = defaultPrefixBlockBytes
	}
	if c.PrefixBlocks <= 0 {
		c.PrefixBlocks = defaultPrefixBlocks
	}
	if c.PrefixSlack <= 0 {
		c.PrefixSlack = defaultPrefixSlack
	}
	if c.RingVnodes <= 0 {
		c.RingVnodes = defaultRingVnodes
	}
	return c
}

func (c Config) validate() error {
	if c.CheckTimeout >= c.CheckInterval {
		return errors.New("CheckTimeout must be < CheckInterval")
	}
	if c.RetryBudgetWindow%time.Second != 0 {
		return errors.New("RetryBudgetWindow must be whole seconds")
	}

	if c.Balancing != "" && c.Balancing != BalancingLOR && c.Balancing != BalancingPrefix {
		return fmt.Errorf("unknown balancing %q, want %q or %q", c.Balancing, BalancingLOR, BalancingPrefix)
	}
	if math.IsNaN(c.PrefixSlack) || math.IsInf(c.PrefixSlack, 0) {
		return errors.New("PrefixSlack must be a finite number")
	}

	return nil
}

func ParseUpstreams(s string) ([]*url.URL, error) {
	//empty = no upstreams, using builtin mock
	if len(s) == 0 {
		return nil, nil
	}

	urls := strings.Split(s, ",")
	seen := make(map[string]struct{})
	results := make([]*url.URL, 0, len(urls))

	for _, uS := range urls {
		u, err := parseUpstream(uS)
		if err != nil {
			return nil, fmt.Errorf("lb: upstream %q: %w", uS, err)
		}
		key := strings.ToLower(u.Scheme + "://" + u.Host)
		_, exists := seen[key]
		if exists {
			return nil, fmt.Errorf("lb: upstream %q is duplicate", uS)
		}
		seen[key] = struct{}{}
		results = append(results, u)
	}

	return results, nil
}

func parseUpstream(s string) (*url.URL, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return nil, errors.New("empty upstream")
	}
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.New("wrong schema")
	}
	if len(u.Hostname()) == 0 {
		return nil, errors.New("wrong host")
	}
	if u.Path != "/" && u.Path != "" {
		return nil, errors.New("no path allowed")
	}
	if len(u.Fragment) > 0 {
		return nil, errors.New("no fragment allowed")
	}
	if len(u.RawQuery) > 0 {
		return nil, errors.New("no query allowed")
	}
	if u.User != nil {
		return nil, errors.New("no user allowed")
	}

	return u, nil
}

package lb

import (
	"errors"
	"fmt"
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
}

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
	return c
}

func (c Config) validate() error {
	if c.CheckTimeout >= c.CheckInterval {
		return errors.New("CheckTimeout must be < CheckInterval")
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

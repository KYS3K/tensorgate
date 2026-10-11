package gateway

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

type MockConfig struct {
	TTFT   time.Duration // Time To First Token
	TPOT   time.Duration // Time Per Output token
	Tokens int
}

func DefaultMockConfig() MockConfig {
	return MockConfig{
		TTFT:   200 * time.Millisecond,
		TPOT:   30 * time.Millisecond,
		Tokens: 10,
	}
}

const (
	minTTFT   = 0
	maxTTFT   = 30 * time.Second
	minTPOT   = time.Millisecond
	maxTPOT   = 5 * time.Second
	minTokens = 1
	maxTokens = 10_000
)

type HeaderError struct {
	Header string
	Value  string
	Reason string
}

func (e *HeaderError) Error() string {
	return fmt.Sprintf("invalid %s %q: %s", e.Header, e.Value, e.Reason)
}

func (h *CompletionsHandler) resolveConfig(r *http.Request) (MockConfig, error) {
	cfg := h.DefaultConfig
	var err error

	if cfg.TTFT, err = durationHeader(r, "X-Mock-TTFT", cfg.TTFT, minTTFT, maxTTFT); err != nil {
		return MockConfig{}, err
	}
	if cfg.TPOT, err = durationHeader(r, "X-Mock-TPOT", cfg.TPOT, minTPOT, maxTPOT); err != nil {
		return MockConfig{}, err
	}
	if cfg.Tokens, err = intHeader(r, "X-Mock-Tokens", cfg.Tokens, minTokens, maxTokens); err != nil {
		return MockConfig{}, err
	}
	return cfg, nil
}

func durationHeader(r *http.Request, name string, def, lo, hi time.Duration) (time.Duration, error) {
	v := r.Header.Get(name)
	if v == "" {
		return def, nil
	}

	s := v
	if _, err := strconv.Atoi(s); err == nil {
		s += "ms"
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, &HeaderError{name, v, `want a duration like "200ms", "1.5s" or milliseconds like "200"`}
	}
	if d < lo || d > hi {
		return 0, &HeaderError{name, v, fmt.Sprintf("must be between %v and %v", lo, hi)}
	}
	return d, nil
}

func intHeader(r *http.Request, name string, def, lo, hi int) (int, error) {
	v := r.Header.Get(name)
	if v == "" {
		return def, nil
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, &HeaderError{name, v, "want an integer"}
	}
	if n < lo || n > hi {
		return 0, &HeaderError{name, v, fmt.Sprintf("must be between %d and %d", lo, hi)}
	}
	return n, nil
}

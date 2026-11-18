package admission

import (
	"math"
	"strconv"
	"testing"
	"time"
)

// with jitter off the result is deterministic: it pins down rounding, the minimum and the cap
func TestRetryAfterNoJitter(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
		want string
	}{
		{"zero gives the minimum", 0, "1"},
		{"negative gives the minimum", -5 * time.Second, "1"},
		{"1ns rounds up", time.Nanosecond, "1"},
		{"420ms rounds up", 420 * time.Millisecond, "1"},
		{"exactly 1s stays", time.Second, "1"},
		{"1s+1ns rounds up", time.Second + time.Nanosecond, "2"},
		{"2.1s rounds up", 2100 * time.Millisecond, "3"},
		{"exactly 5s stays", 5 * time.Second, "5"},
		{"exactly the cap", maxRetryAfter, "60"},
		{"above the cap is cut", maxRetryAfter + time.Second, "60"},
		// rate.InfDuration is what a limiter returns when a token can never be issued
		{"infinite is cut", time.Duration(math.MaxInt64), "60"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RetryAfter(tt.d, 0); got != tt.want {
				t.Errorf("RetryAfter(%v, 0) = %q, want %q", tt.d, got, tt.want)
			}
		})
	}
}

func TestRetryAfterNegativeJitterIsIgnored(t *testing.T) {
	if got := RetryAfter(2100*time.Millisecond, -time.Second); got != "3" {
		t.Errorf("RetryAfter(2.1s, -1s) = %q, want %q", got, "3")
	}
}

// with jitter on the result is random, so check the range and that it actually varies
func TestRetryAfterJitterRange(t *testing.T) {
	tests := []struct {
		name   string
		d      time.Duration
		jitter time.Duration
		min    int
		max    int
		// whether the integer result must take at least two different values
		spreads bool
	}{
		// 2.1s + [0, 1.5s) = [2.1s, 3.6s) -> 3 or 4
		{"spreads across a second boundary", 2100 * time.Millisecond, 1500 * time.Millisecond, 3, 4, true},
		// 0 + [0, 3s) -> the minimum is 1, the top is 3
		{"zero delay still gets the minimum", 0, 3 * time.Second, 1, 3, true},
		// jitter is added after the cap, so the result may exceed it by up to jitter;
		// 60s + (0, 1s) always rounds up to 61, so there is no spread to check
		{"jitter over the cap", time.Duration(math.MaxInt64), time.Second, 60, 61, false},
	}

	const runs = 2000
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := make(map[int]bool)
			for range runs {
				got, err := strconv.Atoi(RetryAfter(tt.d, tt.jitter))
				if err != nil {
					t.Fatalf("RetryAfter returned a non-integer: %v", err)
				}
				if got < tt.min || got > tt.max {
					t.Fatalf("RetryAfter(%v, %v) = %d, want in [%d, %d]", tt.d, tt.jitter, got, tt.min, tt.max)
				}
				seen[got] = true
			}
			// without this, a jitter that is silently always 0 would pass the range check
			if tt.spreads && len(seen) < 2 {
				t.Errorf("after %d runs got only %v, jitter does not spread", runs, seen)
			}
		})
	}
}

// the 429/503 path is hot under overload: building the header must not allocate
func BenchmarkRetryAfter(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		RetryAfter(2100*time.Millisecond, 500*time.Millisecond)
	}
}

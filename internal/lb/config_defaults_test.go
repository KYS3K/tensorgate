package lb

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestWithDefaults_FillsZeroValues(t *testing.T) {
	got := Config{}.withDefaults()

	want := Config{
		MaxIdleConnsPerHost:   defaultMaxIdleConnsPerHost,
		IdleConnTimeout:       defaultIdleConnTimeout,
		ResponseHeaderTimeout: defaultResponseHeaderTimeout,
		DialTimeout:           defaultDialTimeout,
		CheckInterval:         defaultCheckInterval,
		CheckTimeout:          defaultCheckTimeout,
		FailThreshold:         defaultFailThreshold,
		RiseThreshold:         defaultRiseThreshold,
		PassiveThreshold:      defaultPassiveThreshold,
		HealthPath:            "/healthz",
		MaxBodyBytes:          defaultMaxBodyBytes,
		MaxAttempts:           defaultMaxAttempts,
		RetryBudgetPercent:    defaultRetryBudgetPercent,
		RetryBudgetWindow:     defaultRetryBudgetWindow,
		RetryMinPerSec:        defaultRetryMinPerSec,
		Balancing:             BalancingLOR,
		PrefixBlockBytes:      defaultPrefixBlockBytes,
		PrefixBlocks:          defaultPrefixBlocks,
		PrefixSlack:           defaultPrefixSlack,
		RingVnodes:            defaultRingVnodes,
	}
	if got.Upstreams != nil {
		t.Errorf("Upstreams = %v, want nil", got.Upstreams)
	}
	got.Upstreams = nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("withDefaults() = %+v, want %+v", got, want)
	}
}

func TestWithDefaults_KeepsExplicitValues(t *testing.T) {
	in := Config{
		MaxIdleConnsPerHost:   7,
		IdleConnTimeout:       time.Second,
		ResponseHeaderTimeout: 2 * time.Second,
		DialTimeout:           3 * time.Second,
		CheckInterval:         4 * time.Second,
		CheckTimeout:          5 * time.Second,
		FailThreshold:         6,
		RiseThreshold:         8,
		PassiveThreshold:      9,
		HealthPath:            "/ping",
		MaxBodyBytes:          10,
		MaxAttempts:           3,
		RetryBudgetPercent:    11,
		RetryBudgetWindow:     12 * time.Second,
		RetryMinPerSec:        13,
		Balancing:             BalancingPrefix,
		PrefixBlockBytes:      14,
		PrefixBlocks:          15,
		PrefixSlack:           0.5,
		RingVnodes:            16,
	}
	if got := in.withDefaults(); !reflect.DeepEqual(got, in) {
		t.Errorf("withDefaults() overwrote explicit values: got %+v, want %+v", got, in)
	}
}

func TestWithDefaults_ReplacesNegative(t *testing.T) {
	got := Config{MaxIdleConnsPerHost: -1, DialTimeout: -time.Second, FailThreshold: -5}.withDefaults()

	if got.MaxIdleConnsPerHost != defaultMaxIdleConnsPerHost ||
		got.DialTimeout != defaultDialTimeout ||
		got.FailThreshold != defaultFailThreshold {
		t.Errorf("negative values were not replaced: %+v", got)
	}
}

func TestWithDefaults_Idempotent(t *testing.T) {
	once := Config{FailThreshold: 10}.withDefaults()
	twice := once.withDefaults()
	if !reflect.DeepEqual(once, twice) {
		t.Errorf("not idempotent: once %+v, twice %+v", once, twice)
	}
}

func TestWithDefaults_DoesNotMutateReceiver(t *testing.T) {
	in := Config{}
	_ = in.withDefaults()
	if !reflect.DeepEqual(in, Config{}) {
		t.Errorf("receiver was modified: %+v", in)
	}
}

func TestDefaults_CheckTimeoutBelowInterval(t *testing.T) {
	if defaultCheckTimeout >= defaultCheckInterval {
		t.Fatalf("defaultCheckTimeout %v must be below defaultCheckInterval %v",
			defaultCheckTimeout, defaultCheckInterval)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"defaults are valid", Config{}.withDefaults(), false},
		{"timeout below interval", Config{CheckInterval: 2 * time.Second, CheckTimeout: time.Second}, false},
		{"timeout equals interval", Config{CheckInterval: time.Second, CheckTimeout: time.Second}, true},
		{"timeout above interval", Config{CheckInterval: time.Second, CheckTimeout: 2 * time.Second}, true},
		{"prefix balancing", Config{Balancing: BalancingPrefix}.withDefaults(), false},
		{"unknown balancing", Config{Balancing: "round-robin"}.withDefaults(), true},
		{"slack is NaN", Config{PrefixSlack: math.NaN()}.withDefaults(), true},
		{"slack is infinite", Config{PrefixSlack: math.Inf(1)}.withDefaults(), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

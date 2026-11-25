package main

import (
	"flag"
	"time"

	"github.com/KYS3K/tensorgate/internal/admission"
	"github.com/KYS3K/tensorgate/internal/lb"
)

type config struct {
	pprofAddr     string
	flushInterval time.Duration

	// admission control is off by default so the benchmarks in BENCHMARKS.md measure the bare gateway
	admissionOn bool
	admission   admission.Config

	// empty upstreams = answer with the built-in mock, otherwise balance over these URLs
	upstreams          string
	lb                 lb.Config // Upstreams is filled from the upstreams flag in run
	lbRetryAfter       time.Duration
	lbRetryAfterJitter time.Duration
}

func parseFlags() config {
	var c config

	flag.StringVar(&c.pprofAddr, "pprof", "127.0.0.1:6060", `pprof listen address, "" disables it`)
	flag.DurationVar(&c.flushInterval, "flush-interval", 100*time.Millisecond,
		"batch each stream's tokens into at most one write per interval, 0 writes every token at once")

	flag.BoolVar(&c.admissionOn, "admission", false, "enable per-key rate limiting and the concurrent stream limit")
	a := &c.admission
	// rate.Limit is a float64 underneath, so a flag can write into it through a converted pointer
	flag.Float64Var((*float64)(&a.RatePerKey), "rate-per-key", 20,
		"admission: new requests per second allowed for one API key")
	flag.IntVar(&a.BurstPerKey, "burst-per-key", 40,
		"admission: requests one API key may send at once before the rate applies")
	flag.DurationVar(&a.KeyTTL, "key-ttl", 10*time.Minute,
		"admission: forget an API key after it was idle this long; must be longer than burst/rate or an idle key regains a full bucket early")
	flag.DurationVar(&a.SweepEvery, "sweep-every", time.Minute,
		"admission: how often idle API keys are removed")
	flag.IntVar(&a.MaxKeys, "max-keys", 100_000,
		"admission: API keys tracked at once; new keys are rejected with 429 above this, which bounds memory against random-key floods")
	// the 2 vCPU benchmark holds 5000 streams within the SLO (6000 at 96% CPU, see BENCHMARKS.md)
	flag.IntVar(&a.MaxStreams, "max-streams", 5000,
		"admission: concurrent SSE streams; one more waits -max-queue-wait for a slot, then gets 503")
	flag.DurationVar(&a.MaxQueueWait, "max-queue-wait", 20*time.Millisecond,
		"admission: how long a request waits for a free stream slot before 503")
	flag.DurationVar(&a.RateLimitJitter, "rate-limit-jitter", time.Second,
		"admission: random time added to Retry-After of a 429; keep it small, the client knows when its token returns")
	// measured on 2 vCPU with 8000 clients against 5000 slots: at 1s the rejected clients came back ~2700 times a second
	// and the accepted streams' lateness p99 was 221 ms, at 10s it was 533 retries a second and 156 ms
	flag.DurationVar(&a.OverloadJitter, "overload-jitter", 10*time.Second,
		"admission: random time added to Retry-After of a 503; wide on purpose, it spreads the retries of the rejected clients")

	flag.StringVar(&c.upstreams, "upstreams", "",
		`comma-separated upstream URLs (scheme://host:port, no path) to balance over, e.g. "http://10.0.0.1:8000,http://10.0.0.2:8000"; "" answers with the built-in mock`)
	l := &c.lb
	// zero means "use the lb package default" for every lb flag below except the Retry-After ones
	flag.DurationVar(&l.CheckInterval, "lb-check-interval", 0, "lb: time between health probes of one upstream (default 2s)")
	flag.DurationVar(&l.CheckTimeout, "lb-check-timeout", 0, "lb: time an upstream has to answer a probe, must be below -lb-check-interval (default 1s)")
	flag.IntVar(&l.FailThreshold, "lb-fail-threshold", 0, "lb: failed probes in a row that mark an upstream dead (default 3)")
	flag.IntVar(&l.RiseThreshold, "lb-rise-threshold", 0, "lb: good probes in a row that bring a dead upstream back (default 2)")
	flag.IntVar(&l.PassiveThreshold, "lb-passive-threshold", 0, "lb: consecutive failed real requests (network error, 502/503/504) that mark an upstream dead (default 3)")
	flag.StringVar(&l.HealthPath, "lb-health-path", "", `lb: path probed on every upstream (default "/healthz")`)
	flag.Int64Var(&l.MaxBodyBytes, "lb-max-body", 0, "lb: request body limit in bytes, the body is buffered to be replayed on retry; above it the client gets 413 (default 1 MiB)")
	flag.DurationVar(&l.ResponseHeaderTimeout, "lb-header-timeout", 0,
		"lb: how long an upstream may take to send response headers, which for a model is about its TTFT; keep it above the TTFT p99 with a margin (default 10s)")
	flag.IntVar(&l.MaxAttempts, "lb-max-attempts", 0, "lb: tries per request, 1 turns retries off (default 2)")
	flag.IntVar(&l.RetryBudgetPercent, "lb-retry-budget-percent", 0,
		"lb: retries are limited to this percent of the requests of the last -lb-retry-budget-window (default 20)")
	flag.DurationVar(&l.RetryBudgetWindow, "lb-retry-budget-window", 0, "lb: window of the retry budget, whole seconds (default 10s)")
	flag.IntVar(&l.RetryMinPerSec, "lb-retry-min-per-sec", 0,
		"lb: retries per second allowed on top of the percentage, so a quiet gateway can still retry (default 1)")
	flag.DurationVar(&c.lbRetryAfter, "lb-retry-after", 2*time.Second,
		"lb: Retry-After of the 503 sent when every upstream is dead")
	flag.DurationVar(&c.lbRetryAfterJitter, "lb-retry-after-jitter", time.Second,
		"lb: random time added to that Retry-After")

	flag.Parse()
	return c
}

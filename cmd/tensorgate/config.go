package main

import (
	"flag"
	"time"

	"github.com/KYS3K/tensorgate/internal/admission"
)

type config struct {
	pprofAddr     string
	flushInterval time.Duration

	// admission control is off by default so the benchmarks in BENCHMARKS.md measure the bare gateway
	admissionOn bool
	admission   admission.Config
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

	flag.Parse()
	return c
}

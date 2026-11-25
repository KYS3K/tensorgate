// Package metrics defines tensorgate's Prometheus metrics
// All methods are safe to call on a nil *Metrics
package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// LBNoBackend is the backend label of requests that never reached an upstream.
const LBNoBackend = "-"

// LBClasses are the outcome classes of a request that reached an upstream.
var LBClasses = []string{"2xx", "3xx", "4xx", "5xx", "canceled", "aborted"}

type Outcome string

const (
	OutcomeCompleted      Outcome = "completed"
	OutcomeInvalidRequest Outcome = "invalid_request"
	OutcomeCanceled       Outcome = "canceled"
	OutcomeWriteError     Outcome = "write_error"
)

type AdmissionResult string

const (
	AdmissionAccepted    AdmissionResult = "accepted"
	AdmissionRejected429 AdmissionResult = "rejected_429"
	AdmissionRejected503 AdmissionResult = "rejected_503"
	AdmissionClientGone  AdmissionResult = "client_gone" // left while waiting for a slot
)

type Metrics struct {
	factory promauto.Factory

	activeStreams prometheus.Gauge
	requestsTotal *prometheus.CounterVec
	ttftSeconds   prometheus.Histogram
	tpotSeconds   prometheus.Histogram
	flushEvents   prometheus.Histogram

	lbRequestsTotal        *prometheus.CounterVec
	lbTTFTSeconds          *prometheus.HistogramVec
	lbRetriesTotal         prometheus.Counter
	lbRetryBudgetExhausted prometheus.Counter

	// counters are resolved once: WithLabelValues on the request path would hash and look up a map every time
	admissionAccepted    prometheus.Counter
	admissionRejected429 prometheus.Counter
	admissionRejected503 prometheus.Counter
	admissionClientGone  prometheus.Counter
	queueWaitSeconds     prometheus.Histogram
}

func New(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	admissionTotal := factory.NewCounterVec(prometheus.CounterOpts{
		Name: "tensorgate_admission_requests_total",
		Help: "Requests seen by admission control, by result.",
	}, []string{"result"})

	m := &Metrics{
		factory: factory,

		admissionAccepted:    admissionTotal.WithLabelValues(string(AdmissionAccepted)),
		admissionRejected429: admissionTotal.WithLabelValues(string(AdmissionRejected429)),
		admissionRejected503: admissionTotal.WithLabelValues(string(AdmissionRejected503)),
		admissionClientGone:  admissionTotal.WithLabelValues(string(AdmissionClientGone)),
		// the buckets are fine-grained on purpose: the wait is capped at tens of milliseconds
		// and p99 is interpolated inside a bucket, so coarse buckets would flatten the chart
		queueWaitSeconds: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "tensorgate_admission_queue_wait_seconds",
			Help:    "Time a request waited for a stream slot; 0 when a slot was free. Includes requests shed with 503.",
			Buckets: []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.03, 0.05, 0.1},
		}),

		activeStreams: factory.NewGauge(prometheus.GaugeOpts{
			Name: "tensorgate_active_streams",
			Help: "Number of completions streams currently open.",
		}),
		requestsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "tensorgate_requests_total",
			Help: "Completions requests by model, stream mode and outcome.",
		}, []string{"model", "stream", "outcome"}),
		// buckets reach the X-Mock-TTFT and X-Mock-TPOT maximums (30s and 5s)
		ttftSeconds: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "tensorgate_ttft_seconds",
			Help:    "Time from request arrival to the first content token of a stream.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}),
		tpotSeconds: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "tensorgate_tpot_seconds",
			Help:    "Time between consecutive content tokens of a stream, as the gateway gets them; with flush coalescing on, clients receive them in batches.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}),
		flushEvents: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "tensorgate_stream_flush_events",
			Help:    "SSE events sent per flush of a stream, i.e. per write to the client connection.",
			Buckets: []float64{1, 2, 3, 4, 5, 6, 8, 10, 15, 20, 50},
		}),
		lbRequestsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "tensorgate_lb_requests_total",
			Help: "Requests that reached the load balancer, by upstream and outcome class (2xx, 3xx, 4xx, 5xx, canceled, aborted, no_backend).",
		}, []string{"backend", "class"}),
		lbRetriesTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "tensorgate_lb_retries_total",
			Help: "Requests sent to a second upstream after the first failed before anything reached the client.",
		}),
		lbRetryBudgetExhausted: factory.NewCounter(prometheus.CounterOpts{
			Name: "tensorgate_lb_retry_budget_exhausted_total",
			Help: "Retries that were not made because the retry budget was used up.",
		}),
		lbTTFTSeconds: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "tensorgate_lb_ttft_seconds",
			Help:    "Time from request arrival at the load balancer to the first byte of a 2xx response body written to the client, by upstream.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"backend"}),
	}
	// A counter that is created by its first increment shows up already at 1 (or 267), and rate() and
	// increase() cannot see that jump: it has no earlier sample to start from. So every series is created at 0.
	for _, class := range []string{"no_backend", "4xx"} {
		m.lbRequestsTotal.WithLabelValues(LBNoBackend, class)
	}
	return m
}

func (m *Metrics) StreamStarted() {
	if m == nil {
		return
	}
	m.activeStreams.Inc()
}

func (m *Metrics) StreamEnded() {
	if m == nil {
		return
	}
	m.activeStreams.Dec()
}

func (m *Metrics) RequestDone(model string, stream bool, outcome Outcome) {
	if m == nil {
		return
	}
	m.requestsTotal.WithLabelValues(model, strconv.FormatBool(stream), string(outcome)).Inc()
}

func (m *Metrics) ObserveTTFT(d time.Duration) {
	if m == nil {
		return
	}
	m.ttftSeconds.Observe(d.Seconds())
}

func (m *Metrics) ObserveTPOT(d time.Duration) {
	if m == nil {
		return
	}
	m.tpotSeconds.Observe(d.Seconds())
}

func (m *Metrics) ObserveFlush(events int) {
	if m == nil {
		return
	}
	m.flushEvents.Observe(float64(events))
}

// AdmissionDone counts one request by its admission result. An unknown result is ignored
func (m *Metrics) AdmissionDone(result AdmissionResult) {
	if m == nil {
		return
	}
	switch result {
	case AdmissionAccepted:
		m.admissionAccepted.Inc()
	case AdmissionRejected429:
		m.admissionRejected429.Inc()
	case AdmissionRejected503:
		m.admissionRejected503.Inc()
	case AdmissionClientGone:
		m.admissionClientGone.Inc()
	}
}

func (m *Metrics) ObserveQueueWait(d time.Duration) {
	if m == nil {
		return
	}
	m.queueWaitSeconds.Observe(d.Seconds())
}

// TrackKeys exposes the number of rate-limiter keys currently in memory. size is called
// on every scrape, so it must be cheap and safe for concurrent use. Call it at most once per registry:
// a second registration of the same metric panics
func (m *Metrics) TrackKeys(size func() int) {
	if m == nil {
		return
	}
	m.factory.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "tensorgate_admission_tracked_keys",
		Help: "API keys currently tracked by the rate limiter.",
	}, func() float64 { return float64(size()) })
}

func (m *Metrics) LBRequestDone(backend, class string) {
	if m == nil {
		return
	}
	m.lbRequestsTotal.WithLabelValues(backend, class).Inc()
}

func (m *Metrics) LBRetry() {
	if m == nil {
		return
	}
	m.lbRetriesTotal.Inc()
}

func (m *Metrics) LBRetryBudgetExhausted() {
	if m == nil {
		return
	}
	m.lbRetryBudgetExhausted.Inc()
}

func (m *Metrics) ObserveLBTTFT(backend string, d time.Duration) {
	if m == nil {
		return
	}
	m.lbTTFTSeconds.WithLabelValues(backend).Observe(d.Seconds())
}

// TrackBackend exposes the state of one upstream. Both functions are called on every scrape,
// so they must be cheap and safe for concurrent use. Call it once per backend name and registry:
// a second registration of the same name panics
func (m *Metrics) TrackBackend(name string, alive func() bool, inFlight func() int64) {
	if m == nil {
		return
	}
	for _, class := range LBClasses {
		m.lbRequestsTotal.WithLabelValues(name, class) // created at 0, see New
	}
	m.lbTTFTSeconds.WithLabelValues(name)
	labels := prometheus.Labels{"backend": name}
	m.factory.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "tensorgate_lb_backend_alive",
		Help:        "1 if the load balancer sends requests to this upstream, 0 if it is marked dead.",
		ConstLabels: labels,
	}, func() float64 {
		if alive() {
			return 1
		}
		return 0
	})
	m.factory.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "tensorgate_lb_backend_inflight",
		Help:        "Requests currently in flight to this upstream.",
		ConstLabels: labels,
	}, func() float64 { return float64(inFlight()) })
}

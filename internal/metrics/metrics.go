// Package metrics defines tensorgate's Prometheus metrics
// All methods are safe to call on a nil *Metrics
package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Outcome string

const (
	OutcomeCompleted      Outcome = "completed"
	OutcomeInvalidRequest Outcome = "invalid_request"
	OutcomeCanceled       Outcome = "canceled"
	OutcomeWriteError     Outcome = "write_error"
)

type Metrics struct {
	activeStreams prometheus.Gauge
	requestsTotal *prometheus.CounterVec
	ttftSeconds   prometheus.Histogram
	tpotSeconds   prometheus.Histogram
	flushEvents   prometheus.Histogram
}

func New(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
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
	}
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

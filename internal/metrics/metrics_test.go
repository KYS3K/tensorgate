package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNilMetricsIsNoop(t *testing.T) {
	var m *Metrics // handlers built without metrics, e.g. in tests

	m.StreamStarted()
	m.StreamEnded()
	m.RequestDone("model", true, OutcomeCompleted)
	m.ObserveTTFT(time.Second)
	m.ObserveTPOT(time.Second)
	m.ObserveFlush(3)
}

func TestMetrics(t *testing.T) {
	m := New(prometheus.NewRegistry())

	m.StreamStarted()
	m.StreamStarted()
	m.StreamEnded()
	if got := testutil.ToFloat64(m.activeStreams); got != 1 {
		t.Errorf("active streams = %v, want 1", got)
	}

	m.RequestDone("a", true, OutcomeCompleted)
	m.RequestDone("a", true, OutcomeCompleted)
	m.RequestDone("a", false, OutcomeCanceled)
	if got := testutil.ToFloat64(m.requestsTotal.WithLabelValues("a", "true", "completed")); got != 2 {
		t.Errorf("completed streams = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.requestsTotal.WithLabelValues("a", "false", "canceled")); got != 1 {
		t.Errorf("canceled non-streams = %v, want 1", got)
	}
}

func TestNewTwiceOnSameRegistryPanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	New(reg)

	defer func() {
		if recover() == nil {
			t.Error("second New on the same registry should panic")
		}
	}()
	New(reg)
}

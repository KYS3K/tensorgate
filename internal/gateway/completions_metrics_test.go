package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KYS3K/tensorgate/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

// meteredCompletions returns a handler with metrics on its own registry, so tests can run in parallel
func meteredCompletions(cfg MockConfig) (*CompletionsHandler, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	return &CompletionsHandler{DefaultConfig: cfg, metrics: metrics.New(reg)}, reg
}

// gather returns the only series of the named metric
func gather(t *testing.T, reg *prometheus.Registry, name string) *dto.Metric {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == name {
			if len(f.Metric) != 1 {
				t.Fatalf("%s has %d series, want 1", name, len(f.Metric))
			}
			return f.Metric[0]
		}
	}
	t.Fatalf("metric %s not found", name)
	return nil
}

func TestCompletionsMetricsRequests(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		headers  map[string]string
		canceled bool // client is gone before the handler runs
		want     string
	}{
		{"stream completed", `{"stream":true,` + hiMessages + `}`, nil, false,
			`{model="tensorgate-mock-v0",outcome="completed",stream="true"}`},
		{"non-stream completed", `{` + hiMessages + `}`, nil, false,
			`{model="tensorgate-mock-v0",outcome="completed",stream="false"}`},
		{"invalid JSON", `{bad`, nil, false,
			`{model="other",outcome="invalid_request",stream="false"}`},
		{"invalid header", `{"stream":true,` + hiMessages + `}`, map[string]string{"X-Mock-TPOT": "0"}, false,
			`{model="tensorgate-mock-v0",outcome="invalid_request",stream="true"}`},
		// the model name comes from the client and must not become a label value
		{"unknown model", `{"model":"made-up-123","stream":true,` + hiMessages + `}`, nil, false,
			`{model="other",outcome="completed",stream="true"}`},
		{"stream client gone", `{"stream":true,` + hiMessages + `}`, map[string]string{"X-Mock-TPOT": "5s"}, true,
			`{model="tensorgate-mock-v0",outcome="canceled",stream="true"}`},
		{"non-stream client gone", `{` + hiMessages + `}`, map[string]string{"X-Mock-TTFT": "30s"}, true,
			`{model="tensorgate-mock-v0",outcome="canceled",stream="false"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, reg := meteredCompletions(MockConfig{TPOT: time.Millisecond, Tokens: 3})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.canceled {
				cancel()
			}
			req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", strings.NewReader(tt.body))
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			h.ServeHTTP(httptest.NewRecorder(), req)

			want := `
# HELP tensorgate_requests_total Completions requests by model, stream mode and outcome.
# TYPE tensorgate_requests_total counter
tensorgate_requests_total` + tt.want + " 1\n"
			if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "tensorgate_requests_total"); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestCompletionsMetricsLatency(t *testing.T) {
	t.Parallel()
	const (
		ttft   = 20 * time.Millisecond
		tpot   = 10 * time.Millisecond
		tokens = 4
	)
	h, reg := meteredCompletions(MockConfig{TTFT: ttft, TPOT: tpot, Tokens: tokens})
	post(t, h, `{"stream":true,`+hiMessages+`}`)

	got := gather(t, reg, "tensorgate_ttft_seconds").GetHistogram()
	if got.GetSampleCount() != 1 {
		t.Errorf("ttft count = %d, want 1", got.GetSampleCount())
	}
	if s := got.GetSampleSum(); s < ttft.Seconds() || s > 1 {
		t.Errorf("ttft = %vs, want about %v", s, ttft)
	}

	// one gap between each pair of tokens
	got = gather(t, reg, "tensorgate_tpot_seconds").GetHistogram()
	if got.GetSampleCount() != tokens-1 {
		t.Errorf("tpot count = %d, want %d", got.GetSampleCount(), tokens-1)
	}
	// a ticker tick can land slightly early relative to the first send, hence the slack
	if s, lo := got.GetSampleSum(), 0.8*(tokens-1)*tpot.Seconds(); s < lo || s > 1 {
		t.Errorf("tpot sum = %vs, want about %vs", s, (tokens-1)*tpot.Seconds())
	}
}

func TestCompletionsMetricsActiveStreams(t *testing.T) {
	t.Parallel()
	h, reg := meteredCompletions(MockConfig{TPOT: time.Hour, Tokens: 2}) // blocks after the first token
	active := func() float64 { return gather(t, reg, "tensorgate_active_streams").GetGauge().GetValue() }

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"stream":true,`+hiMessages+`}`))
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(httptest.NewRecorder(), req)
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for active() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("active streams = %v while streaming, want 1", active())
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	<-done
	if got := active(); got != 0 {
		t.Errorf("active streams = %v after the stream ended, want 0", got)
	}
}

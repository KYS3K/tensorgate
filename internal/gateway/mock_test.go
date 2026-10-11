package gateway

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestResolveConfigDefaults(t *testing.T) {
	h := &CompletionsHandler{DefaultConfig: DefaultMockConfig()}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)

	cfg, err := h.resolveConfig(req)
	if err != nil {
		t.Fatal(err)
	}
	if cfg != DefaultMockConfig() {
		t.Errorf("cfg = %+v, want defaults %+v", cfg, DefaultMockConfig())
	}
}

func TestResolveConfigBoundaries(t *testing.T) {
	def := DefaultMockConfig()

	tests := []struct {
		name   string
		header string
		value  string
		want   MockConfig // checked only when wantErr is empty
		// wantErr is a substring of the error; "" means the value must be accepted
		wantErr string
	}{
		// both formats
		{"duration with unit", "X-Mock-TTFT", "1.5s", MockConfig{1500 * time.Millisecond, def.TPOT, def.Tokens}, ""},
		{"bare number is ms", "X-Mock-TTFT", "250", MockConfig{250 * time.Millisecond, def.TPOT, def.Tokens}, ""},
		{"mixed units", "X-Mock-TTFT", "1s500ms", MockConfig{1500 * time.Millisecond, def.TPOT, def.Tokens}, ""},

		// TTFT: [0, 30s]
		{"TTFT zero allowed", "X-Mock-TTFT", "0", MockConfig{0, def.TPOT, def.Tokens}, ""},
		{"TTFT at max", "X-Mock-TTFT", "30s", MockConfig{maxTTFT, def.TPOT, def.Tokens}, ""},
		{"TTFT just over max", "X-Mock-TTFT", "30001", MockConfig{}, "between"},
		{"TTFT negative", "X-Mock-TTFT", "-1ms", MockConfig{}, "between"},
		{"TTFT negative bare", "X-Mock-TTFT", "-5", MockConfig{}, "between"},

		// TPOT: [1ms, 5s]; zero would panic in time.NewTicker
		{"TPOT at min", "X-Mock-TPOT", "1", MockConfig{def.TTFT, time.Millisecond, def.Tokens}, ""},
		{"TPOT at max", "X-Mock-TPOT", "5s", MockConfig{def.TTFT, maxTPOT, def.Tokens}, ""},
		{"TPOT zero", "X-Mock-TPOT", "0", MockConfig{}, "between"},
		{"TPOT below min", "X-Mock-TPOT", "999us", MockConfig{}, "between"},
		{"TPOT just over max", "X-Mock-TPOT", "5001ms", MockConfig{}, "between"},
		{"TPOT huge", "X-Mock-TPOT", "1000h", MockConfig{}, "between"},

		// overflow: 10^13 ms doesn't fit in a Duration (max ~292 years);
		// a naive time.Duration(ms) * time.Millisecond would wrap to a garbage value
		{"bare ms overflow", "X-Mock-TTFT", "10000000000000", MockConfig{}, "want a duration"},
		{"unit overflow", "X-Mock-TTFT", "999999999h", MockConfig{}, "want a duration"},

		// garbage
		{"words", "X-Mock-TTFT", "2 seconds", MockConfig{}, "want a duration"},
		{"unknown unit", "X-Mock-TTFT", "5d", MockConfig{}, "want a duration"},
		{"float without unit", "X-Mock-TTFT", "1.5", MockConfig{}, "want a duration"},

		// Tokens: [1, 10000]
		{"tokens at min", "X-Mock-Tokens", "1", MockConfig{def.TTFT, def.TPOT, 1}, ""},
		{"tokens at max", "X-Mock-Tokens", "10000", MockConfig{def.TTFT, def.TPOT, maxTokens}, ""},
		{"tokens zero", "X-Mock-Tokens", "0", MockConfig{}, "between"},
		{"tokens negative", "X-Mock-Tokens", "-3", MockConfig{}, "between"},
		{"tokens over max", "X-Mock-Tokens", "10001", MockConfig{}, "between"},
		{"tokens overflow int", "X-Mock-Tokens", "99999999999999999999", MockConfig{}, "want an integer"},
		{"tokens float", "X-Mock-Tokens", "2.5", MockConfig{}, "want an integer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &CompletionsHandler{DefaultConfig: def}
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			req.Header.Set(tt.header, tt.value)

			cfg, err := h.resolveConfig(req)

			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if cfg != tt.want {
					t.Errorf("cfg = %+v, want %+v", cfg, tt.want)
				}
				return
			}

			herr, ok := errors.AsType[*HeaderError](err)
			if !ok {
				t.Fatalf("err = %v, want *HeaderError", err)
			}
			if herr.Header != tt.header {
				t.Errorf("Header = %q, want %q", herr.Header, tt.header)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("err = %q, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

func TestResolveConfigFirstBadHeaderWins(t *testing.T) {
	h := &CompletionsHandler{DefaultConfig: DefaultMockConfig()}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("X-Mock-TTFT", "100ms") // valid
	req.Header.Set("X-Mock-TPOT", "0")     // invalid
	req.Header.Set("X-Mock-Tokens", "abc") // invalid, but not reached

	_, err := h.resolveConfig(req)
	if herr, ok := errors.AsType[*HeaderError](err); !ok || herr.Header != "X-Mock-TPOT" {
		t.Fatalf("err = %v, want HeaderError for X-Mock-TPOT", err)
	}
}

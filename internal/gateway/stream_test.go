package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KYS3K/tensorgate/internal/sse"
)

type event struct {
	typ, data string
}

// testStreamer doesn't wait 500ms per token
func testStreamer() *Streamer {
	return &Streamer{Tokens: 5, Interval: time.Millisecond, Retry: 2 * time.Second}
}

func collect(t *testing.T, body string) []event {
	t.Helper()
	var got []event
	err := sse.ReadEvents(strings.NewReader(body), func(typ, data string) {
		got = append(got, event{typ, data})
	})
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	return got
}

func tokens(from, to int) []event {
	var ev []event
	for id := from; id <= to; id++ {
		ev = append(ev, event{"message", fmt.Sprintf(`{"chunk":%d,"text":"token_%d"}`, id, id)})
	}
	return append(ev, event{"message", "[DONE]"})
}

func TestStreamer(t *testing.T) {
	tests := []struct {
		name        string
		lastEventID string
		wantStatus  int
		want        []event
	}{
		{"fresh stream", "", http.StatusOK, tokens(1, 5)},
		{"resume", "3", http.StatusOK, tokens(4, 5)},
		{"resume from zero", "0", http.StatusOK, tokens(1, 5)},
		{"already finished", "5", http.StatusNoContent, nil},
		{"past the end", "99", http.StatusNoContent, nil},
		{"not a number", "abc", http.StatusBadRequest, nil},
		{"negative", "-1", http.StatusBadRequest, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, "/events", nil)
			if tt.lastEventID != "" {
				req.Header.Set("Last-Event-ID", tt.lastEventID)
			}
			rec := httptest.NewRecorder()

			testStreamer().ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if tt.wantStatus != http.StatusOK {
				if strings.Contains(rec.Body.String(), "data:") {
					t.Fatalf("non-200 response must not stream events, got %q", rec.Body.String())
				}
				return
			}

			if !strings.HasPrefix(rec.Body.String(), "retry: 2000\n\n") {
				t.Errorf("stream must start with retry field, got %q", rec.Body.String())
			}
			if got := collect(t, rec.Body.String()); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("events =\n%v\nwant\n%v", got, tt.want)
			}
		})
	}
}

func TestStreamerIDs(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	req.Header.Set("Last-Event-ID", "2")
	rec := httptest.NewRecorder()

	testStreamer().ServeHTTP(rec, req)

	// ReadEvents ignores id:, so check the raw lines
	var ids []string
	for line := range strings.Lines(rec.Body.String()) {
		if id, ok := strings.CutPrefix(line, "id: "); ok {
			ids = append(ids, strings.TrimSuffix(id, "\n"))
		}
	}
	if want := []string{"3", "4", "5"}; !reflect.DeepEqual(ids, want) {
		t.Errorf("ids = %v, want %v", ids, want)
	}
}

func TestStreamerClientGone(t *testing.T) {
	s := testStreamer()
	s.Interval = time.Hour // make sure the context wins the select

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/events", nil)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		s.ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not return after client disconnected")
	}
	if strings.Contains(rec.Body.String(), "data:") {
		t.Errorf("no events expected after disconnect, got %q", rec.Body.String())
	}
}

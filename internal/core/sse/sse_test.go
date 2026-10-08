package sse

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

type event struct {
	typ, data string
}

func TestMain(m *testing.M) {
	// don't wait 500ms per token in tests
	tickInterval = time.Millisecond
	os.Exit(m.Run())
}

func collect(t *testing.T, body string) []event {
	t.Helper()
	var got []event
	err := ReadEvents(strings.NewReader(body), func(typ, data string) {
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

func TestHandler(t *testing.T) {
	tests := []struct {
		name        string
		lastEventID string
		wantStatus  int
		want        []event
	}{
		{"fresh stream", "", http.StatusOK, tokens(1, totalEvents)},
		{"resume", "3", http.StatusOK, tokens(4, totalEvents)},
		{"resume from zero", "0", http.StatusOK, tokens(1, totalEvents)},
		{"already finished", "5", http.StatusNoContent, nil},
		{"past the end", "99", http.StatusNoContent, nil},
		{"not a number", "abc", http.StatusBadRequest, nil},
		{"negative", "-1", http.StatusBadRequest, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/events", nil)
			if tt.lastEventID != "" {
				req.Header.Set("Last-Event-ID", tt.lastEventID)
			}
			rec := httptest.NewRecorder()

			Handler(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if tt.wantStatus != http.StatusOK {
				if strings.Contains(rec.Body.String(), "data:") {
					t.Fatalf("non-200 response must not stream events, got %q", rec.Body.String())
				}
				return
			}

			if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
				t.Errorf("Content-Type = %q", ct)
			}
			if !strings.HasPrefix(rec.Body.String(), fmt.Sprintf("retry: %d\n\n", retryMs)) {
				t.Errorf("stream must start with retry field, got %q", rec.Body.String())
			}
			if got := collect(t, rec.Body.String()); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("events =\n%v\nwant\n%v", got, tt.want)
			}
		})
	}
}

func TestHandlerIDs(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	req.Header.Set("Last-Event-ID", "2")
	rec := httptest.NewRecorder()

	Handler(rec, req)

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

func TestHandlerClientGone(t *testing.T) {
	old := tickInterval
	tickInterval = time.Hour // make sure the context wins the select
	t.Cleanup(func() { tickInterval = old })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/events", nil)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		Handler(rec, req)
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

func TestReadEvents(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []event
	}{
		{"default type", "data: hi\n\n", []event{{"message", "hi"}}},
		{"named event", "event: ping\ndata: x\n\n", []event{{"ping", "x"}}},
		{"multi-line data", "data: a\ndata: b\n\n", []event{{"message", "a\nb"}}},
		{"comment ignored", ": keep-alive\ndata: x\n\n", []event{{"message", "x"}}},
		{"no data, no dispatch", "retry: 2000\n\nevent: ping\n\n", nil},
		{"only one space trimmed", "data:  world\n\n", []event{{"message", " world"}}},
		{"no space after colon", "data:x\n\n", []event{{"message", "x"}}},
		{"empty data line", "data\n\n", []event{{"message", ""}}},
		{"crlf line endings", "data: x\r\n\r\n", []event{{"message", "x"}}},
		{"unterminated event dropped", "data: x\n", nil},
		{"event type resets", "event: a\ndata: 1\n\ndata: 2\n\n", []event{{"a", "1"}, {"message", "2"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := collect(t, tt.in); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadEventsLongLine(t *testing.T) {
	big := strings.Repeat("x", 200*1024) // over the default 64KB scanner limit
	got := collect(t, "data: "+big+"\n\n")
	if len(got) != 1 || got[0].data != big {
		t.Fatalf("200KB line not read intact")
	}

	tooBig := strings.Repeat("x", 1<<20)
	err := ReadEvents(strings.NewReader("data: "+tooBig+"\n\n"), func(string, string) {})
	if !errors.Is(err, bufio.ErrTooLong) {
		t.Fatalf("err = %v, want bufio.ErrTooLong", err)
	}
}

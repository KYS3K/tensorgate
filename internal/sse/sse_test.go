package sse

import (
	"bufio"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

type event struct {
	typ, data string
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

func TestNewWriterHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	NewWriter(rec)

	if rec.Code != 200 {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("Cache-Control = %q", cc)
	}
}

func TestWriterRetry(t *testing.T) {
	rec := httptest.NewRecorder()
	if err := NewWriter(rec).Retry(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	if got := rec.Body.String(); got != "retry: 2000\n\n" {
		t.Errorf("body = %q", got)
	}
	if !rec.Flushed {
		t.Error("retry was not flushed")
	}
}

func TestWriterSend(t *testing.T) {
	tests := []struct {
		name string
		in   Event
		wire string
	}{
		{"data only", Event{Data: "hi"}, "data: hi\n\n"},
		{"all fields", Event{ID: "7", Type: "ping", Data: "x"}, "id: 7\nevent: ping\ndata: x\n\n"},
		{"multi-line data", Event{Data: "a\nb"}, "data: a\ndata: b\n\n"},
		{"crlf in data", Event{Data: "a\r\nb\rc"}, "data: a\ndata: b\ndata: c\n\n"},
		{"empty data", Event{}, "data: \n\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			sw := NewWriter(rec)
			if err := sw.Send(tt.in); err != nil {
				t.Fatal(err)
			}
			if got := rec.Body.String(); got != tt.wire {
				t.Errorf("wire = %q, want %q", got, tt.wire)
			}
		})
	}
}

func TestWriterSendRejectsLineBreaks(t *testing.T) {
	for _, e := range []Event{
		{ID: "1\ndata: injected", Data: "x"},
		{Type: "a\rb", Data: "x"},
	} {
		rec := httptest.NewRecorder()
		if err := NewWriter(rec).Send(e); !errors.Is(err, ErrInvalidField) {
			t.Errorf("Send(%+v) err = %v, want ErrInvalidField", e, err)
		}
		if rec.Body.Len() != 0 {
			t.Errorf("rejected event was written: %q", rec.Body.String())
		}
	}
}

// what Writer sends, ReadEvents must read back unchanged
func TestWriterRoundTrip(t *testing.T) {
	in := []Event{
		{Data: "plain"},
		{Type: "token", Data: " leading space"},
		{Data: "line one\nline two\n"},
		{Data: `{"json":true}`},
	}

	rec := httptest.NewRecorder()
	sw := NewWriter(rec)
	for _, e := range in {
		if err := sw.Send(e); err != nil {
			t.Fatal(err)
		}
	}

	want := []event{
		{"message", "plain"},
		{"token", " leading space"},
		{"message", "line one\nline two\n"},
		{"message", `{"json":true}`},
	}
	if got := collect(t, rec.Body.String()); !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
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

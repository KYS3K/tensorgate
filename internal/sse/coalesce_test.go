package sse

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// flushCounter is a ResponseRecorder that counts flushes and remembers what each one sent
type flushCounter struct {
	*httptest.ResponseRecorder
	sent    int
	flushes []string // body sent by each flush
}

func newFlushCounter() *flushCounter {
	return &flushCounter{ResponseRecorder: httptest.NewRecorder()}
}

func (f *flushCounter) Flush() {
	body := f.Body.String()
	f.flushes = append(f.flushes, body[f.sent:])
	f.sent = len(body)
}

func add(t *testing.T, c *Coalescer, data string, more bool) int {
	t.Helper()
	n, err := c.Add([]byte(data), more)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCoalescerZeroIntervalFlushesEachEvent(t *testing.T) {
	fc := newFlushCounter()
	c := NewCoalescer(NewWriter(fc), 0)

	for i, d := range []string{"a", "b", "c"} {
		if n := add(t, c, d, false); n != 1 {
			t.Errorf("event %d: flushed %d events, want 1", i, n)
		}
	}
	if len(fc.flushes) != 3 {
		t.Errorf("flushes = %d, want 3", len(fc.flushes))
	}
	if c.C() != nil {
		t.Error("C is armed with nothing pending")
	}
}

// what goes out batched must be byte-for-byte what SendData would send event by event
func TestCoalescerWireMatchesSendData(t *testing.T) {
	events := []string{"one", "two\nlines", `{"json":1}`, ""}

	want := httptest.NewRecorder()
	sw := NewWriter(want)
	for _, e := range events {
		if err := sw.SendData([]byte(e)); err != nil {
			t.Fatal(err)
		}
	}

	fc := newFlushCounter()
	c := NewCoalescer(NewWriter(fc), time.Hour)
	for _, e := range events {
		add(t, c, e, false)
	}
	if _, err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := fc.Body.String(); got != want.Body.String() {
		t.Errorf("wire = %q, want %q", got, want.Body.String())
	}
}

func TestCoalescerMoreHoldsBurst(t *testing.T) {
	fc := newFlushCounter()
	c := NewCoalescer(NewWriter(fc), 0)

	add(t, c, "a", true)
	add(t, c, "b", true)
	if len(fc.flushes) != 0 {
		t.Fatalf("flushed %d times while more events were at hand", len(fc.flushes))
	}
	if n := add(t, c, "c", false); n != 3 {
		t.Errorf("burst flushed %d events, want 3", n)
	}
	if want := "data: a\n\ndata: b\n\ndata: c\n\n"; len(fc.flushes) != 1 || fc.flushes[0] != want {
		t.Errorf("flushes = %q, want one with %q", fc.flushes, want)
	}
}

func TestCoalescerInterval(t *testing.T) {
	const interval = 50 * time.Millisecond
	fc := newFlushCounter()
	c := NewCoalescer(NewWriter(fc), interval)

	// the first event after a pause goes out at once
	if n := add(t, c, "first", false); n != 1 {
		t.Fatalf("first event flushed %d, want 1", n)
	}
	start := time.Now()

	// the next ones are held until the interval since the previous flush is up
	add(t, c, "second", false)
	add(t, c, "third", false)
	if len(fc.flushes) != 1 {
		t.Fatalf("flushes = %d inside the interval, want 1", len(fc.flushes))
	}
	if c.C() == nil {
		t.Fatal("C isn't armed while events wait")
	}

	select {
	case <-c.C():
	case <-time.After(time.Second):
		t.Fatal("C didn't fire")
	}
	if waited := time.Since(start); waited < interval-5*time.Millisecond {
		t.Errorf("C fired after %v, before the interval %v", waited, interval)
	}
	n, err := c.Flush()
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 || len(fc.flushes) != 2 || fc.flushes[1] != "data: second\n\ndata: third\n\n" {
		t.Errorf("second flush sent %d events %q, want second and third together", n, fc.flushes)
	}
	if c.C() != nil {
		t.Error("C still armed after Flush")
	}
}

func TestCoalescerEventAfterPauseIsNotDelayed(t *testing.T) {
	const interval = 20 * time.Millisecond
	fc := newFlushCounter()
	c := NewCoalescer(NewWriter(fc), interval)

	add(t, c, "a", false)
	time.Sleep(interval + 5*time.Millisecond)
	if n := add(t, c, "b", false); n != 1 {
		t.Errorf("event after a pause longer than the interval flushed %d, want 1 right away", n)
	}
}

func TestCoalescerFlushWithNothingPending(t *testing.T) {
	fc := newFlushCounter()
	c := NewCoalescer(NewWriter(fc), time.Hour)
	if n, err := c.Flush(); n != 0 || err != nil {
		t.Errorf("Flush() = %d, %v; want 0, nil", n, err)
	}
	if len(fc.flushes) != 0 {
		t.Error("empty Flush reached the connection")
	}
}

// the role chunk + first token pattern: held with more, then out at once on the first plain Add
func TestCoalescerFirstBurstGoesOutAtOnce(t *testing.T) {
	fc := newFlushCounter()
	c := NewCoalescer(NewWriter(fc), time.Hour)
	add(t, c, "role", true)
	if n := add(t, c, "token", false); n != 2 {
		t.Errorf("first burst flushed %d, want 2", n)
	}
	if !strings.Contains(fc.flushes[0], "role") || !strings.Contains(fc.flushes[0], "token") {
		t.Errorf("first flush = %q", fc.flushes[0])
	}
}

package sse

import "time"

// Coalescer batches events of one stream into fewer flushes. Each flush is a write syscall and
// usually a TCP packet with an ACK coming back, and under load that per-packet cost, not the
// bytes, is what runs out first; a few tokens per flush cut it several times.
//
// Add buffers an event. The buffer goes out:
//   - never while the caller says the next event is already at hand (more), so a burst
//     leaves as one write. This adds no delay;
//   - right away if the previous flush was at least interval ago, so the first event
//     after a pause, such as the first token of a stream, is never held back;
//   - otherwise when the interval since the previous flush is up: the caller selects
//     on C and calls Flush then.
//
// So an event waits at most interval, and a stream flushes at most once per interval.
// With interval 0 every event that isn't followed by more is flushed at once.
//
// Like Writer, a Coalescer is not safe for concurrent use.
type Coalescer struct {
	w        *Writer
	interval time.Duration
	last     time.Time // previous flush
	pending  int       // events buffered since then
	timer    *time.Timer
	armed    bool // timer runs and fires when pending events are due
}

func NewCoalescer(w *Writer, interval time.Duration) *Coalescer {
	t := time.NewTimer(time.Hour)
	t.Stop()
	return &Coalescer{w: w, interval: interval, timer: t}
}

// Add buffers data as a data-only event (see Writer.SendData) and flushes if it's time to.
// It returns the number of events flushed, 0 if the event was only buffered.
func (c *Coalescer) Add(data []byte, more bool) (int, error) {
	if err := c.w.WriteData(data); err != nil {
		return 0, err
	}
	c.pending++
	if more {
		return 0, nil
	}

	now := time.Now()
	if wait := c.interval - now.Sub(c.last); wait > 0 {
		if !c.armed {
			c.timer.Reset(wait)
			c.armed = true
		}
		return 0, nil
	}
	return c.flush(now)
}

// C fires when buffered events are due; then call Flush. It is nil while nothing waits,
// and a nil channel never fires in a select.
func (c *Coalescer) C() <-chan time.Time {
	if !c.armed {
		return nil
	}
	return c.timer.C
}

// Flush sends the buffered events now, e.g. at the end of the stream or when C fires.
// It returns the number of events sent.
func (c *Coalescer) Flush() (int, error) {
	return c.flush(time.Now())
}

func (c *Coalescer) flush(now time.Time) (int, error) {
	if c.armed {
		c.timer.Stop()
		c.armed = false
	}
	n := c.pending
	if n == 0 {
		return 0, nil
	}
	c.pending = 0
	c.last = now
	return n, c.w.Flush()
}

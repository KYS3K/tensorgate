// Package sse implements the text/event-stream wire format: writing events to an
// http.ResponseWriter and parsing them back from a stream.
package sse

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Event struct {
	ID   string
	Type string
	Data string
}

var ErrInvalidField = errors.New("sse: id and event type must not contain line breaks")

type Writer struct {
	w  http.ResponseWriter
	rc *http.ResponseController
}

// NewWriter sets the event-stream headers and sends the 200 status line.
// Call it only once you know the request is valid: the status can't change afterwards.
func NewWriter(w http.ResponseWriter) *Writer {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	// stop nginx from buffering the stream
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	return &Writer{w: w, rc: http.NewResponseController(w)}
}

// Retry tells the client how long to wait before reconnecting.
func (s *Writer) Retry(d time.Duration) error {
	if _, err := fmt.Fprintf(s.w, "retry: %d\n\n", d.Milliseconds()); err != nil {
		return err
	}
	return s.rc.Flush()
}

// Send writes one event and flushes it, write or flush error means the client is gone
func (s *Writer) Send(e Event) error {
	//prevent injection like ID = "adasd\ndata:...." - \n triggers field parsing and
	//can lead to data injection
	if strings.ContainsAny(e.ID, "\r\n") || strings.ContainsAny(e.Type, "\r\n") {
		return ErrInvalidField
	}

	var b strings.Builder
	if e.ID != "" {
		fmt.Fprintf(&b, "id: %s\n", e.ID)
	}
	if e.Type != "" {
		fmt.Fprintf(&b, "event: %s\n", e.Type)
	}
	// a line break inside data would end the field early, so each line gets its own data: field;
	// the client joins them back with \n
	data := strings.ReplaceAll(e.Data, "\r\n", "\n")
	data = strings.ReplaceAll(data, "\r", "\n")
	for line := range strings.SplitSeq(data, "\n") {
		fmt.Fprintf(&b, "data: %s\n", line)
	}
	b.WriteByte('\n')

	if _, err := io.WriteString(s.w, b.String()); err != nil {
		return err
	}
	//don't w8 for 4KB buffer being filled
	return s.rc.Flush()
}

// ReadEvents is a minimal text/event-stream parser, use it in tests or in a service that consumes SSE.
// It calls fn for every dispatched event; id and retry fields are ignored.
func ReadEvents(r io.Reader, fn func(event, data string)) error {
	sc := bufio.NewScanner(r)

	// default max line is 64KB; longer lines make Scan fail -> raise the cap to 1MB
	sc.Buffer(make([]byte, 0, 4096), 1<<20)

	var event string
	var data []string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "": // event border
			if len(data) > 0 { // no dispatched event without data
				if event == "" {
					event = "message"
				}
				fn(event, strings.Join(data, "\n"))
			}
			event, data = "", nil
		case strings.HasPrefix(line, ":"): // comment
		default:
			field, value, _ := strings.Cut(line, ":")
			value = strings.TrimPrefix(value, " ") // removing exactly one white space
			switch field {
			case "event":
				event = value
			case "data":
				data = append(data, value)
			}
		}
	}
	return sc.Err()
}

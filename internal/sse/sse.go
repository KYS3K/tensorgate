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

// Writer is not safe for concurrent use, same as the http.ResponseWriter it wraps.
type Writer struct {
	w  http.ResponseWriter
	rc *http.ResponseController
	// reused across events, so once it has grown to the largest event Send doesn't allocate
	buf []byte
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

	b := s.buf[:0]
	if e.ID != "" {
		b = append(b, "id: "...)
		b = append(b, e.ID...)
		b = append(b, '\n')
	}
	if e.Type != "" {
		b = append(b, "event: "...)
		b = append(b, e.Type...)
		b = append(b, '\n')
	}
	b = appendData(b, e.Data)
	if err := s.write(b); err != nil {
		return err
	}
	return s.Flush()
}

// SendData sends a data-only event, the same as Send(Event{Data: string(data)}) without
// converting data to a string. Writer doesn't keep data, so the caller may reuse it.
func (s *Writer) SendData(data []byte) error {
	if err := s.WriteData(data); err != nil {
		return err
	}
	return s.Flush()
}

// WriteData is SendData without the flush: the event stays in the response buffer until
// Flush, or until the buffer fills up. See Coalescer.
func (s *Writer) WriteData(data []byte) error {
	return s.write(appendData(s.buf[:0], data))
}

// Flush sends everything written so far to the client
func (s *Writer) Flush() error {
	//don't w8 for 4KB buffer being filled
	return s.rc.Flush()
}

func (s *Writer) write(b []byte) error {
	s.buf = b
	_, err := s.w.Write(b)
	return err
}

// appendData appends data as data: fields and the blank line that ends the event.
// A line break inside data would end the field early, so each line gets its own data: field;
// \r\n, \r and \n all count as line breaks and the client joins the lines back with \n.
func appendData[T string | []byte](b []byte, data T) []byte {
	for {
		b = append(b, "data: "...)
		i := indexLineBreak(data)
		if i < 0 {
			b = append(b, data...)
			return append(b, '\n', '\n')
		}
		b = append(b, data[:i]...)
		b = append(b, '\n')
		if data[i] == '\r' && i+1 < len(data) && data[i+1] == '\n' {
			i++
		}
		data = data[i+1:]
	}
}

func indexLineBreak[T string | []byte](data T) int {
	for i := 0; i < len(data); i++ {
		if c := data[i]; c == '\n' || c == '\r' {
			return i
		}
	}
	return -1
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

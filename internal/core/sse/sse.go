package sse

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	totalEvents = 5
	retryMs     = 2000
)

// simulates some computations, moved to var so tests can reuse it
var tickInterval = 500 * time.Millisecond

type chunk struct {
	Chunk int    `json:"chunk"`
	Text  string `json:"text"`
}

func Handler(w http.ResponseWriter, r *http.Request) {
	//start from last event
	next := 1
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		id, err := strconv.Atoi(v)
		if err != nil || id < 0 {
			http.Error(w, "bad Last-Event-ID", http.StatusBadRequest)
			return
		}
		next = id + 1
	}

	if next > totalEvents {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	//retry without "data"
	fmt.Fprintf(w, "retry: %d\n\n", retryMs)
	if err := rc.Flush(); err != nil {
		//client dropped before stream started
		return
	}

	// simulate some heavy computations
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for id := next; id <= totalEvents; id++ {
		select {
		case <-r.Context().Done():
			//client ended his session
			return
		case <-ticker.C:
		}

		payload, _ := json.Marshal(chunk{Chunk: id, Text: fmt.Sprintf("token_%d", id)})
		//omit writing "event", it's type is "message" by default
		fmt.Fprintf(w, "id: %d\ndata: %s\n\n", id, payload)
		if err := rc.Flush(); err != nil {
			return
		}
	}

	fmt.Fprintf(w, "data: [DONE]\n\n")
	//don't wait till buffer will fill 4KB
	_ = rc.Flush()
}

// ReadEvents is a minimal parser text/event-stream, use it in tests or in service that consumes SSE
func ReadEvents(r io.Reader, fn func(event, data string)) error {
	sc := bufio.NewScanner(r)

	//by default scanner trims string longer than 64KB
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)

	var event string
	var data []string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "": //event border
			if len(data) > 0 { //no dispatched event without data
				if event == "" {
					event = "message"
				}
				fn(event, strings.Join(data, "\n"))
			}
			event, data = "", nil
		case strings.HasPrefix(line, ":"): //comment
		default:
			field, value, _ := strings.Cut(line, ":")
			value = strings.TrimPrefix(value, " ") //removing exactly one white space
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

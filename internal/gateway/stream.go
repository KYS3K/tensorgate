// Package gateway holds tensorgate's HTTP handlers.
package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/KYS3K/tensorgate/internal/sse"
)

type chunk struct {
	Chunk int    `json:"chunk"`
	Text  string `json:"text"`
}

type Streamer struct {
	Tokens   int
	Interval time.Duration
	Retry    time.Duration
}

// NewStreamer returns a Streamer with demo defaults.
func NewStreamer() *Streamer {
	return &Streamer{
		Tokens:   5,
		Interval: 500 * time.Millisecond,
		Retry:    2 * time.Second,
	}
}

func (s *Streamer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// start from last event
	next := 1
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		id, err := strconv.Atoi(v)
		if err != nil || id < 0 {
			http.Error(w, "bad Last-Event-ID", http.StatusBadRequest)
			return
		}
		next = id + 1
	}

	if next > s.Tokens {
		// 204 tells the browser's EventSource to stop reconnecting
		w.WriteHeader(http.StatusNoContent)
		return
	}

	sw := sse.NewWriter(w)
	if err := sw.Retry(s.Retry); err != nil {
		// client dropped before stream started
		return
	}

	// simulate some heavy computations
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	for id := next; id <= s.Tokens; id++ {
		select {
		case <-r.Context().Done():
			// client ended session or server is down
			return
		case <-ticker.C:
		}

		payload, _ := json.Marshal(chunk{Chunk: id, Text: fmt.Sprintf("token_%d", id)})
		if err := sw.Send(sse.Event{ID: strconv.Itoa(id), Data: string(payload)}); err != nil {
			return
		}
	}

	_ = sw.Send(sse.Event{Data: "[DONE]"})
}

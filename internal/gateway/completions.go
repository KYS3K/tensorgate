package gateway

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/KYS3K/tensorgate/internal/sse"
)

const (
	defaultModel = "tensorgate-mock-v0"
	// so a client can't make us buffer arbitrary amounts of JSON
	maxBodyBytes = 1 << 20
)

// CompletionsHandler serves an OpenAI-compatible /v1/chat/completions endpoint with mock output
type CompletionsHandler struct {
	Tokens int
	Interval time.Duration
}

func NewCompletionsHandler() *CompletionsHandler {
	return &CompletionsHandler{
		Tokens:   5,
		Interval: 100 * time.Millisecond,
	}
}

func (h *CompletionsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	var req ChatCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeds %d bytes", maxBodyBytes), "")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error(), "")
		return
	}

	if len(req.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "messages must be a non-empty array", "messages")
		return
	}
	for i, m := range req.Messages {
		if m.Role == "" {
			writeError(w, http.StatusBadRequest, "message is missing role", fmt.Sprintf("messages[%d].role", i))
			return
		}
	}

	if req.Model == "" {
		req.Model = defaultModel
	}

	c := completion{id: "chatcmpl-" + rand.Text(), created: time.Now().Unix(), model: req.Model}
	if req.Stream {
		h.handleStream(w, r, c)
		return
	}
	h.handleNonStream(w, c)
}

// completion holds what every response or chunk of one request shares
type completion struct {
	id      string
	created int64
	model   string
}

func (c completion) chunk(delta ChunkDelta, finishReason *string) ChatCompletionChunk {
	return ChatCompletionChunk{
		ID:      c.id,
		Object:  "chat.completion.chunk",
		Created: c.created,
		Model:   c.model,
		Choices: []ChunkChoice{{Index: 0, Delta: delta, FinishReason: finishReason}},
	}
}

func (h *CompletionsHandler) handleNonStream(w http.ResponseWriter, c completion) {
	resp := ChatCompletionResponse{
		ID:      c.id,
		Object:  "chat.completion",
		Created: c.created,
		Model:   c.model,
		Choices: []Choice{{
			Index:        0,
			Message:      ChatMessage{Role: "assistant", Content: "Hello from Tensorgate. This is mock response"},
			FinishReason: "stop",
		}},
		Usage: Usage{
			PromptTokens:     10,
			CompletionTokens: 10,
			TotalTokens:      20,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *CompletionsHandler) handleStream(w http.ResponseWriter, r *http.Request, c completion) {
	sw := sse.NewWriter(w)
	ticker := time.NewTicker(h.Interval)
	defer ticker.Stop()

	// OpenAI sends the role alone in the first chunk, before any content
	if err := sendChunk(sw, c.chunk(ChunkDelta{Role: "assistant"}, nil)); err != nil {
		return
	}

	for i := 1; i <= h.Tokens; i++ {
		select {
		case <-r.Context().Done():
			// client ended session or server is down
			return
		case <-ticker.C:
		}

		if err := sendChunk(sw, c.chunk(ChunkDelta{Content: fmt.Sprintf(" token_%d", i)}, nil)); err != nil {
			return
		}
	}

	stop := "stop"
	if err := sendChunk(sw, c.chunk(ChunkDelta{}, &stop)); err != nil {
		return
	}

	// signal marker
	_ = sw.Send(sse.Event{Data: "[DONE]"})
}

func sendChunk(sw *sse.Writer, chunk ChatCompletionChunk) error {
	payload, err := json.Marshal(chunk)
	if err != nil {
		return err
	}

	return sw.Send(sse.Event{Data: string(payload)})
}

func writeError(w http.ResponseWriter, status int, msg, param string) {
	body := ErrorResponse{Error: APIError{Message: msg, Type: "invalid_request_error"}}
	if param != "" {
		body.Error.Param = &param
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

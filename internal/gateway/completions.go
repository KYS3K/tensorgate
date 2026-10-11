package gateway

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/KYS3K/tensorgate/internal/sse"
)

const (
	defaultModel = "tensorgate-mock-v0"
	// so a client can't make us buffer arbitrary amounts of JSON
	maxBodyBytes = 1 << 20
	// we don't tokenize the prompt so usage reports a fixed count
	mockPromptTokens = 10
)

// CompletionsHandler serves an OpenAI-compatible /v1/chat/completions endpoint with mock output
type CompletionsHandler struct {
	DefaultConfig MockConfig
}

func NewCompletionsHandler() *CompletionsHandler {
	return &CompletionsHandler{
		DefaultConfig: DefaultMockConfig(),
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

	cfg, err := h.resolveConfig(r)
	if err != nil {
		param := ""
		if herr, ok := errors.AsType[*HeaderError](err); ok {
			param = herr.Header
		}
		writeError(w, http.StatusBadRequest, err.Error(), param)
		return
	}
	c := completion{id: "chatcmpl-" + rand.Text(), created: time.Now().Unix(), model: req.Model}
	if req.Stream {
		h.handleStream(w, r, cfg, c)
		return
	}
	h.handleNonStream(w, r, cfg, c)
}

func mockToken(i int) string {
	return fmt.Sprintf(" token_%d", i)
}

func sleep(r *http.Request, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-r.Context().Done():
		return false
	case <-t.C:
		return true
	}
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

func (h *CompletionsHandler) handleNonStream(w http.ResponseWriter, r *http.Request, cfg MockConfig, c completion) {
	if !sleep(r, cfg.TTFT+time.Duration(cfg.Tokens-1)*cfg.TPOT) {
		return
	}

	var content strings.Builder
	for i := 1; i <= cfg.Tokens; i++ {
		content.WriteString(mockToken(i))
	}

	resp := ChatCompletionResponse{
		ID:      c.id,
		Object:  "chat.completion",
		Created: c.created,
		Model:   c.model,
		Choices: []Choice{{
			Index:        0,
			Message:      ChatMessage{Role: "assistant", Content: content.String()},
			FinishReason: "stop",
		}},
		Usage: Usage{
			PromptTokens:     mockPromptTokens,
			CompletionTokens: cfg.Tokens,
			TotalTokens:      mockPromptTokens + cfg.Tokens,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *CompletionsHandler) handleStream(w http.ResponseWriter, r *http.Request, cfg MockConfig, c completion) {
	sw := sse.NewWriter(w)

	if !sleep(r, cfg.TTFT) {
		return
	}

	// OpenAI sends the role alone in the first chunk, before any content
	if err := sendChunk(sw, c.chunk(ChunkDelta{Role: "assistant"}, nil)); err != nil {
		return
	}

	ticker := time.NewTicker(cfg.TPOT)
	defer ticker.Stop()

	for i := 1; i <= cfg.Tokens; i++ {
		// the first token goes out right after TTFT; TPOT is the gap between tokens
		if i > 1 {
			select {
			case <-r.Context().Done():
				// client ended session or server is down
				return
			case <-ticker.C:
			}
		}

		if err := sendChunk(sw, c.chunk(ChunkDelta{Content: mockToken(i)}, nil)); err != nil {
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

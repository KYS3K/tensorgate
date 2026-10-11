package gateway

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/KYS3K/tensorgate/internal/metrics"
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
	metrics       *metrics.Metrics
}

func NewCompletionsHandler(m *metrics.Metrics) *CompletionsHandler {
	return &CompletionsHandler{
		DefaultConfig: DefaultMockConfig(),
		metrics:       m,
	}
}

func (h *CompletionsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	var req ChatCompletionRequest
	// each early return below is a rejected request
	outcome := metrics.OutcomeInvalidRequest
	defer func() {
		h.metrics.RequestDone(modelLabel(req.Model), req.Stream, outcome)
	}()

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
	c := completion{id: "chatcmpl-" + rand.Text(), start: start, model: req.Model}
	if req.Stream {
		outcome = h.handleStream(w, r, cfg, c)
		return
	}
	outcome = h.handleNonStream(w, r, cfg, c)
}

func modelLabel(model string) string {
	if model == defaultModel {
		return model
	}
	return "other"
}

func failed(r *http.Request) metrics.Outcome {
	if r.Context().Err() != nil {
		return metrics.OutcomeCanceled
	}
	return metrics.OutcomeWriteError
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
	id    string
	start time.Time // when the request arrived
	model string
}

func (c completion) chunk(delta ChunkDelta, finishReason *string) ChatCompletionChunk {
	return ChatCompletionChunk{
		ID:      c.id,
		Object:  "chat.completion.chunk",
		Created: c.start.Unix(),
		Model:   c.model,
		Choices: []ChunkChoice{{Index: 0, Delta: delta, FinishReason: finishReason}},
	}
}

func (h *CompletionsHandler) handleNonStream(w http.ResponseWriter, r *http.Request, cfg MockConfig, c completion) metrics.Outcome {
	if !sleep(r, cfg.TTFT+time.Duration(cfg.Tokens-1)*cfg.TPOT) {
		return metrics.OutcomeCanceled
	}

	var content strings.Builder
	for i := 1; i <= cfg.Tokens; i++ {
		content.WriteString(mockToken(i))
	}

	resp := ChatCompletionResponse{
		ID:      c.id,
		Object:  "chat.completion",
		Created: c.start.Unix(),
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
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		return failed(r)
	}
	return metrics.OutcomeCompleted
}

func (h *CompletionsHandler) handleStream(w http.ResponseWriter, r *http.Request, cfg MockConfig, c completion) metrics.Outcome {
	h.metrics.StreamStarted()
	defer h.metrics.StreamEnded()

	sw := sse.NewWriter(w)

	if !sleep(r, cfg.TTFT) {
		return metrics.OutcomeCanceled
	}

	// OpenAI sends the role alone in the first chunk, before any content
	if err := sendChunk(sw, c.chunk(ChunkDelta{Role: "assistant"}, nil)); err != nil {
		return failed(r)
	}

	ticker := time.NewTicker(cfg.TPOT)
	defer ticker.Stop()

	var lastToken time.Time
	for i := 1; i <= cfg.Tokens; i++ {
		// the first token goes out right after TTFT; TPOT is the gap between tokens
		if i > 1 {
			select {
			case <-r.Context().Done():
				// client ended session or server is down
				return metrics.OutcomeCanceled
			case <-ticker.C:
			}
		}

		if err := sendChunk(sw, c.chunk(ChunkDelta{Content: mockToken(i)}, nil)); err != nil {
			return failed(r)
		}

		// measured after the send, i.e. when the client could see the token
		now := time.Now()
		if i == 1 {
			h.metrics.ObserveTTFT(now.Sub(c.start))
		} else {
			h.metrics.ObserveTPOT(now.Sub(lastToken))
		}
		lastToken = now
	}

	stop := "stop"
	if err := sendChunk(sw, c.chunk(ChunkDelta{}, &stop)); err != nil {
		return failed(r)
	}

	// signal marker
	if err := sw.Send(sse.Event{Data: "[DONE]"}); err != nil {
		return failed(r)
	}
	return metrics.OutcomeCompleted
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

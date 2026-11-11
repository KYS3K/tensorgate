package gateway

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
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
	// FlushInterval batches the tokens of a stream into at most one write per interval
	// (see sse.Coalescer), so no token waits longer than that. 0 writes every token at once.
	FlushInterval time.Duration
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
	return string(appendMockToken(nil, i))
}

// appendMockToken is mockToken for the streaming hot path, without allocating
func appendMockToken(b []byte, i int) []byte {
	b = append(b, " token_"...)
	return strconv.AppendInt(b, int64(i), 10)
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

var doneData = []byte("[DONE]")

func (h *CompletionsHandler) handleStream(w http.ResponseWriter, r *http.Request, cfg MockConfig, c completion) metrics.Outcome {
	h.metrics.StreamStarted()
	defer h.metrics.StreamEnded()

	// chunks are encoded into per-stream buffers, so sending a token doesn't allocate
	enc := newChunkEncoder(c)
	var tok []byte

	out := sse.NewCoalescer(sse.NewWriter(w), h.FlushInterval)
	observe := func(flushed int, err error) error {
		if flushed > 0 {
			h.metrics.ObserveFlush(flushed)
		}
		return err
	}

	if !sleep(r, cfg.TTFT) {
		return metrics.OutcomeCanceled
	}

	// OpenAI sends the role alone in the first chunk, before any content;
	// it leaves together with the first token
	if err := observe(out.Add(enc.role("assistant"), true)); err != nil {
		return failed(r)
	}

	// Tokens follow a fixed schedule, like a real backend that keeps generating while the
	// gateway falls behind: a late wakeup sends every token due by now as one burst.
	// The ticker only wakes us up; which tokens are due comes from the clock.
	start := time.Now()
	ticker := time.NewTicker(cfg.TPOT)
	defer ticker.Stop()

	sent := 0
	var lastToken time.Time
	for {
		due := min(cfg.Tokens, 1+int(time.Since(start)/cfg.TPOT))
		for sent < due {
			sent++
			tok = appendMockToken(tok[:0], sent)
			if err := observe(out.Add(enc.content(tok), sent < due)); err != nil {
				return failed(r)
			}

			now := time.Now()
			if sent == 1 {
				// the first token is never held back, so this is when the client could see it
				h.metrics.ObserveTTFT(now.Sub(c.start))
			} else {
				h.metrics.ObserveTPOT(now.Sub(lastToken))
			}
			lastToken = now
		}
		if sent == cfg.Tokens {
			break
		}

		select {
		case <-r.Context().Done():
			// client ended session or server is down
			return metrics.OutcomeCanceled
		case <-ticker.C:
		case <-out.C():
			if err := observe(out.Flush()); err != nil {
				return failed(r)
			}
		}
	}

	// the end of a stream is never held back
	if err := observe(out.Add(enc.finish("stop"), true)); err != nil {
		return failed(r)
	}
	if err := observe(out.Add(doneData, true)); err != nil {
		return failed(r)
	}
	if err := observe(out.Flush()); err != nil {
		return failed(r)
	}
	return metrics.OutcomeCompleted
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

package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KYS3K/tensorgate/internal/sse"
)

const hiMessages = `"messages":[{"role":"user","content":"hi"}]`

func testCompletions() *CompletionsHandler {
	return &CompletionsHandler{Tokens: 3, Interval: time.Millisecond}
}

func post(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// decodeMap checks the wire format with generic maps, so a typo in a struct tag can't hide itself
func decodeMap(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("invalid JSON %q: %v", s, err)
	}
	return m
}

func firstChoice(t *testing.T, m map[string]any) map[string]any {
	t.Helper()
	choices, ok := m["choices"].([]any)
	if !ok || len(choices) != 1 {
		t.Fatalf("choices = %v, want exactly one", m["choices"])
	}
	return choices[0].(map[string]any)
}

func TestCompletionsNonStream(t *testing.T) {
	rec := post(t, testCompletions(), `{`+hiMessages+`}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}

	m := decodeMap(t, rec.Body.String())
	if m["object"] != "chat.completion" {
		t.Errorf("object = %v", m["object"])
	}
	if m["model"] != defaultModel {
		t.Errorf("model = %v, want default %q", m["model"], defaultModel)
	}
	if id, _ := m["id"].(string); !strings.HasPrefix(id, "chatcmpl-") {
		t.Errorf("id = %v", m["id"])
	}
	if _, ok := m["usage"].(map[string]any); !ok {
		t.Errorf("usage missing")
	}

	choice := firstChoice(t, m)
	if choice["finish_reason"] != "stop" {
		t.Errorf("finish_reason = %v", choice["finish_reason"])
	}
	msg, ok := choice["message"].(map[string]any)
	if !ok {
		t.Fatalf(`choice has no "message" key: %v`, choice)
	}
	if msg["role"] != "assistant" || msg["content"] == "" {
		t.Errorf("message = %v", msg)
	}
}

func TestCompletionsKeepsModel(t *testing.T) {
	rec := post(t, testCompletions(), `{"model":"gpt-x",`+hiMessages+`}`)
	if m := decodeMap(t, rec.Body.String()); m["model"] != "gpt-x" {
		t.Errorf("model = %v, want gpt-x", m["model"])
	}
}

func TestCompletionsStream(t *testing.T) {
	rec := post(t, testCompletions(), `{"stream":true,`+hiMessages+`}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}

	var data []string
	if err := sse.ReadEvents(strings.NewReader(rec.Body.String()), func(_, d string) {
		data = append(data, d)
	}); err != nil {
		t.Fatal(err)
	}

	// role chunk + 3 content chunks + finish chunk + [DONE]
	if len(data) != 6 {
		t.Fatalf("got %d events, want 6: %q", len(data), data)
	}
	if data[5] != "[DONE]" {
		t.Errorf("last event = %q, want [DONE]", data[5])
	}

	var ids []any
	var content []string
	for i, d := range data[:5] {
		m := decodeMap(t, d)
		ids = append(ids, m["id"])
		if m["object"] != "chat.completion.chunk" {
			t.Errorf("chunk %d object = %v", i, m["object"])
		}

		choice := firstChoice(t, m)
		delta := choice["delta"].(map[string]any)
		finish, present := choice["finish_reason"]
		if !present {
			t.Errorf("chunk %d: finish_reason must be present (null while streaming)", i)
		}

		switch i {
		case 0:
			if !reflect.DeepEqual(delta, map[string]any{"role": "assistant"}) {
				t.Errorf("first delta = %v, want only the role", delta)
			}
		case 4:
			if len(delta) != 0 || finish != "stop" {
				t.Errorf("final chunk delta = %v finish = %v, want {} and stop", delta, finish)
			}
		default:
			if finish != nil {
				t.Errorf("chunk %d finish_reason = %v, want null", i, finish)
			}
			s, _ := delta["content"].(string)
			content = append(content, s)
		}
	}

	if want := []string{" token_1", " token_2", " token_3"}; !reflect.DeepEqual(content, want) {
		t.Errorf("content = %q, want %q", content, want)
	}
	for _, id := range ids {
		if id != ids[0] {
			t.Errorf("chunk ids differ: %v", ids)
			break
		}
	}
}

func TestCompletionsStreamClientGone(t *testing.T) {
	h := testCompletions()
	h.Interval = time.Hour // make sure the context wins the select

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"stream":true,`+hiMessages+`}`))
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not return after client disconnected")
	}
	if strings.Contains(rec.Body.String(), "[DONE]") {
		t.Errorf("stream must not finish after disconnect, got %q", rec.Body.String())
	}
}

func TestCompletionsContentParts(t *testing.T) {
	// OpenAI allows content as an array of parts, or null for assistant tool calls
	body := `{"messages":[
		{"role":"user","content":[{"type":"text","text":"hi"}]},
		{"role":"assistant","content":null},
		{"role":"user","content":"again"}]}`
	if rec := post(t, testCompletions(), body); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
}

func TestCompletionsErrors(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantParam  any
	}{
		{"invalid JSON", `{bad`, http.StatusBadRequest, nil},
		{"empty body", ``, http.StatusBadRequest, nil},
		{"no messages", `{"model":"m"}`, http.StatusBadRequest, "messages"},
		{"empty messages", `{"messages":[]}`, http.StatusBadRequest, "messages"},
		{"missing role", `{"messages":[{"content":"hi"}]}`, http.StatusBadRequest, "messages[0].role"},
		{"wrong type", `{"messages":"hi"}`, http.StatusBadRequest, nil},
		{"too large", `{"messages":[{"role":"user","content":"` + strings.Repeat("x", maxBodyBytes) + `"}]}`,
			http.StatusRequestEntityTooLarge, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := post(t, testCompletions(), tt.body)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q", ct)
			}

			// exactly one JSON object: nothing may be written after the error
			m := decodeMap(t, rec.Body.String())
			e, ok := m["error"].(map[string]any)
			if !ok {
				t.Fatalf(`body has no "error" object: %v`, m)
			}
			if msg, _ := e["message"].(string); msg == "" {
				t.Errorf("error message is empty")
			}
			if e["type"] != "invalid_request_error" {
				t.Errorf("type = %v", e["type"])
			}
			if e["param"] != tt.wantParam {
				t.Errorf("param = %v, want %v", e["param"], tt.wantParam)
			}
		})
	}
}

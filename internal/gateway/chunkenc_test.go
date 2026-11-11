package gateway

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/KYS3K/tensorgate/internal/sse"
)

// marshalChunk is the reference: what the encoder replaced
func marshalChunk(t testing.TB, c completion, delta ChunkDelta, finishReason *string) []byte {
	t.Helper()
	b, err := json.Marshal(ChatCompletionChunk{
		ID:      c.id,
		Object:  "chat.completion.chunk",
		Created: c.start.Unix(),
		Model:   c.model,
		Choices: []ChunkChoice{{Index: 0, Delta: delta, FinishReason: finishReason}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func checkSame(t *testing.T, what string, got, want []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Errorf("%s:\n got %s\nwant %s", what, got, want)
	}
}

func FuzzChunkEncoder(f *testing.F) {
	for _, s := range []string{
		"", " token_1", "hello", "привет, мир", "日本語", "emoji 🙂",
		`quote " backslash \ slash /`, "<script>&amp;</script>",
		"\x00\x01\x1f\b\f\n\r\t\x7f", "line\u2028sep\u2029par",
		"bad utf8 \xff\xfe", "truncated \xe2\x82", "\xed\xa0\x80 surrogate",
	} {
		f.Add(s, "chatcmpl-abc", "tensorgate-mock-v0", int64(1_700_000_000))
	}
	f.Fuzz(func(t *testing.T, text, id, model string, created int64) {
		c := completion{id: id, model: model, start: time.Unix(created, 0)}
		enc := newChunkEncoder(c)

		checkSame(t, "role", enc.role(text), marshalChunk(t, c, ChunkDelta{Role: text}, nil))
		checkSame(t, "content", enc.content([]byte(text)), marshalChunk(t, c, ChunkDelta{Content: text}, nil))
		checkSame(t, "finish", enc.finish(text), marshalChunk(t, c, ChunkDelta{}, &text))
	})
}

// a reused encoder must not leak bytes of a longer previous chunk into a shorter one
func TestChunkEncoderReuse(t *testing.T) {
	c := completion{id: "chatcmpl-x", model: defaultModel, start: time.Unix(1, 0)}
	enc := newChunkEncoder(c)
	enc.content(bytes.Repeat([]byte("long "), 100))
	checkSame(t, "short after long", enc.content([]byte("a")), marshalChunk(t, c, ChunkDelta{Content: "a"}, nil))
}

func TestAppendMockToken(t *testing.T) {
	for _, i := range []int{1, 9, 10, 12345} {
		if got, want := string(appendMockToken([]byte("junk"), i)[4:]), mockToken(i); got != want {
			t.Errorf("appendMockToken(%d) = %q, want %q", i, got, want)
		}
	}
}

// discardWriter is an http.ResponseWriter + http.Flusher that drops everything,
// so benchmarks measure encoding, not a recorder's growing buffer
type discardWriter struct{ h http.Header }

func (d *discardWriter) Header() http.Header         { return d.h }
func (d *discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (d *discardWriter) WriteHeader(int)             {}
func (d *discardWriter) Flush()                      {}

var benchCompletion = completion{id: "chatcmpl-0123456789abcdef", model: defaultModel, start: time.Unix(1_700_000_000, 0)}

// BenchmarkSendTokenMarshal is the old per-token path: Sprintf + json.Marshal + string(payload) + Send
func BenchmarkSendTokenMarshal(b *testing.B) {
	sw := sse.NewWriter(&discardWriter{h: http.Header{}})
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		payload := marshalChunk(b, benchCompletion, ChunkDelta{Content: mockToken(i)}, nil)
		if err := sw.Send(sse.Event{Data: string(payload)}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSendTokenEncoder is the current per-token path in handleStream
func BenchmarkSendTokenEncoder(b *testing.B) {
	sw := sse.NewWriter(&discardWriter{h: http.Header{}})
	enc := newChunkEncoder(benchCompletion)
	var tok []byte
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		tok = appendMockToken(tok[:0], i)
		if err := sw.SendData(enc.content(tok)); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSendTokenDoesNotAllocate(t *testing.T) {
	sw := sse.NewWriter(&discardWriter{h: http.Header{}})
	enc := newChunkEncoder(benchCompletion)
	var tok []byte
	i := 0
	allocs := testing.AllocsPerRun(1000, func() {
		i++
		tok = appendMockToken(tok[:0], i)
		_ = sw.SendData(enc.content(tok))
	})
	if allocs != 0 {
		t.Errorf("sending a token allocates %v times, want 0", allocs)
	}
}

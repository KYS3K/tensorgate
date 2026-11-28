package lb

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

const (
	testBlock  = 64
	testBlocks = 2
)

func chatBody(model string, msgs ...[2]string) []byte {
	type msg struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	req := struct {
		Model    string `json:"model"`
		Messages []msg  `json:"messages"`
		Stream   bool   `json:"stream"`
	}{Model: model, Stream: true}
	for _, m := range msgs {
		req.Messages = append(req.Messages, msg{m[0], m[1]})
	}
	b, _ := json.Marshal(req)
	return b
}

func key(t *testing.T, body []byte) uint64 {
	t.Helper()
	k, ok := prefixKey(body, testBlock, testBlocks)
	if !ok {
		t.Fatalf("no key for %s", body)
	}
	return k
}

var longSystem = strings.Repeat("You are a careful assistant. ", 20) // 580 bytes, many blocks

func TestPrefixKey_SameStartDifferentTailGivesTheSameKey(t *testing.T) {
	a := key(t, chatBody("m", [2]string{"system", longSystem}, [2]string{"user", "what is 2+2?"}))
	b := key(t, chatBody("m", [2]string{"system", longSystem}, [2]string{"user", "tell me a joke"}))
	if a != b {
		t.Fatal("two requests with one system prompt got different keys")
	}
}

func TestPrefixKey_DifferentStartGivesADifferentKey(t *testing.T) {
	a := key(t, chatBody("m", [2]string{"system", longSystem}))
	b := key(t, chatBody("m", [2]string{"system", "Answer in French. " + longSystem}))
	if a == b {
		t.Fatal("different system prompts got the same key")
	}
}

func TestPrefixKey_OnlyTheFirstBlocksCount(t *testing.T) {
	head := strings.Repeat("x", testBlock*testBlocks)
	a := key(t, chatBody("m", [2]string{"user", head + "tail one"}))
	b := key(t, chatBody("m", [2]string{"user", head + "another tail"}))
	// "user\n" takes the first 5 bytes, so what is counted is those 5 and the head's start:
	// everything after the first testBlock*testBlocks bytes of the prompt is ignored
	if a != b {
		t.Fatal("the tail after the last counted block changed the key")
	}
	c := key(t, chatBody("m", [2]string{"user", "y" + head[1:] + "tail"}))
	if a == c {
		t.Fatal("a change inside the counted blocks did not change the key")
	}
}

func TestPrefixKey_ModelIsPartOfTheKey(t *testing.T) {
	a := key(t, chatBody("model-a", [2]string{"system", longSystem}))
	b := key(t, chatBody("model-b", [2]string{"system", longSystem}))
	if a == b {
		t.Fatal("two models share a key, but not a KV cache")
	}
}

func TestPrefixKey_JSONLayoutDoesNotMatter(t *testing.T) {
	a := []byte(`{"model":"m","stream":true,"messages":[{"role":"system","content":` + jsonString(longSystem) + `}]}`)
	b := []byte(`{
		"messages": [ { "content": ` + jsonString(longSystem) + `, "role": "system" } ],
		"temperature": 0.7,
		"model": "m"
	}`)
	if key(t, a) != key(t, b) {
		t.Fatal("key depends on field order, spaces or other fields")
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestPrefixKey_ContentPartsEqualPlainString(t *testing.T) {
	half := len(longSystem) / 2
	plain := []byte(`{"model":"m","messages":[{"role":"system","content":` + jsonString(longSystem) + `}]}`)
	parts := []byte(fmt.Sprintf(
		`{"model":"m","messages":[{"role":"system","content":[{"type":"text","text":%s},{"type":"text","text":%s}]}]}`,
		jsonString(longSystem[:half]), jsonString(longSystem[half:])))
	if key(t, plain) != key(t, parts) {
		t.Fatal("the same text as a list of parts got another key")
	}
}

func TestPrefixKey_NoKeyWhenThereIsNothingToRouteBy(t *testing.T) {
	short := chatBody("m", [2]string{"user", "hi"})
	bodies := map[string][]byte{
		"shorter than one block": short,
		"not json":               []byte("hello"),
		"empty":                  nil,
		"no messages":            []byte(`{"model":"m"}`),
		"messages is not a list": []byte(`{"model":"m","messages":"hello"}`),
		"null content":           []byte(`{"model":"m","messages":[{"role":"user","content":null}]}`),
	}
	for name, body := range bodies {
		if k, ok := prefixKey(body, testBlock, testBlocks); ok {
			t.Errorf("%s: got key %d, want none", name, k)
		}
	}
}

func TestPrefixKey_ChainMakesPositionMatter(t *testing.T) {
	a := strings.Repeat("A", testBlock)
	b := strings.Repeat("B", testBlock)
	// both blocks are the same in the two requests, only their order differs
	ab := key(t, chatBody("m", [2]string{"user", a + b}))
	ba := key(t, chatBody("m", [2]string{"user", b + a}))
	if ab == ba {
		t.Fatal("the order of blocks does not change the key")
	}
}

func BenchmarkPrefixKey(b *testing.B) {
	for _, size := range []int{1 << 10, 32 << 10, 512 << 10} {
		body := chatBody("m", [2]string{"system", strings.Repeat("x", size)}, [2]string{"user", "question"})
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			for b.Loop() {
				prefixKey(body, 256, 2)
			}
		})
	}
}

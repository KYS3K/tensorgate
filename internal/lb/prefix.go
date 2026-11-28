package lb

import (
	"bytes"
	"encoding/json"

	"github.com/KYS3K/tensorgate/internal/hashring"
)

// prefixKey is the routing key of a chat request: the hash of the first `blocks` blocks of its prompt,
// so requests that start the same way get the same key and meet on the same upstream, where
// the KV cache of that start already lives.
//
// The prompt is the messages written out as role, newline, content, newline, and cut into blocks of
// blockBytes. Only full blocks count, an inference server caches only full blocks too, and a prompt
// shorter than one block has nothing worth routing by: the second result is false and the caller
// falls back to least-outstanding. The hash of a block includes the hash of every block before it,
// and the model name seeds the chain, since another model has another cache.
//
// A prompt that is shorter than `blocks` full blocks is keyed by the blocks it has, so two prompts
// that share one block but differ in length may get different keys. A bigger `blocks` means a longer
// shared start is needed to meet on one upstream, and a better spread of requests that share a system prompt.
func prefixKey(body []byte, blockBytes, blocks int) (uint64, bool) {
	var req struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal(body, &req) != nil {
		return 0, false
	}

	need := blockBytes * blocks
	text := make([]byte, 0, need+blockBytes)
	for _, m := range req.Messages {
		text = append(text, m.Role...)
		text = append(text, '\n')
		text = appendContent(text, m.Content)
		text = append(text, '\n')
		if len(text) >= need {
			break
		}
	}
	text = text[:min(len(text), need)]

	full := len(text) / blockBytes
	if full == 0 {
		return 0, false
	}
	h := hashring.HashString(req.Model)
	for i := range full {
		h = hashring.Chain(h, text[i*blockBytes:(i+1)*blockBytes])
	}
	return h, true
}

// appendContent adds the text of a message content to dst: a string as it is, a list of parts
// as the texts of its parts glued together. Anything else (null, a number) adds nothing.
func appendContent(dst []byte, raw json.RawMessage) []byte {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return dst
	}
	switch raw[0] {
	case '"':
		var s string
		if json.Unmarshal(raw, &s) == nil {
			dst = append(dst, s...)
		}
	case '[':
		var parts []struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(raw, &parts) == nil {
			for _, p := range parts {
				dst = append(dst, p.Text...)
			}
		}
	}
	return dst
}

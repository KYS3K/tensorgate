package gateway

import (
	"strconv"
	"unicode/utf8"
)

// chunkEncoder builds the chat.completion.chunk JSON of one stream by appending into a reused buffer.
// Everything before "delta" is the same in every chunk of a stream, so it is encoded once;
// a token chunk then costs one escape of its content and no allocations.
//
// The output is byte-for-byte what json.Marshal produces for the equivalent ChatCompletionChunk
// (FuzzChunkEncoder checks it), so clients can't tell the two apart.
type chunkEncoder struct {
	prefix []byte // {"id":…,"object":"chat.completion.chunk","created":…,"model":…,"choices":[{"index":0,"delta":
	buf    []byte
}

func newChunkEncoder(c completion) *chunkEncoder {
	p := make([]byte, 0, 128)
	p = append(p, `{"id":`...)
	p = appendJSONString(p, []byte(c.id))
	p = append(p, `,"object":"chat.completion.chunk","created":`...)
	p = strconv.AppendInt(p, c.start.Unix(), 10)
	p = append(p, `,"model":`...)
	p = appendJSONString(p, []byte(c.model))
	p = append(p, `,"choices":[{"index":0,"delta":`...)
	return &chunkEncoder{prefix: p, buf: make([]byte, 0, 2*len(p))}
}

// The returned slices alias the encoder's buffer and stay valid only until the next call.

// role encodes the first chunk of a stream, which carries only the role
func (e *chunkEncoder) role(role string) []byte {
	b := append(e.buf[:0], e.prefix...)
	if role == "" {
		b = append(b, `{}`...) // omitempty
	} else {
		b = append(b, `{"role":`...)
		b = appendJSONString(b, []byte(role))
		b = append(b, '}')
	}
	b = append(b, `,"finish_reason":null}]}`...)
	e.buf = b
	return b
}

// content encodes a chunk carrying one piece of generated text
func (e *chunkEncoder) content(text []byte) []byte {
	b := append(e.buf[:0], e.prefix...)
	if len(text) == 0 {
		b = append(b, `{}`...) // omitempty
	} else {
		b = append(b, `{"content":`...)
		b = appendJSONString(b, text)
		b = append(b, '}')
	}
	b = append(b, `,"finish_reason":null}]}`...)
	e.buf = b
	return b
}

// finish encodes the last chunk: an empty delta and the finish reason
func (e *chunkEncoder) finish(reason string) []byte {
	b := append(e.buf[:0], e.prefix...)
	b = append(b, `{},"finish_reason":`...)
	b = appendJSONString(b, []byte(reason))
	b = append(b, `}]}`...)
	e.buf = b
	return b
}

const hexDigits = "0123456789abcdef"

// appendJSONString appends s as a JSON string escaped the way encoding/json does by default:
// HTML-sensitive <, >, & and U+2028/U+2029 are \u-escaped and each invalid byte becomes U+FFFD.
func appendJSONString(b, s []byte) []byte {
	b = append(b, '"')
	start := 0 // s[start:i] is pending, copied as is
	for i := 0; i < len(s); {
		if c := s[i]; c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' && c != '<' && c != '>' && c != '&' {
				i++
				continue
			}
			b = append(b, s[start:i]...)
			switch c {
			case '"', '\\':
				b = append(b, '\\', c)
			case '\b':
				b = append(b, '\\', 'b')
			case '\f':
				b = append(b, '\\', 'f')
			case '\n':
				b = append(b, '\\', 'n')
			case '\r':
				b = append(b, '\\', 'r')
			case '\t':
				b = append(b, '\\', 't')
			default:
				b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			}
			i++
			start = i
			continue
		}

		r, size := utf8.DecodeRune(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			b = append(b, s[start:i]...)
			b = append(b, "\ufffd"...) // the UTF-8 bytes of U+FFFD, not a \u escape, as encoding/json does
		case r == '\u2028' || r == '\u2029':
			b = append(b, s[start:i]...)
			b = append(b, '\\', 'u', '2', '0', '2', hexDigits[r&0xf])
		default:
			i += size
			continue
		}
		i += size
		start = i
	}
	b = append(b, s[start:]...)
	return append(b, '"')
}

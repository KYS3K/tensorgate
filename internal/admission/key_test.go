package admission

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestKeyFromRequest(t *testing.T) {
	maxKey := strings.Repeat("k", maxKeyLength)
	tooLong := strings.Repeat("k", maxKeyLength+1)

	tests := []struct {
		name    string
		headers map[string]string
		wantKey string
		wantOK  bool
	}{
		{
			name:    "no headers",
			headers: nil,
			wantKey: "",
			wantOK:  false,
		},
		{
			name:    "api key header",
			headers: map[string]string{"X-Api-Key": "abc"},
			wantKey: "abc",
			wantOK:  true,
		},
		{
			name:    "api key header, spaces trimmed",
			headers: map[string]string{"X-Api-Key": "  abc  "},
			wantKey: "abc",
			wantOK:  true,
		},
		{
			name:    "api key header, only spaces",
			headers: map[string]string{"X-Api-Key": "   "},
			wantKey: "",
			wantOK:  false,
		},
		{
			name:    "bearer",
			headers: map[string]string{"Authorization": "Bearer abc"},
			wantKey: "abc",
			wantOK:  true,
		},
		{
			name:    "bearer, lowercase scheme",
			headers: map[string]string{"Authorization": "bearer abc"},
			wantKey: "abc",
			wantOK:  true,
		},
		{
			name:    "bearer, uppercase scheme",
			headers: map[string]string{"Authorization": "BEARER abc"},
			wantKey: "abc",
			wantOK:  true,
		},
		{
			name:    "bearer, extra spaces around token",
			headers: map[string]string{"Authorization": "Bearer   abc  "},
			wantKey: "abc",
			wantOK:  true,
		},
		{
			name:    "bearer, empty token",
			headers: map[string]string{"Authorization": "Bearer "},
			wantKey: "",
			wantOK:  false,
		},
		{
			name:    "bearer, scheme only, no space",
			headers: map[string]string{"Authorization": "Bearer"},
			wantKey: "",
			wantOK:  false,
		},
		{
			name:    "bearer, prefix glued to token",
			headers: map[string]string{"Authorization": "BearerXYZ"},
			wantKey: "",
			wantOK:  false,
		},
		{
			name:    "other scheme",
			headers: map[string]string{"Authorization": "Basic dXNlcjpwYXNz"},
			wantKey: "",
			wantOK:  false,
		},
		{
			name: "both headers, api key wins",
			headers: map[string]string{
				"X-Api-Key":     "from-api-key",
				"Authorization": "Bearer from-bearer",
			},
			wantKey: "from-api-key",
			wantOK:  true,
		},
		{
			name: "api key with other scheme in Authorization",
			headers: map[string]string{
				"X-Api-Key":     "good",
				"Authorization": "Basic dXNlcjpwYXNz",
			},
			wantKey: "good",
			wantOK:  true,
		},
		{
			name:    "api key of max length",
			headers: map[string]string{"X-Api-Key": maxKey},
			wantKey: maxKey,
			wantOK:  true,
		},
		{
			name:    "bearer of max length",
			headers: map[string]string{"Authorization": "Bearer " + maxKey},
			wantKey: maxKey,
			wantOK:  true,
		},
		{
			name:    "api key too long",
			headers: map[string]string{"X-Api-Key": tooLong},
			wantKey: "",
			wantOK:  false,
		},
		{
			name:    "bearer too long",
			headers: map[string]string{"Authorization": "Bearer " + tooLong},
			wantKey: "",
			wantOK:  false,
		},
		{
			// an invalid X-Api-Key must not silently fall back to the bearer token
			name: "api key too long, valid bearer",
			headers: map[string]string{
				"X-Api-Key":     tooLong,
				"Authorization": "Bearer abc",
			},
			wantKey: "",
			wantOK:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			for k, v := range tt.headers {
				r.Header.Set(k, v)
			}

			key, ok := KeyFromRequest(r)
			if key != tt.wantKey || ok != tt.wantOK {
				t.Errorf("KeyFromRequest() = (%q, %v), want (%q, %v)", key, ok, tt.wantKey, tt.wantOK)
			}
		})
	}
}

// the request path is hot: extracting the key must not allocate
func BenchmarkKeyFromRequest(b *testing.B) {
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	r.Header.Set("Authorization", "Bearer sk-test-key-123")

	b.ReportAllocs()
	for b.Loop() {
		KeyFromRequest(r)
	}
}

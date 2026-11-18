package admission

import (
	"net/http"
	"strings"
)

const (
	apiKeyHeader = "X-Api-Key"
	authHeader   = "Authorization"
	maxKeyLength = 128
	bearerPrefix = "Bearer "
)

func KeyFromRequest(r *http.Request) (key string, ok bool) {
	authKey := r.Header.Get(apiKeyHeader)
	token := r.Header.Get(authHeader)
	authKey = strings.TrimSpace(authKey)

	if len(authKey) <= maxKeyLength && len(authKey) > 0 {
		return authKey, true
	} else if len(authKey) > maxKeyLength {
		return "", false
	}

	if token != "" && len(token) >= len(bearerPrefix) && strings.EqualFold(token[:len(bearerPrefix)], bearerPrefix) {
		token = strings.TrimSpace(token[len(bearerPrefix):])
		if len(token) <= maxKeyLength && len(token) > 0 {
			return token, true
		}
	}

	return "", false
}

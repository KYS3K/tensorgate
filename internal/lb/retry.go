package lb

import (
	"context"
	"errors"
	"net"
	"net/http"
)

var errRetry = errors.New("lb: retry on another backend")

// attempt is per-try state the proxy callbacks share with the handler via the request context.
// The budget is taken in the callbacks, at the failure itself: a retry it refuses lets
// the upstream's own answer through instead of a made-up one.
type attempt struct {
	eligible bool
	retry    bool
	denied   bool
}

type attemptKey struct{}

func withAttempt(ctx context.Context, a *attempt) context.Context {
	return context.WithValue(ctx, attemptKey{}, a)
}

func takeRetry(ctx context.Context, budget *retryBudget) bool {
	a, _ := ctx.Value(attemptKey{}).(*attempt)
	if a == nil || !a.eligible {
		return false
	}
	if !budget.withdraw() {
		a.denied = true
		return false
	}
	a.retry = true
	return true
}

// shedding is an upstream refusing load on purpose: it goes to the client as is.
func shedding(code int, h http.Header) bool {
	return code == http.StatusServiceUnavailable && h.Get("Retry-After") != ""
}

// failedBeforeClient is what counts against a backend: 502, 504 and a bare 503.
func failedBeforeClient(code int, h http.Header) bool {
	switch code {
	case http.StatusBadGateway, http.StatusGatewayTimeout:
		return true
	case http.StatusServiceUnavailable:
		return !shedding(code, h)
	}
	return false
}

// retryableStatus is the part of failedBeforeClient that is worth sending to
// another backend. A 504 is not: the upstream already waited for someone for a
// long time, a second try makes the client wait twice and loads a slow backend more.
func retryableStatus(code int, h http.Header) bool {
	return failedBeforeClient(code, h) && code != http.StatusGatewayTimeout
}

// retryableError tells whether a transport error leaves the request safe to
// send again. A failed dial always is: nothing was sent. Any other timeout is
// not: the backend may be generating already, and a repeat doubles the work and the wait.
func retryableError(err error) bool {
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return false
	}
	return true
}

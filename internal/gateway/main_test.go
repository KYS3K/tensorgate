package gateway

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if any test leaves a goroutine running,
// e.g. a stream handler that doesn't return when the client goes away
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

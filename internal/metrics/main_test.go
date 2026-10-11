package metrics

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if any test leaves a goroutine running,
// e.g. a collector that starts a background goroutine
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

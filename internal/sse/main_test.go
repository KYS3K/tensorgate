package sse

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if any test leaves a goroutine running,
// e.g. a Writer or ReadEvents that keeps running after the test returns
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

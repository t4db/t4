package t4

import (
	"os"
	"testing"

	"github.com/t4db/t4/internal/metrics"
)

// Metric collectors are nil until Register runs. Node.Open does this in
// production; tests that exercise helpers directly need it done up front.
func TestMain(m *testing.M) {
	metrics.Register(nil)
	os.Exit(m.Run())
}

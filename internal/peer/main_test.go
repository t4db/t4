package peer_test

import (
	"os"
	"testing"

	"github.com/t4db/t4/internal/metrics"
)

func TestMain(m *testing.M) {
	metrics.Register(nil)
	os.Exit(m.Run())
}

package cli

import (
	"runtime"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

// TestWarnLargeGOMAXPROCS pins the startup warning for a host without a CPU
// limit, and that an explicit GOMAXPROCS silences it.
func TestWarnLargeGOMAXPROCS(t *testing.T) {
	hook := logtest.NewGlobal()
	prev := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(prev)
	warned := func() bool {
		for _, e := range hook.AllEntries() {
			if e.Level == logrus.WarnLevel && strings.Contains(e.Message, "GOMAXPROCS is") {
				return true
			}
		}
		return false
	}

	runtime.GOMAXPROCS(8)
	t.Setenv("GOMAXPROCS", "")
	warnLargeGOMAXPROCS()
	if warned() {
		t.Error("warned at GOMAXPROCS=8")
	}

	runtime.GOMAXPROCS(100)
	warnLargeGOMAXPROCS()
	if !warned() {
		t.Error("no warning at GOMAXPROCS=100 without a GOMAXPROCS setting")
	}

	hook.Reset()
	t.Setenv("GOMAXPROCS", "100")
	warnLargeGOMAXPROCS()
	if warned() {
		t.Error("warned although GOMAXPROCS was set explicitly")
	}
}

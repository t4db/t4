package tla_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/pkg/object"
)

type electionScenario struct {
	Name  string                 `json:"name"`
	Steps []electionScenarioStep `json:"steps"`
}

type electionScenarioStep struct {
	Action       string `json:"action"`
	Node         string `json:"node"`
	Addr         string `json:"addr"`
	FloorTerm    uint64 `json:"floor_term"`
	CommittedRev int64  `json:"committed_rev"`

	ExpectWon    *bool               `json:"expect_won"`
	ExpectRecord *electionLockExpect `json:"expect_record"`
	ExpectLock   *electionLockExpect `json:"expect_lock"`
}

type electionLockExpect struct {
	NodeID              string  `json:"node_id"`
	Term                *uint64 `json:"term"`
	TermGreaterThan     *uint64 `json:"term_greater_than"`
	CommittedRev        *int64  `json:"committed_rev"`
	CommittedRevAtLeast *int64  `json:"committed_rev_at_least"`
}

func TestElectionScenarios(t *testing.T) {
	paths, err := filepath.Glob("election/scenarios/*.json")
	if err != nil {
		t.Fatalf("glob scenarios: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no election scenarios found")
	}

	for _, path := range paths {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read scenario: %v", err)
			}
			var sc electionScenario
			if err := json.Unmarshal(b, &sc); err != nil {
				t.Fatalf("decode scenario: %v", err)
			}
			runElectionScenario(t, sc)
		})
	}
}

func runElectionScenario(t *testing.T, sc electionScenario) {
	t.Helper()
	store := object.NewMem()
	locks := make(map[string]*election.Lock)

	lockFor := func(step electionScenarioStep) *election.Lock {
		if step.Node == "" {
			t.Fatalf("%s: step action %q missing node", sc.Name, step.Action)
		}
		if l := locks[step.Node]; l != nil {
			return l
		}
		addr := step.Addr
		if addr == "" {
			addr = step.Node
		}
		l := election.NewLock(store, step.Node, addr)
		locks[step.Node] = l
		return l
	}

	for i, step := range sc.Steps {
		label := step.Action
		if sc.Name != "" {
			label = sc.Name + ": " + label
		}
		var (
			rec *election.LockRecord
			won bool
			err error
		)

		switch step.Action {
		case "try_acquire":
			rec, won, err = lockFor(step).TryAcquire(context.Background(), step.FloorTerm, election.Fence{Rev: step.CommittedRev})
		case "takeover":
			rec, won, err = lockFor(step).TakeOver(context.Background(), step.FloorTerm, election.Fence{Rev: step.CommittedRev}, nil)
		case "read":
			rec, err = lockFor(step).Read(context.Background())
		default:
			t.Fatalf("%s: step %d: unknown action %q", label, i, step.Action)
		}
		if err != nil {
			t.Fatalf("%s: step %d: %v", label, i, err)
		}
		if step.ExpectWon != nil && won != *step.ExpectWon {
			t.Fatalf("%s: step %d: won=%v want %v", label, i, won, *step.ExpectWon)
		}
		if step.ExpectRecord != nil {
			assertElectionLock(t, label, i, "record", rec, *step.ExpectRecord)
		}
		if step.ExpectLock != nil {
			current, err := lockFor(step).Read(context.Background())
			if err != nil {
				t.Fatalf("%s: step %d: read lock: %v", label, i, err)
			}
			assertElectionLock(t, label, i, "lock", current, *step.ExpectLock)
		}
	}
}

func assertElectionLock(t *testing.T, label string, step int, subject string, rec *election.LockRecord, want electionLockExpect) {
	t.Helper()
	if rec == nil {
		t.Fatalf("%s: step %d: %s=nil, want %+v", label, step, subject, want)
	}
	if want.NodeID != "" && rec.NodeID != want.NodeID {
		t.Fatalf("%s: step %d: %s NodeID=%q want %q", label, step, subject, rec.NodeID, want.NodeID)
	}
	if want.Term != nil && rec.Term != *want.Term {
		t.Fatalf("%s: step %d: %s Term=%d want %d", label, step, subject, rec.Term, *want.Term)
	}
	if want.TermGreaterThan != nil && rec.Term <= *want.TermGreaterThan {
		t.Fatalf("%s: step %d: %s Term=%d want > %d", label, step, subject, rec.Term, *want.TermGreaterThan)
	}
	if want.CommittedRev != nil && rec.CommittedRev != *want.CommittedRev {
		t.Fatalf("%s: step %d: %s CommittedRev=%d want %d", label, step, subject, rec.CommittedRev, *want.CommittedRev)
	}
	if want.CommittedRevAtLeast != nil && rec.CommittedRev < *want.CommittedRevAtLeast {
		t.Fatalf("%s: step %d: %s CommittedRev=%d want >= %d", label, step, subject, rec.CommittedRev, *want.CommittedRevAtLeast)
	}
}

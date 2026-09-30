package store

import (
	"testing"

	"github.com/t4db/t4/internal/wal"
)

func putEntry(rev int64, term uint64) wal.Entry {
	return wal.Entry{Revision: rev, Term: term, Op: wal.OpCreate, Key: "k", Value: []byte("v")}
}

// Entries a leader streamed or wrote (Apply) advance the leader-known
// position; entries restored from object storage (Recover) do not, though both
// advance the applied position.
func TestLeaderKnownPositionAdvancesOnlyOnApply(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetNodeID("n1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply([]wal.Entry{putEntry(1, 1), putEntry(2, 1)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Recover([]wal.Entry{putEntry(3, 2)}); err != nil {
		t.Fatal(err)
	}
	if got, want := s.LeaderKnownPosition(), (Position{Term: 1, Seq: 2}); got != want {
		t.Fatalf("leader-known position = %+v, want %+v", got, want)
	}
	if got, want := s.LastPosition(), (Position{Term: 2, Seq: 3}); got != want {
		t.Fatalf("last position = %+v, want %+v", got, want)
	}

	// Both survive a reopen.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.SetNodeID("n1"); err != nil {
		t.Fatal(err)
	}
	if got, want := s.LeaderKnownPosition(), (Position{Term: 1, Seq: 2}); got != want {
		t.Fatalf("leader-known position after reopen = %+v, want %+v", got, want)
	}
	if got, want := s.LastPosition(), (Position{Term: 2, Seq: 3}); got != want {
		t.Fatalf("last position after reopen = %+v, want %+v", got, want)
	}
}

// The position is keyed by node ID: a store written by another node (as a
// restored checkpoint is) gives this node no leader-known position.
func TestLeaderKnownPositionIsPerNode(t *testing.T) {
	s, err := OpenMem()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.SetNodeID("leader"); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply([]wal.Entry{putEntry(1, 1)}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetNodeID("restorer"); err != nil {
		t.Fatal(err)
	}
	if got := s.LeaderKnownPosition(); got != (Position{}) {
		t.Fatalf("another node's position leaked: %+v", got)
	}

	// A restore writes the node's own position back explicitly.
	own := Position{Term: 1, Seq: 1}
	if err := s.SetLeaderKnownPosition(own); err != nil {
		t.Fatal(err)
	}
	if err := s.SetNodeID("restorer"); err != nil {
		t.Fatal(err)
	}
	if got := s.LeaderKnownPosition(); got != own {
		t.Fatalf("set position not persisted: %+v, want %+v", got, own)
	}
}

// Positions order by term, then sequence: a deposed leader's uncommitted
// suffix, however long, is behind the next leader's entries.
func TestPositionOrder(t *testing.T) {
	for _, c := range []struct {
		p, q Position
		less bool
	}{
		{Position{1, 5}, Position{1, 6}, true},
		{Position{1, 9}, Position{2, 1}, true},
		{Position{2, 1}, Position{1, 9}, false},
		{Position{1, 5}, Position{1, 5}, false},
	} {
		if got := c.p.Less(c.q); got != c.less {
			t.Errorf("%+v.Less(%+v) = %v, want %v", c.p, c.q, got, c.less)
		}
	}
}

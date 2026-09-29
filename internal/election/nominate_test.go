package election

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/t4db/t4/pkg/object"
)

var testRank = RankTimes{Window: 2 * time.Second, Stagger: 6 * time.Second, MaxAge: time.Minute}

func nomAt(id string, seq int64, at time.Time) Nomination {
	return Nomination{NodeID: id, Seq: seq, AtNano: at.UnixNano()}
}

// In the example that motivates ranking, F caught up to 150 while G holds 160:
// G goes first, and F only if G has not taken over within Stagger.
func TestRankPrefersMostUpToDate(t *testing.T) {
	t0 := time.Unix(1000, 0)
	rec := &LockRecord{NodeID: "L", Followers: []string{"F", "G", "L"}, Nominations: []Nomination{
		nomAt("F", 150, t0),
		nomAt("G", 160, t0.Add(time.Second)),
	}}
	// Every expected candidate has nominated: complete at G's nomination.
	at := t0.Add(time.Second)
	if !rec.MayTakeOverByRank("G", at, testRank) {
		t.Fatal("top nominee may not take over once every expected candidate nominated")
	}
	if rec.MayTakeOverByRank("F", at, testRank) {
		t.Fatal("second nominee may take over before the top one had its turn")
	}
	if !rec.MayTakeOverByRank("F", at.Add(testRank.Stagger), testRank) {
		t.Fatal("second nominee may not take over after the top one let its turn pass")
	}
	if rec.MayTakeOverByRank("L", at.Add(time.Hour), testRank) {
		t.Fatal("a node that did not nominate may take over")
	}
}

// An expected candidate that has not nominated holds the election open for
// Window after the first nomination, plus Skew.
func TestRankWaitsForExpectedCandidates(t *testing.T) {
	t0 := time.Unix(1000, 0)
	rank := testRank
	rank.Skew = 500 * time.Millisecond
	rec := &LockRecord{NodeID: "L", Followers: []string{"F", "G"}, Nominations: []Nomination{nomAt("F", 150, t0)}}
	if rec.MayTakeOverByRank("F", t0.Add(rank.Window), rank) {
		t.Fatal("took over before the window and skew passed while G had not nominated")
	}
	if !rec.MayTakeOverByRank("F", t0.Add(rank.Window+rank.Skew), rank) {
		t.Fatal("still waiting after the window for a candidate that never nominated")
	}

	// Without a follower list (a leader of an earlier release, or one no
	// follower ever connected to) the window alone completes the election.
	rec.Followers = nil
	if rec.MayTakeOverByRank("F", t0.Add(time.Second), rank) || !rec.MayTakeOverByRank("F", t0.Add(rank.Window+rank.Skew), rank) {
		t.Fatal("without followers the election should complete by the window")
	}
}

// Nominations older than MaxAge belong to an election nobody completed and
// must not outrank later ones.
func TestRankIgnoresStaleNominations(t *testing.T) {
	t0 := time.Unix(1000, 0)
	now := t0.Add(testRank.MaxAge + time.Second)
	rec := &LockRecord{NodeID: "L", Nominations: []Nomination{
		nomAt("dead", 999, t0),
		nomAt("F", 150, now.Add(-testRank.Window)),
	}}
	if rec.Nominated("dead", now, testRank) {
		t.Fatal("stale nomination counted")
	}
	if !rec.MayTakeOverByRank("F", now, testRank) {
		t.Fatal("a stale nomination held back the only live nominee")
	}
}

// Ties on sequence go by node ID, so every node computes the same ranking.
func TestRankTieBreaksByNodeID(t *testing.T) {
	t0 := time.Unix(1000, 0)
	rec := &LockRecord{NodeID: "L", Followers: []string{"a", "b"}, Nominations: []Nomination{
		nomAt("b", 150, t0), nomAt("a", 150, t0),
	}}
	if !rec.MayTakeOverByRank("a", t0, testRank) || rec.MayTakeOverByRank("b", t0, testRank) {
		t.Fatal("equal sequences should rank by node ID")
	}
}

func TestNominate(t *testing.T) {
	store := object.NewMem()
	leader := newLockShared(store, "L", "addrL")
	rec, _, err := leader.TryAcquire(context.Background(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	f := newLockShared(store, "F", "addrF")
	g := newLockShared(store, "G", "addrG")

	// Nothing is written while allow rejects the record.
	if _, ok, err := f.Nominate(ctx, Nomination{NodeID: "F", Seq: 1}, func(*LockRecord) bool { return false }); err != nil || ok {
		t.Fatalf("nominated although allow rejected the record: %v, %v", ok, err)
	}
	for _, n := range []struct {
		lock *Lock
		nom  Nomination
	}{
		{f, Nomination{NodeID: "F", Seq: 1}},
		{g, Nomination{NodeID: "G", Seq: 2}},
		{f, Nomination{NodeID: "F", Seq: 3}}, // replaces F's earlier nomination
	} {
		if _, ok, err := n.lock.Nominate(ctx, n.nom, nil); err != nil || !ok {
			t.Fatalf("Nominate %+v: %v, %v", n.nom, ok, err)
		}
	}
	got, err := leader.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Term != rec.Term || got.NodeID != "L" {
		t.Fatalf("nominating changed the lock's holder: %+v", got)
	}
	seqs := map[string]int64{}
	for _, n := range got.Nominations {
		seqs[n.NodeID] = n.Seq
	}
	if len(got.Nominations) != 2 || seqs["F"] != 3 || seqs["G"] != 2 {
		t.Fatalf("nominations = %+v, want F:3 and G:2", got.Nominations)
	}

	// The holder never nominates for its own lock.
	if _, ok, _ := leader.Nominate(ctx, Nomination{NodeID: "L"}, nil); ok {
		t.Fatal("the lock holder nominated itself")
	}
}

// A lock write of the leader starts a clean slate: renewing lists its
// followers and drops nominations, and so does relinquishing the lock.
func TestLeaderWritesClearNominations(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()
	leader := newLockShared(store, "L", "addrL")
	rec, _, err := leader.TryAcquire(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	f := newLockShared(store, "F", "addrF")
	if _, ok, err := f.Nominate(ctx, Nomination{NodeID: "F"}, nil); err != nil || !ok {
		t.Fatal(err)
	}
	_, etag, err := leader.ReadETag(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := leader.Renew(ctx, rec.Term, "addrL", etag, 5, time.Now(), FastTTL, []string{"F", "G"}); err != nil {
		t.Fatal(err)
	}
	got, err := leader.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Nominations) != 0 || len(got.Followers) != 2 {
		t.Fatalf("after renew: nominations %+v, followers %v", got.Nominations, got.Followers)
	}

	if _, ok, err := f.Nominate(ctx, Nomination{NodeID: "F"}, nil); err != nil || !ok {
		t.Fatal(err)
	}
	if err := leader.Relinquish(ctx, rec.Term, 5); err != nil {
		t.Fatal(err)
	}
	if got, err = leader.Read(ctx); err != nil || len(got.Nominations) != 0 || !got.Released() {
		t.Fatalf("after relinquish: %+v, %v", got, err)
	}
}

// A takeover that checks its rank on the record it replaces fails when a
// better nomination landed first.
func TestTakeOverChecksRank(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()
	leader := newLockShared(store, "L", "addrL")
	rec, _, err := leader.TryAcquire(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// The leader lists both followers, so the election completes as soon as
	// both have nominated.
	_, etag, err := leader.ReadETag(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := leader.Renew(ctx, rec.Term, "addrL", etag, 0, time.Now().Add(-time.Minute), FastTTL, []string{"F", "G"}); err != nil {
		t.Fatal(err)
	}
	f := newLockShared(store, "F", "addrF")
	g := newLockShared(store, "G", "addrG")
	now := time.Now()
	for _, n := range []struct {
		lock *Lock
		nom  Nomination
	}{{f, nomAt("F", 150, now)}, {g, nomAt("G", 160, now)}} {
		if _, ok, err := n.lock.Nominate(ctx, n.nom, nil); err != nil || !ok {
			t.Fatal(err)
		}
	}
	byRank := func(id string) func(*LockRecord) bool {
		return func(r *LockRecord) bool { return r.MayTakeOverByRank(id, time.Now(), testRank) }
	}
	if _, won, err := f.TakeOver(ctx, rec.Term, 0, byRank("F")); err != nil || won {
		t.Fatalf("F took over although G outranks it: %v, %v", won, err)
	}
	if _, won, err := g.TakeOver(ctx, rec.Term, 0, byRank("G")); err != nil || !won {
		t.Fatalf("G could not take over at the top of the ranking: %v, %v", won, err)
	}
}

// Without conditional writes, concurrent nominations would overwrite each
// other, so none are written.
func TestNominateNeedsConditionalWrites(t *testing.T) {
	type plainStore struct{ object.Store }
	l := NewLock(plainStore{object.NewMem()}, "F", "addrF")
	if l.CanNominate() {
		t.Fatal("CanNominate on a store without conditional writes")
	}
	if _, _, err := l.Nominate(context.Background(), Nomination{NodeID: "F"}, nil); !errors.Is(err, ErrNominationsUnsupported) {
		t.Fatalf("Nominate = %v, want ErrNominationsUnsupported", err)
	}
}

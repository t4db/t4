package peer

import "testing"

// TestFollowDivergenceRules pins checkContinuesLog (#441). The leader has
// term 3, inherited a log ending at sequence 10 with term 2, and wrote from
// 11 on.
func TestFollowDivergenceRules(t *testing.T) {
	s := NewServer(16, 0, nil)
	s.SetTerm(3)
	s.SetStartRev(11, 2)
	for _, c := range []struct {
		name string
		req  FollowRequest
		want func(error) bool
	}{
		{"own entries", FollowRequest{FromRevision: 14, LastTerm: 3, Proto: 1}, ok},
		{"diverged ahead (the #441 case)", FollowRequest{FromRevision: 14, LastTerm: 2, Proto: 1}, diverged},
		{"at the inherited end", FollowRequest{FromRevision: 11, LastTerm: 2, Proto: 1}, ok},
		{"diverged at the inherited end", FollowRequest{FromRevision: 11, LastTerm: 1, Proto: 1}, diverged},
		{"below the inherited end", FollowRequest{FromRevision: 5, LastTerm: 1, Proto: 1}, ok}, // resync decides
		{"newer term: stale leader", FollowRequest{FromRevision: 14, LastTerm: 4, Proto: 1}, IsLeaderStale},
		{"legacy follower", FollowRequest{FromRevision: 14, LastTerm: 0}, ok},
		{"empty follower", FollowRequest{FromRevision: 1, Proto: 1}, ok},
	} {
		if err := s.checkContinuesLog(&c.req); !c.want(err) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	// A leader whose inherited last term is unknown does not check it.
	s.SetStartRev(11, 0)
	if err := s.checkContinuesLog(&FollowRequest{FromRevision: 11, LastTerm: 1, Proto: 1}); err != nil {
		t.Errorf("unknown inherited term: %v", err)
	}
	if term, isDiv := IsDiverged(DivergedError(7)); !isDiv || term != 7 {
		t.Errorf("IsDiverged(DivergedError(7)) = %d, %v", term, isDiv)
	}
	if _, isDiv := IsDiverged(ErrResyncRequired); isDiv {
		t.Error("ErrResyncRequired reads as diverged")
	}
}

func ok(err error) bool { return err == nil }

func diverged(err error) bool { _, d := IsDiverged(err); return d }

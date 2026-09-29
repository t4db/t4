package election

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/t4db/t4/pkg/object"
)

// Takeover ranking (docs/design/takeover-ranking.md). When the leader is gone,
// candidates nominate themselves in the lock with their applied position
// before any of them takes over, and the most up-to-date one goes first. The
// lock's fence only records the leader's revision at its last lock write, so
// a candidate that caught up from object storage can pass it while lacking
// writes a better-placed candidate holds; nominations let that candidate see
// it is not the best one.

// Nomination is a candidate's bid to succeed the lock's leader.
type Nomination struct {
	NodeID string `json:"node_id"`
	Seq    int64  `json:"seq"`     // applied WAL sequence when it nominated
	Rev    int64  `json:"rev"`     // applied revision when it nominated
	AtNano int64  `json:"at_nano"` // Unix ns, the nominator's clock
}

// RankTimes are the timings of a ranked takeover.
type RankTimes struct {
	// Window is how long after the first nomination an election waits for
	// expected candidates (Followers) that have not nominated.
	Window time.Duration
	// Stagger is how long each nominee may keep the lead without taking over
	// before the next-ranked one may.
	Stagger time.Duration
	// Skew bounds the clock offset between nodes.
	Skew time.Duration
	// MaxAge is how old a nomination may be before it is ignored, so that
	// nominations of an election nobody completed do not rank later ones.
	MaxAge time.Duration
}

// ErrNominationsUnsupported is returned by Nominate on a store without
// conditional writes, where concurrent nominations would overwrite each other.
var ErrNominationsUnsupported = errors.New("election: nominations need conditional writes")

// live returns r's nominations younger than t.MaxAge at now, best first:
// highest sequence, then node ID.
func (r *LockRecord) live(now time.Time, t RankTimes) []Nomination {
	out := make([]Nomination, 0, len(r.Nominations))
	for _, n := range r.Nominations {
		if t.MaxAge <= 0 || now.Sub(time.Unix(0, n.AtNano)) <= t.MaxAge {
			out = append(out, n)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Seq != out[j].Seq {
			return out[i].Seq > out[j].Seq
		}
		return out[i].NodeID < out[j].NodeID
	})
	return out
}

// Nominated reports whether nodeID has a live nomination in r.
func (r *LockRecord) Nominated(nodeID string, now time.Time, t RankTimes) bool {
	for _, n := range r.live(now, t) {
		if n.NodeID == nodeID {
			return true
		}
	}
	return false
}

// completeAt returns when the election in r is complete: once every expected
// candidate (Followers other than the leader that held the lock) has
// nominated, at the latest of their nominations; otherwise Window after the
// earliest nomination, plus Skew. ok is false if nobody has nominated.
func (r *LockRecord) completeAt(noms []Nomination, t RankTimes) (at time.Time, ok bool) {
	if len(noms) == 0 {
		return time.Time{}, false
	}
	earliest, byNode := noms[0].AtNano, make(map[string]int64, len(noms))
	for _, n := range noms {
		earliest = min(earliest, n.AtNano)
		byNode[n.NodeID] = n.AtNano
	}
	deadline := time.Unix(0, earliest).Add(t.Window + t.Skew)
	var latest int64
	expected := 0
	for _, id := range r.Followers {
		if id == r.NodeID {
			continue
		}
		expected++
		at, ok := byNode[id]
		if !ok {
			return deadline, true
		}
		latest = max(latest, at)
	}
	if expected == 0 {
		return deadline, true
	}
	if all := time.Unix(0, latest); all.Before(deadline) {
		return all, true
	}
	return deadline, true
}

// MayTakeOverByRank reports whether nodeID may take over at now by the
// nominations in r: it must have nominated, and the nominee ranked k-th
// (from 0) may from the election's completion plus k·Stagger on.
func (r *LockRecord) MayTakeOverByRank(nodeID string, now time.Time, t RankTimes) bool {
	noms := r.live(now, t)
	complete, ok := r.completeAt(noms, t)
	if !ok {
		return false
	}
	for k, n := range noms {
		if n.NodeID == nodeID {
			return !now.Before(complete.Add(time.Duration(k) * t.Stagger))
		}
	}
	return false
}

// CanNominate reports whether this lock's store supports nominations.
func (l *Lock) CanNominate() bool { return l.conditional != nil }

// Nominate records nom, replacing an earlier nomination of the same node,
// with a conditional write on the lock's ETag, retrying if another write lands
// in between. allow is evaluated on the record each write is based on, so a
// nomination is only written while it holds. Nothing is written when the lock
// is absent, held by this node, or allow rejects it; the record read is
// returned with nominated false.
func (l *Lock) Nominate(ctx context.Context, nom Nomination, allow func(*LockRecord) bool) (*LockRecord, bool, error) {
	if l.conditional == nil {
		return nil, false, ErrNominationsUnsupported
	}
	for attempt := 0; ; attempt++ {
		cur, err := l.readWithETag(ctx)
		if err != nil {
			return nil, false, err
		}
		if cur.rec == nil || cur.rec.NodeID == l.nodeID || (allow != nil && !allow(cur.rec)) {
			return cur.rec, false, nil
		}
		rec := *cur.rec
		rec.Nominations = make([]Nomination, 0, len(cur.rec.Nominations)+1)
		for _, n := range cur.rec.Nominations {
			if n.NodeID != nom.NodeID {
				rec.Nominations = append(rec.Nominations, n)
			}
		}
		rec.Nominations = append(rec.Nominations, nom)
		b, err := json.Marshal(&rec)
		if err != nil {
			return nil, false, err
		}
		err = l.conditional.PutIfMatch(ctx, LockKey, bytes.NewReader(b), cur.etag)
		if err == nil {
			return &rec, true, nil
		}
		if !errors.Is(err, object.ErrPreconditionFailed) || attempt == 4 {
			return nil, false, fmt.Errorf("election: nominate: %w", err)
		}
	}
}

package t4

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/internal/peer"
)

// Leader lease.
//
// Leadership is an object-storage lock. Nothing stops a leader at the moment
// another node takes the lock over, so the leader stops serving first: it
// holds a lease that starts when a lock write of its begins and ends
// leaseSafetyMargin before anyone may take the lock over. While the lease
// holds it may acknowledge writes and serve linearizable reads. This is safe
// as long as the clocks of any two nodes differ by less than
// leaseSafetyMargin.
//
// To keep object-storage traffic low the leader renews slowly while it can
// show that every follower that could take over is hearing it, and fast
// otherwise:
//
//   - Slow mode: every follower of this term is connected, exchanges
//     heartbeats and was heard within silenceTimeout, and none left within
//     knownFollowerWindow. The lock is renewed every Config.LeaderWatchInterval
//     and stays valid for slowTTLFactor times that. Followers only heartbeat
//     while they hear the leader, so none of them is about to take over.
//   - Fast mode otherwise: renewed every fastRenewInterval, valid for fastTTL.
//
// A node that is not a known follower (see mayTakeOver) takes over only once
// the lock's ValidUntil has passed. A known follower that lost the leader may
// take over after knownTakeoverDelay once the lock has not been renewed for
// fastTTL: by then the leader has noticed the silence and left slow mode.
//
// The timing has three inputs; everything else that matters for safety is
// derived from them below, and TestLeaseTimingInvariants checks the
// inequalities the safety argument needs.

// Inputs.
const (
	// heartbeatInterval is how often leader and followers exchange
	// heartbeats on the WAL stream.
	heartbeatInterval = peer.HeartbeatInterval // 500 ms

	// leaseSafetyMargin is the largest clock offset between two nodes the
	// lease tolerates.
	leaseSafetyMargin = 2 * time.Second

	// fastTTL is how long a lock written in fast mode stays valid. It is
	// fixed by compatibility: nodes of earlier releases back off for this
	// long after LastSeenNano.
	fastTTL = election.FastTTL // 6 s

	// The third input is Config.LeaderWatchInterval, the slow renewal
	// interval.
)

// Derived.
const (
	// silenceTimeout is how long either side of a WAL stream goes without
	// hearing the other before it drops the stream. The leader also stays in
	// slow mode only while it heard every follower within this long.
	silenceTimeout = peer.HeartbeatTimeout // 4 heartbeats = 2 s

	// fastRenewInterval leaves room for two missed renewals within fastTTL.
	fastRenewInterval = fastTTL / 3 // 2 s

	// slowTTLFactor is how many slow renewal intervals a lock written in
	// slow mode stays valid, again tolerating two missed renewals.
	slowTTLFactor = 3

	// minLeaderWatchInterval is the shortest slow renewal interval allowed.
	minLeaderWatchInterval = fastRenewInterval

	// knownTakeoverDelay is how long a known follower waits after it last
	// heard the leader before it may take over. It must cover the time a
	// live leader needs to leave slow mode: the follower keeps heartbeating
	// until silenceTimeout after it last heard the leader, the leader leaves
	// slow mode once it has not heard the follower for silenceTimeout, their
	// clocks may differ by leaseSafetyMargin, plus two heartbeats of slack
	// for send cadence and delivery.
	knownTakeoverDelay = 2*silenceTimeout + leaseSafetyMargin + 2*heartbeatInterval // 7 s

	// knownFollowerWindow is how long after a follower's stream ends the
	// leader stays in fast mode for it. It is a protocol constant, not
	// derived from Config, because leader and followers must agree on it.
	// Afterwards the follower counts as unknown and goes by ValidUntil.
	knownFollowerWindow = 60 * time.Second

	// followerKnownWindow is how long after it last heard the leader a
	// follower counts itself as known to it. The leader drops a follower's
	// stream at most silenceTimeout plus a heartbeat after the follower last
	// heard it, and clocks may differ by leaseSafetyMargin, so this ends
	// before the leader's knownFollowerWindow does.
	followerKnownWindow = knownFollowerWindow - silenceTimeout - heartbeatInterval - leaseSafetyMargin // 55.5 s
)

// Tuning. These trade latency and availability and play no part in safety.
const (
	// fenceAckWait is how long a write waits for every connected follower
	// to acknowledge it before the leader moves the election fence instead.
	fenceAckWait = time.Second

	// fenceTimeout bounds how long a write waits for the fence to move.
	fenceTimeout = 10 * time.Second

	// leaderCheckInterval is how often the leader re-evaluates its mode.
	leaderCheckInterval = 250 * time.Millisecond
)

// leaseGrant is the result of the last successful lock write of this leader.
type leaseGrant struct {
	start time.Time // when the write started
	slow  bool      // written in slow mode (valid for slowTTL)
}

// extendLease records a successful lock write that started at start.
func (n *Node) extendLease(start time.Time, slow bool) {
	g := &leaseGrant{start: start, slow: slow}
	for {
		cur := n.lease.Load()
		if cur != nil && !start.After(cur.start) {
			return
		}
		if n.lease.CompareAndSwap(cur, g) {
			return
		}
	}
}

// slowTTL is how long a lock written in slow mode stays valid.
func (n *Node) slowTTL() time.Duration {
	return slowTTLFactor * n.cfg.LeaderWatchInterval
}

// slowSafe reports whether every follower that could take over is hearing
// this leader (see peer.Server.SlowSafe).
func (n *Node) slowSafe(now time.Time) bool {
	srv := n.leasePeers.Load()
	return srv == nil || srv.SlowSafe(now, silenceTimeout, knownFollowerWindow)
}

// leaseDeadline returns when this leader's lease ends, or the zero time if it
// has none.
func (n *Node) leaseDeadline(now time.Time) time.Time {
	g := n.lease.Load()
	if g == nil {
		return time.Time{}
	}
	ttl := fastTTL
	if g.slow && n.slowSafe(now) {
		ttl = n.slowTTL()
	}
	return g.start.Add(ttl - leaseSafetyMargin)
}

// checkLease returns an error wrapping ErrNoLeader when this node is the
// leader but can no longer be sure it still is: acknowledging a write or
// serving a linearizable read could then contradict a newer leader. Nodes in
// other roles have no lease and pass.
func (n *Node) checkLease() error {
	if n.loadRole() != roleLeader {
		return nil
	}
	now := time.Now()
	if !now.Before(n.leaseDeadline(now)) {
		return fmt.Errorf("%w: leader lease expired; leadership cannot be confirmed", ErrNoLeader)
	}
	return nil
}

// mayTakeOver decides, on the lock record a takeover would replace, whether
// its holder may be presumed gone. heardTerm and heardAt are the term of the
// leader this node last heard heartbeats from and when (zero if never).
func mayTakeOver(rec *election.LockRecord, heardTerm uint64, heardAt, now time.Time) bool {
	if rec.Released() {
		return true
	}
	known := heardTerm != 0 && heardTerm == rec.Term && now.Sub(heardAt) < followerKnownWindow
	if known {
		// The leader has left slow mode by now if it is alive, so a lock
		// not renewed for fastTTL means its lease has ended.
		return now.Sub(heardAt) >= knownTakeoverDelay && now.Sub(rec.Renewed()) > fastTTL
	}
	return now.After(rec.ValidUntil())
}

// ── Election fence ───────────────────────────────────────────────────────────
//
// A candidate must not win if it lacks an acknowledged write. The lock's
// CommittedRev blocks every node whose revision is below it; fenceSeq is the
// sequence it covers. Rather than moving it on a timer, the leader moves it
// when a write would otherwise be acknowledged while some node that may lack
// it is not blocked: a connected follower that has not acknowledged it, a
// follower that left, or any node the leader has never heard from (bounded by
// the sequence this term started at). In a healthy cluster that is one lock
// write per term and one per follower that leaves or falls behind.

// fenceState coordinates fence requests from the commit loop with the lock
// writes of the watch loop.
type fenceState struct {
	mu       sync.Mutex
	wantSeq  int64
	wantRev  int64
	wantTerm uint64
	done     chan struct{} // closed and replaced after each successful lock write
}

// fenceNeeded reports whether acknowledging entries up to seq would leave a
// node that may lack them able to win a takeover.
func (n *Node) fenceNeeded(seq int64) bool {
	srv := n.leasePeers.Load()
	if srv == nil {
		return false
	}
	outside := srv.MaxSeqWithout(seq)
	if start := n.termStartSeq.Load(); start > outside {
		outside = start
	}
	return outside >= n.fenceSeq.Load()
}

// committedFence returns this node's applied state as an election fence: the
// position of its last applied entry and its revision.
func (n *Node) committedFence() election.Fence {
	db := n.db.Load()
	pos := db.LastPosition()
	return election.Fence{Rev: db.CurrentRevision(), Seq: pos.Seq, Term: pos.Term}
}

// leaderKnownFence returns what this node may claim as a takeover candidate:
// its leader-known position (what it applied from a leader or wrote as one),
// and its revision for lock records of earlier releases, which fence by
// revision alone.
func (n *Node) leaderKnownFence() election.Fence {
	db := n.db.Load()
	pos := db.LeaderKnownPosition()
	return election.Fence{Rev: db.CurrentRevision(), Seq: pos.Seq, Term: pos.Term}
}

// fenced records a successful lock write whose CommittedRev covers seq.
func (n *Node) fenced(seq int64) {
	for {
		cur := n.fenceSeq.Load()
		if seq <= cur || n.fenceSeq.CompareAndSwap(cur, seq) {
			break
		}
	}
	n.fence.mu.Lock()
	close(n.fence.done)
	n.fence.done = make(chan struct{})
	n.fence.mu.Unlock()
}

// fenceWanted returns the sequence, revision and entry term the next lock
// write must cover for pending fence requests.
func (n *Node) fenceWanted() (seq, rev int64, term uint64) {
	n.fence.mu.Lock()
	defer n.fence.mu.Unlock()
	return n.fence.wantSeq, n.fence.wantRev, n.fence.wantTerm
}

// fencePending reports whether a fence request is waiting for a lock write:
// the fence must move, or the lock must record a change of the object
// storage flag.
func (n *Node) fencePending() bool {
	wantSeq, _, _ := n.fenceWanted()
	return wantSeq > n.fenceSeq.Load() || n.flagStale()
}

// flagWriteStarting records that a lock write carrying complete as the lock's
// ObjectStoreComplete is about to be attempted. A write that reports an error
// may still have landed, and the lock claiming object storage is complete
// when it is not is the unsafe direction: a write of true counts from the
// attempt on, and only a successful write of false (flagWritten) clears it.
func (n *Node) flagWriteStarting(complete bool) {
	if complete {
		n.lockObjectStoreComplete.Store(true)
	}
}

// flagWritten records that a lock write carrying complete succeeded.
func (n *Node) flagWritten(complete bool) {
	n.lockObjectStoreComplete.Store(complete)
}

// flagStale reports whether the lock's ObjectStoreComplete, as last written,
// differs from what the commit loop now wants it to say.
func (n *Node) flagStale() bool {
	return n.lockObjectStoreComplete.Load() != n.objectStoreComplete.Load()
}

// awaitFence asks the watch loop to move the fence to cover the entry at
// seq, of revision rev and term term, and to record the current object
// storage flag, and waits until a lock write has done both.
func (n *Node) awaitFence(ctx context.Context, seq, rev int64, term uint64) error {
	ctx, cancel := context.WithTimeout(ctx, fenceTimeout)
	defer cancel()
	for {
		if n.fenceSeq.Load() >= seq && !n.flagStale() {
			return nil
		}
		n.fence.mu.Lock()
		if seq > n.fence.wantSeq {
			n.fence.wantSeq, n.fence.wantRev, n.fence.wantTerm = seq, rev, term
		}
		done := n.fence.done
		n.fence.mu.Unlock()
		select {
		case n.fenceReqC <- struct{}{}:
		default:
		}
		select {
		case <-done:
		case <-ctx.Done():
			return fmt.Errorf("%w: could not record the election fence: %v", ErrNoLeader, ctx.Err())
		}
	}
}

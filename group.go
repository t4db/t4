package t4

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/pkg/object"
)

// Group leadership.
//
// A group is the election side of t4: the leader lock, the lease a lock
// write grants (lease.go), and the loop that renews the lock and records the
// election fence. Its member is the database it leads or follows, which
// keeps its own log, its fence position and its replication. A node is, for
// now, a group of one: itself (docs/design/group-leadership.md).
type group struct {
	lock          *election.Lock // nil without object storage
	nodeID        string
	advertiseAddr string
	watchInterval time.Duration // Config.LeaderWatchInterval
	log           Logger

	// lease is the group's last successful lock write as leader; nil until
	// it first holds the lock.
	lease atomic.Pointer[leaseGrant]
	// fenceReqC wakes the watch loop when the member needs the fence moved
	// (see Node.awaitFence).
	fenceReqC chan struct{}

	member *Node
}

// newGroup returns the group of member, configured by cfg, whose ObjectStore
// is the one the node uses.
func newGroup(cfg Config, log Logger, member *Node) *group {
	g := &group{
		nodeID:        cfg.NodeID,
		advertiseAddr: cfg.AdvertisePeerAddr,
		watchInterval: cfg.LeaderWatchInterval,
		log:           log,
		fenceReqC:     make(chan struct{}, 1),
		member:        member,
	}
	if cfg.ObjectStore != nil {
		g.lock = election.NewLock(cfg.ObjectStore, cfg.NodeID, cfg.AdvertisePeerAddr)
	}
	return g
}

// extendLease records a successful lock write that started at start.
func (g *group) extendLease(start time.Time, slow bool) {
	lg := &leaseGrant{start: start, slow: slow}
	for {
		cur := g.lease.Load()
		if cur != nil && !start.After(cur.start) {
			return
		}
		if g.lease.CompareAndSwap(cur, lg) {
			return
		}
	}
}

// slowTTL is how long a lock written in slow mode stays valid.
func (g *group) slowTTL() time.Duration {
	return slowTTLFactor * g.watchInterval
}

// slowSafe reports whether every follower that could take over is hearing
// this leader (see peer.Server.SlowSafe).
func (g *group) slowSafe(now time.Time) bool {
	if g.member == nil {
		return true
	}
	srv := g.member.leasePeers.Load()
	return srv == nil || srv.SlowSafe(now, silenceTimeout, knownFollowerWindow)
}

// leaseDeadline returns when this leader's lease ends, or the zero time if it
// has none.
func (g *group) leaseDeadline(now time.Time) time.Time {
	lg := g.lease.Load()
	if lg == nil {
		return time.Time{}
	}
	ttl := fastTTL
	if lg.slow && g.slowSafe(now) {
		ttl = g.slowTTL()
	}
	return lg.start.Add(ttl - leaseSafetyMargin)
}

// watchLoop keeps the group's leadership valid and detects when it is lost.
//
// It renews the lock (see lease.go): it reads the lock, then rewrites it with
// the time the renewal started, how long it stays valid and the election
// fence, conditioned on the ETag it read, so a TakeOver in between is
// detected and the member steps down. It renews every LeaderWatchInterval
// while every follower is demonstrably hearing it, every fastRenewInterval
// otherwise, at once when it leaves slow mode, and whenever the member's
// commit loop needs the fence moved.
func (g *group) watchLoop(ctx context.Context, term uint64) {
	m := g.member
	var disconnectC <-chan struct{}
	if m.peerSrv != nil {
		disconnectC = m.peerSrv.DisconnectC
	}
	check := time.NewTicker(leaderCheckInterval)
	defer check.Stop()
	slowInterval := g.watchInterval

	stepDown := func(reason, why string) {
		g.log.Errorf("t4: leader watch (%s): %s — stepping down", reason, why)
		m.stepDown(fmt.Errorf("t4: stepped down as leader: %s", why))
	}

	// failed handles a renewal that could not reach object storage. Once
	// the lease has ended too, this leader serves nothing, yet followers
	// that still hear it never take over: step down so that one that can
	// reach object storage does. It waits a renewal interval past the end
	// of the lease first, so that one failed renewal right when the lease
	// shortens (leaving slow mode) is retried rather than ending the term.
	failed := func(reason string, err error) bool {
		if now := time.Now(); now.Before(g.leaseDeadline(now).Add(fastRenewInterval)) {
			return true
		}
		stepDown(reason, fmt.Sprintf("lease ended and the lock cannot be renewed: %v", err))
		return false
	}

	// renew writes the lock once; it returns false after stepping down. It
	// does not take the member's fenceMu: writes waiting on follower
	// acknowledgements must not delay it. Safety comes from the lease, which
	// every write acknowledgement and linearizable read checks.
	renew := func(reason string, slow bool) bool {
		// A fenced node (Close in flight, commitLoop dead from a fatal
		// error, …) must not assert liveness it cannot back up; whichever
		// path set n.closed tears down the peer server.
		if m.closed.Load() {
			g.log.Debugf("t4: leader watch (%s): node fenced — exiting watch loop", reason)
			return false
		}
		start := time.Now()
		rCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		rec, etag, err := g.lock.ReadETag(rCtx)
		cancel()
		if err != nil {
			g.log.Warnf("t4: leader watch (%s): read lock: %v", reason, err)
			return failed(reason, err)
		}
		if rec == nil || rec.Term != term || rec.NodeID != g.nodeID {
			stepDown(reason, fmt.Sprintf("lock superseded (current: %+v)", rec))
			return false
		}
		fence, seq, complete := m.lockFence()
		ttl := fastTTL
		if slow {
			ttl = g.slowTTL()
		}
		tCtx, tCancel := context.WithTimeout(ctx, 5*time.Second)
		err = g.lock.Renew(tCtx, term, g.advertiseAddr, etag, fence, complete, start, ttl)
		tCancel()
		if errors.Is(err, object.ErrPreconditionFailed) {
			stepDown(reason, "renewal precondition failed — lock taken")
			return false
		}
		if err != nil {
			g.log.Warnf("t4: leader watch (%s): renew lock: %v", reason, err)
			return failed(reason, err)
		}
		g.extendLease(start, slow)
		m.lockWritten(seq, complete)
		return true
	}

	var lastAttempt time.Time
	if !renew("start", false) {
		return
	}
	lastAttempt = time.Now()
	for {
		reason := ""
		select {
		case <-check.C:
		case <-disconnectC:
			reason = "disconnect"
		case <-g.fenceReqC:
			reason = "fence"
		case <-ctx.Done():
			return
		}
		now := time.Now()
		slow := g.slowSafe(now)
		lg := g.lease.Load()
		interval := fastRenewInterval
		if slow {
			interval = slowInterval
		}
		switch {
		case now.Sub(lastAttempt) >= interval:
			if reason == "" {
				reason = "renew"
			}
		case lg != nil && lg.slow && !slow:
			// The last renewal was slow but a follower may no longer
			// hear this leader: the lease now runs from that renewal
			// for fastTTL only and may have ended. Renew at once.
			reason = "fast mode"
		case lg != nil && !lg.slow && slow:
			// Entering slow mode, the next renewal is slowInterval
			// away, but the lease from the last (fast) renewal ends
			// fastTTL after it. Renew now to obtain a slow lease.
			reason = "slow mode"
		case m.fencePending():
			reason = "fence"
		default:
			continue
		}
		if now.Sub(lastAttempt) < leaderCheckInterval {
			continue // at most one attempt per check interval
		}
		lastAttempt = now
		if !renew(reason, slow) {
			return
		}
	}
}

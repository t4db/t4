package t4

import (
	"fmt"
	"time"

	"github.com/t4db/t4/internal/peer"
)

// Leader lease.
//
// Leadership is an object-storage lock that a follower may take over once
// the leader's liveness record (LockRecord.LastSeenNano) is older than
// peer.LeaderLivenessTTL. Nothing stops the previous leader at that moment,
// so the leader itself must stop serving first: it holds a lease that starts
// when it begins a successful lock write and lasts leaseSafetyMargin less
// than the liveness TTL. While the lease is valid it may acknowledge writes
// and serve linearizable reads; once it lapses it refuses both until it
// renews. A candidate can take over only after the full TTL, so the previous
// leader has stopped serving by then, provided the clocks of the two nodes
// differ by less than leaseSafetyMargin.
//
// The leader renews the lease every peer.FollowerRetryInterval, well within
// its duration.
const leaseSafetyMargin = peer.FollowerRetryInterval

const leaseDuration = peer.LeaderLivenessTTL - leaseSafetyMargin

// extendLease records a successful lock write that started at writeStart.
func (n *Node) extendLease(writeStart time.Time) {
	deadline := writeStart.Add(leaseDuration)
	for {
		cur := n.leaseDeadline.Load()
		if cur != nil && !deadline.After(*cur) {
			return
		}
		if n.leaseDeadline.CompareAndSwap(cur, &deadline) {
			return
		}
	}
}

// checkLease returns an error wrapping ErrNoLeader when this node is the
// leader but can no longer be sure it still is: acknowledging a write or
// serving a linearizable read could then contradict a newer leader. Nodes in
// other roles have no lease and pass.
func (n *Node) checkLease() error {
	if n.loadRole() != roleLeader {
		return nil
	}
	deadline := n.leaseDeadline.Load()
	if deadline == nil || !time.Now().Before(*deadline) {
		return fmt.Errorf("%w: leader lease expired; leadership cannot be confirmed", ErrNoLeader)
	}
	return nil
}

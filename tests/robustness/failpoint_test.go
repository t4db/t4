package robustness

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// failpoint is a failure injected into a running cluster while traffic runs.
// Inject returns once the cluster has recovered.
type failpoint interface {
	Name() string
	// Available reports whether the failpoint applies to a cluster this size.
	Available(size int) bool
	Inject(ctx context.Context, c *cluster, bin string) error
}

// killFailpoint SIGKILLs nodes and restarts them after a pause.
type killFailpoint struct {
	name string
	pick func(ctx context.Context, c *cluster) ([]*node, error)
	// multiNode marks failpoints that only make sense with followers.
	multiNode bool
}

func (f killFailpoint) Name() string { return f.name }

func (f killFailpoint) Available(size int) bool { return !f.multiNode || size > 1 }

func (f killFailpoint) Inject(ctx context.Context, c *cluster, bin string) error {
	victims, err := f.pick(ctx, c)
	if err != nil {
		return err
	}
	for _, n := range victims {
		n.kill()
	}
	time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
	for _, n := range victims {
		if err := n.start(bin); err != nil {
			return err
		}
	}
	return c.waitReady(ctx)
}

// stopFailpoint restarts the leader gracefully, so it hands leadership over
// rather than having it taken away.
type stopFailpoint struct{}

func (stopFailpoint) Name() string { return "RestartLeader" }

func (stopFailpoint) Available(size int) bool { return size > 1 }

func (stopFailpoint) Inject(ctx context.Context, c *cluster, bin string) error {
	l, err := c.leader(ctx)
	if err != nil {
		return err
	}
	l.stop()
	if err := l.start(bin); err != nil {
		return err
	}
	return c.waitReady(ctx)
}

// networkFailpoint degrades one node's network for a while, then heals it.
// Network failpoints need a cluster: a single node runs without S3 and has
// no peers.
type networkFailpoint struct {
	name string
	pick func(ctx context.Context, c *cluster) (*node, error)
	// fault applies the fault to the node's proxies, or heals them.
	fault func(n *node, on bool)
	hold  time.Duration
}

func (f networkFailpoint) Name() string { return f.name }

func (f networkFailpoint) Available(size int) bool { return size > 1 }

func (f networkFailpoint) Inject(ctx context.Context, c *cluster, _ string) error {
	n, err := f.pick(ctx, c)
	if err != nil {
		return err
	}
	f.fault(n, true)
	select {
	case <-time.After(f.hold):
	case <-ctx.Done():
	}
	f.fault(n, false)
	return c.waitReady(ctx)
}

func pickLeader(ctx context.Context, c *cluster) (*node, error) {
	return c.leader(ctx)
}

func pickFollower(ctx context.Context, c *cluster) (*node, error) {
	l, err := c.leader(ctx)
	if err != nil {
		return nil, err
	}
	var followers []*node
	for _, n := range c.nodes {
		if n != l {
			followers = append(followers, n)
		}
	}
	return followers[rand.Intn(len(followers))], nil
}

var failpoints = []failpoint{
	killFailpoint{
		name: "KillRandomNode",
		pick: func(_ context.Context, c *cluster) ([]*node, error) {
			return []*node{c.nodes[rand.Intn(len(c.nodes))]}, nil
		},
	},
	killFailpoint{
		name:      "KillLeader",
		multiNode: true,
		pick: func(ctx context.Context, c *cluster) ([]*node, error) {
			l, err := c.leader(ctx)
			if err != nil {
				return nil, err
			}
			return []*node{l}, nil
		},
	},
	killFailpoint{
		name:      "KillFollower",
		multiNode: true,
		pick: func(ctx context.Context, c *cluster) ([]*node, error) {
			l, err := c.leader(ctx)
			if err != nil {
				return nil, err
			}
			var followers []*node
			for _, n := range c.nodes {
				if n != l {
					followers = append(followers, n)
				}
			}
			return []*node{followers[rand.Intn(len(followers))]}, nil
		},
	},
	killFailpoint{
		name:      "KillAllNodes",
		multiNode: true,
		pick: func(_ context.Context, c *cluster) ([]*node, error) {
			return c.nodes, nil
		},
	},
	stopFailpoint{},
	// Followers lose the leader. It keeps renewing its lock through S3, so
	// it stays leader and makes writes durable in S3 until they return.
	networkFailpoint{
		name:  "BlackholePeerLeader",
		pick:  pickLeader,
		fault: func(n *node, on bool) { n.peerProxy.setBlackhole(on) },
		hold:  5 * time.Second,
	},
	networkFailpoint{
		name: "DelayPeerLeader",
		pick: pickLeader,
		fault: func(n *node, on bool) {
			var d time.Duration
			if on {
				d = time.Duration(50+rand.Intn(150)) * time.Millisecond
			}
			n.peerProxy.setDelay(d)
		},
		hold: 5 * time.Second,
	},
	// The leader loses S3 for longer than its lease: it stops serving and
	// steps down, its followers stop hearing it and one of them takes over
	// through S3. t4 exits on stepping down; the cluster restarts it.
	networkFailpoint{
		name:  "BlackholeS3Leader",
		pick:  pickLeader,
		fault: func(n *node, on bool) { n.s3Proxy.setBlackhole(on) },
		hold:  10 * time.Second,
	},
	networkFailpoint{
		name:  "BlackholeS3Follower",
		pick:  pickFollower,
		fault: func(n *node, on bool) { n.s3Proxy.setBlackhole(on) },
		hold:  10 * time.Second,
	},
	// The leader loses object storage and its followers at once. This is the
	// shape the leader lease exists for, and the only one that puts two nodes
	// in a position to each believe they lead: the leader cannot renew the
	// lock, so its lease runs out, while its followers stop hearing it and one
	// takes over. Neither of the single faults reaches it —
	// BlackholeS3Leader's peers keep hearing it, so nobody takes over, and
	// BlackholePeerLeader keeps renewing through object storage, so it stays
	// leader. The hold outlasts the 6 s lease, and clients can still reach the
	// old leader throughout, which is what makes a stale read from it
	// possible if the lease were ever not enforced.
	networkFailpoint{
		name: "BlackholeS3AndPeerLeader",
		pick: pickLeader,
		fault: func(n *node, on bool) {
			n.s3Proxy.setBlackhole(on)
			n.peerProxy.setBlackhole(on)
		},
		// Just past the lease. Cutting both at once leaves the whole cluster
		// unable to write until a follower takes over, so every extra second
		// of hold is another few hundred failed operations, and failed
		// operations are what the linearizability checker is expensive in.
		//
		// Read a failure here carefully. Over five runs of EtcdPutDeleteLease
		// this produced one "Linearization illegal" and four checker
		// timeouts, and never a clean pass; a 10 s hold timed out as well.
		// A timeout says the history was too costly to settle, not that T4
		// misbehaved. The one illegal run localised to a delete that had
		// timed out inside the isolation window, and the unpatched
		// cross-check at robustness_test.go:197 could not settle that history
		// either, in 30 minutes per pass — so whether it was T4 or the
		// patching of failed operations from the watch stream is still open.
		// TestSupersededLeaderServesNoStaleRead covers the one property this
		// scenario was added for, deterministically.
		hold: 7 * time.Second,
	},
}

func failpointByName(name string) (failpoint, error) {
	for _, f := range failpoints {
		if f.Name() == name {
			return f, nil
		}
	}
	return nil, fmt.Errorf("unknown failpoint %q", name)
}

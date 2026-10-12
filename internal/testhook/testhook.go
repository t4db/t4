// Package testhook holds switches that let tests put T4 into states a
// production binary never chooses on its own. Nothing outside tests may set
// them.
package testhook

import (
	"sync/atomic"

	"github.com/t4db/t4/internal/wal"
)

// LegacyNewDatabases makes nodes create new databases without the meta
// keyspace, the way releases before it did. Tests use it to exercise the
// legacy format that existing databases keep.
var LegacyNewDatabases atomic.Bool

// V1Leader makes a leader reject forwarded writes that a v1.1 leader would
// not understand (newer forward ops, txn condition targets, or txn op types)
// instead of executing them. Tests use it to check that followers on this
// release stay compatible with a leader that has not been upgraded yet.
var V1Leader atomic.Bool

// BeforeMetaGenesis, if set, runs in Open on a leader just before it fixes the
// meta mode of a new database, with the peer server already serving. Tests
// use it to hold a leader in that window.
var BeforeMetaGenesis atomic.Pointer[func()]

// BeforeFollowerApply, if set, runs on a follower just before it applies a
// batch the leader committed, after the follower has made it durable and
// acknowledged it. Tests use it to hold a follower behind what its leader
// acknowledged.
var BeforeFollowerApply atomic.Pointer[func(entries []wal.Entry)]

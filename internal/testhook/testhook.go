// Package testhook holds switches that let tests put T4 into states a
// production binary never chooses on its own. Nothing outside tests may set
// them.
package testhook

import "sync/atomic"

// LegacyNewDatabases makes nodes create new databases without the meta
// keyspace, the way releases before it did. Tests use it to exercise the
// legacy format that existing databases keep.
var LegacyNewDatabases atomic.Bool

// V1Leader makes a leader reject forwarded writes that a v1.1 leader would
// not understand (newer forward ops, txn condition targets, or txn op types)
// instead of executing them. Tests use it to check that followers on this
// release stay compatible with a leader that has not been upgraded yet.
var V1Leader atomic.Bool

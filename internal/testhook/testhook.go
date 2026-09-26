// Package testhook holds switches that let tests put T4 into states a
// production binary never chooses on its own. Nothing outside tests may set
// them.
package testhook

import "sync/atomic"

// LegacyNewDatabases makes nodes create new databases without the meta
// keyspace, the way releases before it did. Tests use it to exercise the
// legacy format that existing databases keep.
var LegacyNewDatabases atomic.Bool

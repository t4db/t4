package wal

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/makhov/strata/internal/object"
)

// GCSegments deletes WAL segments from object storage whose entire revision
// range is covered by checkpointRev.
//
// A segment starting at firstRev[i] covers entries [firstRev[i], firstRev[i+1]-1].
// It is safe to delete when firstRev[i+1]-1 <= checkpointRev, i.e.
// firstRev[i+1] <= checkpointRev+1.
//
// The most recent segment that starts at or before the checkpoint is always
// retained as it may contain entries after checkpointRev.
//
// Returns the number of segments deleted.
func GCSegments(ctx context.Context, store object.Store, checkpointRev int64) (int, error) {
	keys, err := store.List(ctx, "wal/")
	if err != nil {
		return 0, fmt.Errorf("wal gc: list: %w", err)
	}
	if len(keys) < 2 {
		return 0, nil // nothing or only one segment — nothing to GC
	}

	// keys are returned sorted (lexicographic == chronological given our naming).
	firstRevs := make([]int64, len(keys))
	for i, k := range keys {
		firstRevs[i] = parseFirstRev(k)
	}

	var deleted int
	for i := 0; i < len(keys)-1; i++ {
		nextFirstRev := firstRevs[i+1]
		// Segment i ends at nextFirstRev-1; safe to delete if that is <= checkpointRev.
		if nextFirstRev-1 <= checkpointRev {
			if err := store.Delete(ctx, keys[i]); err != nil {
				logrus.Warnf("wal gc: delete %q: %v", keys[i], err)
				continue
			}
			deleted++
			logrus.Debugf("wal gc: deleted %q (covered by checkpoint rev=%d)", keys[i], checkpointRev)
		}
	}
	return deleted, nil
}

// parseFirstRev extracts the firstRev integer from a WAL object key of the
// form "wal/{term:010d}/{firstRev:020d}".
func parseFirstRev(key string) int64 {
	parts := strings.Split(key, "/")
	if len(parts) < 3 {
		return 0
	}
	rev, _ := strconv.ParseInt(strings.TrimLeft(parts[2], "0"), 10, 64)
	return rev
}

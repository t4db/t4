package t4

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/t4db/t4/internal/wal"
)

func (n *Node) initRevisionSampler() {
	if n.cfg.AutoCompactMode != AutoCompactTime || n.cfg.AutoCompactRetention <= 0 {
		return
	}
	_, ts, ok, err := n.db.Load().LatestRevisionSample()
	if err != nil {
		n.log.Warnf("t4: auto-compact sample init: %v", err)
		return
	}
	if ok {
		atomic.StoreInt64(&n.lastRevisionSampleUnix, ts.UnixNano())
		return
	}
	rev := n.db.Load().CurrentRevision()
	if rev == 0 {
		return
	}
	now := time.Now()
	if err := n.db.Load().RevisionSample(rev, now); err != nil {
		n.log.Warnf("t4: auto-compact startup sample: %v", err)
		return
	}
	atomic.StoreInt64(&n.lastRevisionSampleUnix, now.UnixNano())
}

func (n *Node) maybeRecordRevisionSample(entries []wal.Entry) {
	if n.cfg.AutoCompactMode != AutoCompactTime || n.cfg.AutoCompactRetention <= 0 || n.cfg.AutoCompactSampleInterval <= 0 {
		return
	}
	rev := maxUserRevision(entries)
	if rev <= 0 {
		return
	}
	now := time.Now()
	last := atomic.LoadInt64(&n.lastRevisionSampleUnix)
	if last > 0 && now.Sub(time.Unix(0, last)) < n.cfg.AutoCompactSampleInterval {
		return
	}
	if err := n.db.Load().RevisionSample(rev, now); err != nil {
		n.log.Warnf("t4: record auto-compact revision sample: %v", err)
		return
	}
	atomic.StoreInt64(&n.lastRevisionSampleUnix, now.UnixNano())
}

func maxUserRevision(entries []wal.Entry) int64 {
	var maxRev int64
	for i := range entries {
		if entries[i].Op == wal.OpCompact {
			continue
		}
		if entries[i].Revision > maxRev {
			maxRev = entries[i].Revision
		}
	}
	return maxRev
}

func (n *Node) autoCompactLoop(ctx context.Context) {
	n.maybeAutoCompact(ctx)

	ticker := time.NewTicker(n.cfg.AutoCompactInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.maybeAutoCompact(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (n *Node) maybeAutoCompact(ctx context.Context) {
	if !n.autoCompactEnabled() || n.loadRole() == roleFollower {
		return
	}
	var rev int64
	var sampleTime time.Time
	switch n.cfg.AutoCompactMode {
	case AutoCompactTime:
		targetRev, targetTime, ok, err := n.autoCompactTimeTarget()
		if err != nil {
			n.log.Warnf("t4: auto-compact sample lookup: %v", err)
			return
		}
		if !ok {
			return
		}
		rev, sampleTime = targetRev, targetTime
	case AutoCompactRevision:
		targetRev, ok := n.autoCompactRevisionTarget()
		if !ok {
			return
		}
		rev = targetRev
	default:
		return
	}
	if rev <= n.CompactRevision() || rev > n.CurrentRevision() {
		return
	}

	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err := n.Compact(cctx, rev)
	cancel()
	if err != nil {
		n.log.Warnf("t4: auto-compact rev=%d: %v", rev, err)
		return
	}
	if n.cfg.AutoCompactMode == AutoCompactTime {
		if err := n.db.Load().DeleteRevisionSamplesBefore(sampleTime); err != nil {
			n.log.Warnf("t4: auto-compact sample gc: %v", err)
		}
		n.log.Infof("t4: auto-compacted history at rev=%d (mode=time sample_time=%s retention=%s)",
			rev, sampleTime.Format(time.RFC3339), n.cfg.AutoCompactRetention)
		return
	}
	n.log.Infof("t4: auto-compacted history at rev=%d (mode=revision retained_revisions=%d)",
		rev, n.cfg.AutoCompactRevisionRetention)
}

func (n *Node) autoCompactEnabled() bool {
	switch n.cfg.AutoCompactMode {
	case AutoCompactTime:
		return n.cfg.AutoCompactRetention > 0 && n.cfg.AutoCompactInterval > 0
	case AutoCompactRevision:
		return n.cfg.AutoCompactRevisionRetention > 0 && n.cfg.AutoCompactInterval > 0
	default:
		return false
	}
}

func (n *Node) autoCompactTimeTarget() (rev int64, sampleTime time.Time, ok bool, err error) {
	cutoff := time.Now().Add(-n.cfg.AutoCompactRetention)
	return n.db.Load().RevisionSampleAtOrBefore(cutoff)
}

func (n *Node) autoCompactRevisionTarget() (rev int64, ok bool) {
	target := n.CurrentRevision() - n.cfg.AutoCompactRevisionRetention
	if target <= 0 {
		return 0, false
	}
	return target, true
}

package etcd_test

import (
	"context"
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
)

// Kubernetes decides whether a storage backend supports RequestWatchProgress
// from the version string the backend reports in Status — nothing else. From
// k8s.io/apiserver/pkg/storage/feature/feature_support_checker.go:
//
//	if ver.LessThan(3.4.31) || (ver.AtLeast(3.5.0) && ver.LessThan(3.5.13)) {
//	        return false
//	}
//
// When that check fails, ConsistentListFromCache and WatchList both switch
// off, and every list with ResourceVersion="" becomes a quorum range against
// the store instead of being served from the apiserver's watch cache. That is
// the single largest allocation a control plane makes, on both sides, and it
// comes back silently — upstream logs a line and carries on.
//
// So the version T4 reports is load-bearing for memory, and it is one string
// literal. This test is what stands between editing it and every apiserver in
// the fleet quietly reverting to full lists.
func TestReportedVersionKeepsWatchProgressSupport(t *testing.T) {
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("t4.Open: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })

	srv := t4etcd.New(node, nil, nil)
	resp, err := srv.Status(context.Background(), &etcdserverpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if resp.Version == "" {
		t.Fatal("Status reported no version; Kubernetes reads this to decide watch-progress support")
	}

	var major, minor, patch int
	if _, err := fmt.Sscanf(resp.Version, "%d.%d.%d", &major, &minor, &patch); err != nil {
		t.Fatalf("Status version %q is not major.minor.patch: %v", resp.Version, err)
	}

	// The two windows upstream rejects.
	tooOld := lessThan(major, minor, patch, 3, 4, 31)
	in35Window := !lessThan(major, minor, patch, 3, 5, 0) && lessThan(major, minor, patch, 3, 5, 13)
	if tooOld || in35Window {
		t.Fatalf("Status reports etcd version %q, which Kubernetes treats as not supporting "+
			"RequestWatchProgress: consistent-list-from-cache and WatchList switch off and every "+
			"RV=\"\" list becomes a full range read. Report >= 3.5.13 (or >= 3.4.31 and < 3.5.0).",
			resp.Version)
	}
}

func lessThan(major, minor, patch, wantMajor, wantMinor, wantPatch int) bool {
	if major != wantMajor {
		return major < wantMajor
	}
	if minor != wantMinor {
		return minor < wantMinor
	}
	return patch < wantPatch
}

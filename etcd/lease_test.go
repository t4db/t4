package etcd_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	t4etcd "github.com/t4db/t4/etcd"
)

// TestLeaseGrantConcurrentExplicitSameIDRejected exercises the v1 contract
// that a caller-supplied lease ID can be granted by exactly one concurrent
// caller. LeaseGrant performs an internal exists-check followed by a Put on
// the lease key. If the check-then-put is not atomic against concurrent
// callers, two grants could both pass the read step and both succeed without
// AlreadyExists. This test fails if more than one Grant call for the same
// explicit ID succeeds.
func TestLeaseGrantConcurrentExplicitSameIDRejected(t *testing.T) {
	_, cli := newWatchNode(t)
	lease := etcdserverpb.NewLeaseClient(cli.ActiveConnection())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const id int64 = 0x4242
	const goroutines = 8

	var (
		successes      atomic.Int64
		alreadyExists  atomic.Int64
		unexpectedErrs atomic.Int64
		wg             sync.WaitGroup
	)

	start := make(chan struct{})
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			_, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 60})
			switch {
			case err == nil:
				successes.Add(1)
			case status.Code(err) == codes.AlreadyExists:
				alreadyExists.Add(1)
			default:
				unexpectedErrs.Add(1)
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := successes.Load(); got != 1 {
		t.Errorf("successes: got %d, want exactly 1", got)
	}
	if got := alreadyExists.Load(); got != goroutines-1 {
		t.Errorf("AlreadyExists: got %d, want %d", got, goroutines-1)
	}
	if got := unexpectedErrs.Load(); got != 0 {
		t.Errorf("unexpected errors: %d", got)
	}
}

// raceRevoke revokes a fresh lease concurrently with write, rounds times.
// setup attaches whatever the round needs to the lease first.
func raceRevoke(t *testing.T, srv *t4etcd.Server, rounds int, setup func(lease int64), write func(lease int64), check func(lease int64)) {
	t.Helper()
	ctx := context.Background()
	for range rounds {
		g, err := srv.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 60})
		if err != nil {
			t.Fatal(err)
		}
		setup(g.ID)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := srv.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: g.ID}); err != nil {
				t.Errorf("revoke: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			write(g.ID)
		}()
		wg.Wait()
		check(g.ID)
	}
}

// TestLeaseRevokeRacesDetachingPut pins that a revoke deletes only keys still
// attached to the lease when it commits: a key re-put without the lease
// concurrently must exist afterwards, whichever went first.
func TestLeaseRevokeRacesDetachingPut(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()
	raceRevoke(t, srv, 200,
		func(lease int64) {
			if _, err := srv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/k"), Value: []byte("leased"), Lease: lease}); err != nil {
				t.Fatal(err)
			}
		},
		func(int64) {
			if _, err := srv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/k"), Value: []byte("plain")}); err != nil {
				t.Errorf("put: %v", err)
			}
		},
		func(int64) {
			r, err := srv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/k")})
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Kvs) != 1 || r.Kvs[0].Lease != 0 {
				t.Fatalf("after revoke raced an unleased put, /k = %v, want the unleased value", r.Kvs)
			}
		})
}

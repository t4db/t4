package replicate

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/internal/testhook"
)

// startT4 starts a T4 node behind the etcd adapter and returns a client.
func startT4(t *testing.T, legacy bool) *clientv3.Client {
	t.Helper()
	testhook.LegacyNewDatabases.Store(legacy)
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	testhook.LegacyNewDatabases.Store(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(t4etcd.NewServerOptions(nil, nil)...)
	t4etcd.New(node, nil, nil).Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{lis.Addr().String()},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func quietLog() logrus.FieldLogger {
	l := logrus.New()
	l.SetLevel(logrus.WarnLevel)
	return l
}

func newReplicator(t *testing.T, source, target *clientv3.Client) *Replicator {
	t.Helper()
	r, err := New(Config{
		Source:            source,
		Target:            target,
		LeaseTTLMargin:    time.Minute,
		ReconcileInterval: 100 * time.Millisecond,
		RetryInterval:     50 * time.Millisecond,
		Log:               quietLog(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// run starts rep in the background. stop cancels it and returns Run's
// result; done is closed once Run has returned, with the result in err.
type running struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func (r *running) stop() error {
	r.cancel()
	<-r.done
	return r.err
}

func run(t *testing.T, rep *Replicator) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{cancel: cancel, done: make(chan struct{})}
	go func() {
		r.err = rep.Run(ctx)
		close(r.done)
	}()
	t.Cleanup(func() { _ = r.stop() })
	return r
}

// waitApplied waits until the target has caught up with the source.
func waitApplied(t *testing.T, r *Replicator, source *clientv3.Client) {
	t.Helper()
	ctx := testCtx(t)
	resp, err := source.Get(ctx, "/")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for r.Applied() < resp.Header.Revision {
		if time.Now().After(deadline) {
			t.Fatalf("target at revision %d, source at %d", r.Applied(), resp.Header.Revision)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func kvString(kv *mvccpb.KeyValue) string {
	return fmt.Sprintf("%s=%q c=%d m=%d v=%d l=%x", kv.Key, kv.Value, kv.CreateRevision, kv.ModRevision, kv.Version, kv.Lease)
}

// assertReplicated checks that the target holds exactly the source's keys
// with the same metadata, at the same revision.
func assertReplicated(t *testing.T, source, target *clientv3.Client) {
	t.Helper()
	ctx := testCtx(t)
	src, err := source.Get(ctx, "", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	dst, err := target.Get(ctx, "", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if src.Header.Revision != dst.Header.Revision {
		t.Fatalf("revision: source %d, target %d", src.Header.Revision, dst.Header.Revision)
	}
	var want, got []string
	for _, kv := range src.Kvs {
		want = append(want, kvString(kv))
	}
	for _, kv := range dst.Kvs {
		if !strings.HasPrefix(string(kv.Key), DefaultStatePrefix) {
			got = append(got, kvString(kv))
		}
	}
	if strings.Join(want, "\n") != strings.Join(got, "\n") {
		t.Fatalf("target differs from source:\nsource:\n%s\ntarget:\n%s", strings.Join(want, "\n"), strings.Join(got, "\n"))
	}
}

func mustHalt(t *testing.T, r *running, contains string) {
	t.Helper()
	select {
	case <-r.done:
		var h *HaltError
		if !errors.As(r.err, &h) || !strings.Contains(r.err.Error(), contains) {
			t.Fatalf("Run = %v, want a HaltError mentioning %q", r.err, contains)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("replicator did not halt")
	}
}

// workload writes a mix of operations to the source.
func workload(t *testing.T, cli *clientv3.Client, tag string) {
	t.Helper()
	ctx := testCtx(t)
	lease, err := cli.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	short, err := cli.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	steps := []func() error{
		func() error { _, err := cli.Put(ctx, "/"+tag+"/a", "1"); return err },
		func() error { _, err := cli.Put(ctx, "/"+tag+"/a", "2"); return err },
		func() error { _, err := cli.Put(ctx, "/"+tag+"/leased", "x", clientv3.WithLease(lease.ID)); return err },
		func() error { _, err := cli.Put(ctx, "/"+tag+"/short", "x", clientv3.WithLease(short.ID)); return err },
		func() error {
			_, err := cli.Txn(ctx).If(clientv3.Compare(clientv3.Version("/"+tag+"/a"), "=", 2)).
				Then(clientv3.OpPut("/"+tag+"/b", "1"), clientv3.OpPut("/"+tag+"/c", "1"), clientv3.OpDelete("/"+tag+"/a")).Commit()
			return err
		},
		func() error { _, err := cli.KeepAliveOnce(ctx, lease.ID); return err },
		func() error { _, err := cli.Revoke(ctx, short.ID); return err },
		func() error { _, err := cli.Delete(ctx, "/"+tag+"/", clientv3.WithPrefix()); return err },
		func() error { _, err := cli.Put(ctx, "/"+tag+"/d", "1", clientv3.WithLease(lease.ID)); return err },
	}
	for i, step := range steps {
		if err := step(); err != nil {
			t.Fatalf("workload step %d: %v", i, err)
		}
	}
}

func TestReplicateGreenfield(t *testing.T) {
	source, target := startT4(t, false), startT4(t, false)
	// History written before the replicator starts is replayed too.
	workload(t, source, "before")

	r := newReplicator(t, source, target)
	run(t, r)
	workload(t, source, "during")
	waitApplied(t, r, source)
	assertReplicated(t, source, target)

	// Leases revoked on the source disappear from the target once their
	// keys are gone; live ones are mirrored with the same ID.
	ctx := testCtx(t)
	deadline := time.Now().Add(5 * time.Second)
	for {
		src, err := source.Leases(ctx)
		if err != nil {
			t.Fatal(err)
		}
		dst, err := target.Leases(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(src.Leases) == fmt.Sprint(dst.Leases) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leases: source %v, target %v", src.Leases, dst.Leases)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestReplicateResumesFromCursor(t *testing.T) {
	source, target := startT4(t, false), startT4(t, false)
	r := newReplicator(t, source, target)
	first := run(t, r)
	workload(t, source, "first")
	waitApplied(t, r, source)
	if err := first.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run after stop = %v", err)
	}

	workload(t, source, "offline")
	r2 := newReplicator(t, source, target)
	run(t, r2)
	workload(t, source, "second")
	waitApplied(t, r2, source)
	assertReplicated(t, source, target)
}

func TestReplicateHaltsOnStrayTargetWrite(t *testing.T) {
	source, target := startT4(t, false), startT4(t, false)
	r := newReplicator(t, source, target)
	running := run(t, r)
	workload(t, source, "w")
	waitApplied(t, r, source)

	ctx := testCtx(t)
	if _, err := target.Put(ctx, "/stray", "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Put(ctx, "/next", "x"); err != nil {
		t.Fatal(err)
	}
	mustHalt(t, running, "diverged")
}

func TestReplicateTwoReplicatorsDoNotInterleave(t *testing.T) {
	source, target := startT4(t, false), startT4(t, false)
	a, b := newReplicator(t, source, target), newReplicator(t, source, target)
	runA, runB := run(t, a), run(t, b)
	for i := 0; i < 50; i++ {
		if _, err := source.Put(testCtx(t), fmt.Sprintf("/k/%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	// Whichever loses a race halts; the target stays consistent.
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := source.Get(testCtx(t), "/")
		if err != nil {
			t.Fatal(err)
		}
		if a.Applied() >= resp.Header.Revision || b.Applied() >= resp.Header.Revision {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("neither replicator caught up")
		}
		time.Sleep(10 * time.Millisecond)
	}
	assertReplicated(t, source, target)
	for _, r := range []*running{runA, runB} {
		select {
		case <-r.done:
			var h *HaltError
			if !errors.As(r.err, &h) {
				t.Fatalf("replicator stopped with %v, want a HaltError", r.err)
			}
		default:
		}
	}
}

func TestReplicateRefusesTargetWithoutCursor(t *testing.T) {
	source, target := startT4(t, false), startT4(t, false)
	if _, err := target.Put(testCtx(t), "/existing", "x"); err != nil {
		t.Fatal(err)
	}
	mustHalt(t, run(t, newReplicator(t, source, target)), "no replication cursor")
}

// A database created before the meta keyspace spends revisions on lease
// bookkeeping that its watch stream never shows.
func TestReplicateHaltsOnRevisionGap(t *testing.T) {
	source, target := startT4(t, true), startT4(t, false)
	ctx := testCtx(t)
	if _, err := source.Put(ctx, "/a", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Grant(ctx, 60); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Put(ctx, "/b", "1"); err != nil {
		t.Fatal(err)
	}
	mustHalt(t, run(t, newReplicator(t, source, target)), "produced no events")
}

func TestReplicateHaltsOnCompactedSource(t *testing.T) {
	source, target := startT4(t, false), startT4(t, false)
	ctx := testCtx(t)
	var last int64
	for i := 0; i < 5; i++ {
		resp, err := source.Put(ctx, "/k", fmt.Sprint(i))
		if err != nil {
			t.Fatal(err)
		}
		last = resp.Header.Revision
	}
	if _, err := source.Compact(ctx, last); err != nil {
		t.Fatal(err)
	}
	mustHalt(t, run(t, newReplicator(t, source, target)), "bootstrapped again")
}

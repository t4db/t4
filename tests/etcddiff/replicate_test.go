package etcddiff

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	buildOnce sync.Once
	buildDir  string // removed by TestMain
	t4Binary  string
	buildErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if buildDir != "" {
		_ = os.RemoveAll(buildDir)
	}
	os.Exit(code)
}

// buildT4 builds the t4 binary from this repository once per test run.
func buildT4(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := filepath.Abs(filepath.Join("..", ".."))
		if err != nil {
			buildErr = err
			return
		}
		if buildDir, err = os.MkdirTemp("", "t4-etcddiff-"); err != nil {
			buildErr = err
			return
		}
		t4Binary = filepath.Join(buildDir, "t4")
		cmd := exec.Command("go", "build", "-o", t4Binary, "./cmd/t4")
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return t4Binary
}

// startReplicator runs `t4 replicate run` from source to target and returns
// its combined output.
func startReplicator(t *testing.T, source, target string) *bytes.Buffer {
	t.Helper()
	var out bytes.Buffer
	cmd := exec.Command(buildT4(t), "replicate", "run",
		"--source-endpoints", source,
		"--target-endpoints", target,
		"--lease-reconcile-interval", "200ms",
		"--metrics-addr", "",
		"--log-level", "warn")
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGINT)
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("replicator output:\n%s", out.String())
		}
	})
	return &out
}

// waitReplicated waits until the target's cursor reaches the source revision.
func waitReplicated(t *testing.T, ctx context.Context, source, target *clientv3.Client) {
	t.Helper()
	src, err := source.Get(ctx, "/")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		cur, err := target.Get(ctx, "/__t4_replication/cursor")
		if err == nil && len(cur.Kvs) == 1 {
			if rev, _ := strconv.ParseInt(string(cur.Kvs[0].Value), 10, 64); rev >= src.Header.Revision {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("target did not reach source revision %d (cursor %v, err %v)", src.Header.Revision, cur, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func stateOf(t *testing.T, ctx context.Context, cli *clientv3.Client) (int64, string) {
	t.Helper()
	resp, err := cli.Get(ctx, "", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	var kvs []string
	for _, kv := range resp.Kvs {
		if !strings.HasPrefix(string(kv.Key), "/__t4_replication/") {
			kvs = append(kvs, kvString(kv))
		}
	}
	return resp.Header.Revision, strings.Join(kvs, "\n")
}

func leaseIDs(t *testing.T, ctx context.Context, cli *clientv3.Client) string {
	t.Helper()
	resp, err := cli.Leases(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(resp.Leases))
	for i, l := range resp.Leases {
		ids[i] = fmt.Sprintf("%x", l.ID)
	}
	return strings.Join(ids, ",")
}

// TestReplicateT4ToEtcd drives a T4 source and a reference etcd with the same
// random workload while `t4 replicate run` copies the T4 source into a second
// etcd. All three must end at the same revision with identical keys and
// metadata, and the replica must mirror the source's leases.
func TestReplicateT4ToEtcd(t *testing.T) {
	s := seed(t)
	t.Logf("seed %d (rerun with ETCDDIFF_SEED=%d)", s, s)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sourceAddr, replicaAddr := startT4(t), startEtcd(t)
	p := &pair{t: t, ctx: ctx, etcd: dial(t, startEtcd(t)), t4: dial(t, sourceAddr)}
	replica := dial(t, replicaAddr)
	w := &workload{pair: p, rnd: rand.New(rand.NewSource(s)), nextID: 0x1000, noCompact: true}

	// Half the history exists before the replicator starts.
	for p.step = 1; p.step <= 150; p.step++ {
		w.step()
	}
	startReplicator(t, sourceAddr, replicaAddr)
	for ; p.step <= 400; p.step++ {
		w.step()
	}
	waitReplicated(t, ctx, p.t4, replica)

	refRev, refState := stateOf(t, ctx, p.etcd)
	srcRev, srcState := stateOf(t, ctx, p.t4)
	repRev, repState := stateOf(t, ctx, replica)
	if refRev != srcRev || srcRev != repRev {
		t.Fatalf("revisions: reference etcd %d, T4 source %d, replica %d", refRev, srcRev, repRev)
	}
	if srcState != repState {
		t.Fatalf("replica differs from source:\nsource:\n%s\nreplica:\n%s", srcState, repState)
	}
	if refState != srcState {
		t.Fatalf("source differs from reference etcd:\nreference:\n%s\nsource:\n%s", refState, srcState)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		src, rep := leaseIDs(t, ctx, p.t4), leaseIDs(t, ctx, replica)
		if src == rep {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leases: source %s, replica %s", src, rep)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestReplicateLeaseExpiry: when a lease expires on the source, its keys'
// deletions reach the replica at the same revision, and the replica's copy of
// the lease, which has a longer TTL, is revoked once its keys are gone.
func TestReplicateLeaseExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sourceAddr, replicaAddr := startT4(t), startEtcd(t)
	source, replica := dial(t, sourceAddr), dial(t, replicaAddr)
	startReplicator(t, sourceAddr, replicaAddr)

	lease, err := source.Grant(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"/leased/a", "/leased/b"} {
		if _, err := source.Put(ctx, k, "v", clientv3.WithLease(lease.ID)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.Put(ctx, "/plain", "v"); err != nil {
		t.Fatal(err)
	}
	waitReplicated(t, ctx, source, replica)
	ttl, err := replica.TimeToLive(ctx, lease.ID)
	if err != nil || ttl.GrantedTTL <= 3 {
		t.Fatalf("replica lease: %+v, %v; want the source lease with a longer TTL", ttl, err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		resp, err := source.Get(ctx, "/leased/", clientv3.WithPrefix(), clientv3.WithCountOnly())
		if err == nil && resp.Count == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("source lease did not expire")
		}
		time.Sleep(200 * time.Millisecond)
	}
	waitReplicated(t, ctx, source, replica)
	srcRev, srcState := stateOf(t, ctx, source)
	repRev, repState := stateOf(t, ctx, replica)
	if srcRev != repRev || srcState != repState {
		t.Fatalf("after expiry: source rev %d\n%s\nreplica rev %d\n%s", srcRev, srcState, repRev, repState)
	}
	deadline = time.Now().Add(5 * time.Second)
	for leaseIDs(t, ctx, replica) != "" {
		if time.Now().After(deadline) {
			t.Fatalf("replica still has leases %s after the source's expired", leaseIDs(t, ctx, replica))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

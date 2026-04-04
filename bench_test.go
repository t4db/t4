package strata_test

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strata-db/strata"
	"github.com/strata-db/strata/pkg/object"
)

func openBenchNode(b *testing.B) *strata.Node {
	b.Helper()
	n, err := strata.Open(strata.Config{DataDir: b.TempDir()})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { n.Close() })
	return n
}

func BenchmarkPut(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.Put(ctx, fmt.Sprintf("/bench/put/%d", i), []byte("value"), 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutSameKey(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.Put(ctx, "/bench/same", []byte("value"), 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	if _, err := n.Put(ctx, "/bench/get", []byte("value"), 0); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.Get("/bench/get"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCreate(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.Create(ctx, fmt.Sprintf("/bench/create/%d", i), []byte("v"), 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdate(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	rev, err := n.Put(ctx, "/bench/update", []byte("v0"), 0)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newRev, _, _, err := n.Update(ctx, "/bench/update", []byte("v"), rev, 0)
		if err != nil {
			b.Fatal(err)
		}
		rev = newRev
	}
}

func BenchmarkDelete(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		key := fmt.Sprintf("/bench/del/%d", i)
		if _, err := n.Put(ctx, key, []byte("v"), 0); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := n.Delete(ctx, key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkList(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if _, err := n.Put(ctx, fmt.Sprintf("/bench/list/%04d", i), []byte("v"), 0); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.List("/bench/list/"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutParallel measures Put throughput with concurrent writers.
// Group-commit batches concurrent writes into a single WAL fsync, so this
// benchmark is where the Option-A improvement shows up.
func BenchmarkPutParallel(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	var counter atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			if _, err := n.Put(ctx, fmt.Sprintf("/bench/par/%d", i), []byte("value"), 0); err != nil {
				b.Error(err)
			}
		}
	})
}

// BenchmarkPutParallelSingleProc verifies that group-commit batching works even
// without true CPU parallelism. It pins GOMAXPROCS=1 so goroutines are
// cooperatively scheduled, but still spawns 16 concurrent writers. Because each
// writer unlocks n.mu before blocking on the done channel, all 16 can queue
// their requests before the commit loop drains writeC — producing batches of
// ~16 writes per fsync even on one OS thread.
func BenchmarkPutParallelSingleProc(b *testing.B) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)

	n := openBenchNode(b)
	ctx := context.Background()

	var writers = 16 * prev
	var (
		counter atomic.Int64
		wg      sync.WaitGroup
		work    = make(chan struct{}, b.N)
	)
	for i := 0; i < b.N; i++ {
		work <- struct{}{}
	}
	close(work)

	b.ResetTimer()
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func() {
			defer wg.Done()
			for range work {
				i := counter.Add(1)
				if _, err := n.Put(ctx, fmt.Sprintf("/bench/singleproc/%d", i), []byte("value"), 0); err != nil {
					b.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}

// BenchmarkPutParallelScaled runs 16*GOMAXPROCS concurrent writers so the
// writer-to-CPU ratio stays constant across -cpu= values. Shows how group-commit
// throughput scales as both CPU count and concurrency grow together.
func BenchmarkPutParallelScaled(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()

	writers := 16 * runtime.GOMAXPROCS(0)
	var (
		counter atomic.Int64
		wg      sync.WaitGroup
		work    = make(chan struct{}, b.N)
	)
	for i := 0; i < b.N; i++ {
		work <- struct{}{}
	}
	close(work)

	b.ResetTimer()
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func() {
			defer wg.Done()
			for range work {
				i := counter.Add(1)
				if _, err := n.Put(ctx, fmt.Sprintf("/bench/scaled/%d", i), []byte("value"), 0); err != nil {
					b.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkWatch(b *testing.B) {
	n := openBenchNode(b)
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	ch, err := n.Watch(ctx, "/bench/watch/", 0)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n.Put(ctx, fmt.Sprintf("/bench/watch/%d", i), []byte("v"), 0)
		<-ch
	}
}

// freeBenchAddr allocates a free TCP port for benchmark use.
func freeBenchAddr(b *testing.B) string {
	b.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("freeBenchAddr: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

// BenchmarkGetSerializable is the baseline: single-node local read, no sync RPC.
func BenchmarkGetSerializable(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	if _, err := n.Put(ctx, "/bench/get", []byte("value"), 0); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.Get("/bench/get"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetLinearizableLeader measures LinearizableGet on the leader (sync
// is a no-op) to confirm zero overhead over a plain Get.
func BenchmarkGetLinearizableLeader(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	if _, err := n.Put(ctx, "/bench/get", []byte("value"), 0); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.LinearizableGet(ctx, "/bench/get"); err != nil {
			b.Fatal(err)
		}
	}
}

// openBenchCluster starts a 3-node cluster using an in-memory object store and
// returns the leader node. All nodes are registered for cleanup.
func openBenchCluster(b *testing.B) *strata.Node {
	b.Helper()
	store := object.NewMem()
	ctx := context.Background()

	var nodes [3]*strata.Node
	for i := 0; i < 3; i++ {
		addr := freeBenchAddr(b)
		n, err := strata.Open(strata.Config{
			DataDir:           b.TempDir(),
			ObjectStore:       store,
			NodeID:            fmt.Sprintf("bench-%d", i),
			PeerListenAddr:    addr,
			AdvertisePeerAddr: addr,
		})
		if err != nil {
			b.Fatalf("open node %d: %v", i, err)
		}
		nodes[i] = n
		b.Cleanup(func() { n.Close() })
	}

	// Wait for a leader and all followers to be connected.
	deadline := time.Now().Add(15 * time.Second)
	var leader *strata.Node
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				leader = n
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if leader == nil {
		b.Fatal("no leader elected within 15s")
	}
	// Seed one write so followers are connected and ACK-capable.
	if _, err := leader.Put(ctx, "/bench/seed", []byte("v"), 0); err != nil {
		b.Fatalf("seed write: %v", err)
	}
	// Give followers time to connect and ACK the seed entry.
	time.Sleep(200 * time.Millisecond)
	return leader
}

// BenchmarkPutCluster measures serial Put throughput on a 3-node cluster
// (localhost). Each write must be ACKed by all followers before returning.
func BenchmarkPutCluster(b *testing.B) {
	leader := openBenchCluster(b)
	ctx := context.Background()
	var counter atomic.Int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := counter.Add(1)
		if _, err := leader.Put(ctx, fmt.Sprintf("/bench/cluster/put/%d", k), []byte("value"), 0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutParallelCluster measures parallel Put throughput on a 3-node
// cluster. Group-commit batches concurrent writes into a single WAL fsync +
// quorum ACK round, so throughput scales with concurrency.
func BenchmarkPutParallelCluster(b *testing.B) {
	leader := openBenchCluster(b)
	ctx := context.Background()
	var counter atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k := counter.Add(1)
			if _, err := leader.Put(ctx, fmt.Sprintf("/bench/cluster/par/%d", k), []byte("value"), 0); err != nil {
				b.Error(err)
			}
		}
	})
}

// BenchmarkGetLinearizableFollower measures the full ReadIndex cost on a
// follower: ForwardGetRevision RPC to leader + local Pebble read.
// This is the realistic overhead for linearizable reads in a multi-node cluster.
func BenchmarkGetLinearizableFollower(b *testing.B) {
	store := object.NewMem()
	ctx := context.Background()

	leaderAddr := freeBenchAddr(b)
	leader, err := strata.Open(strata.Config{
		DataDir:        b.TempDir(),
		ObjectStore:    store,
		NodeID:         "bench-leader",
		PeerListenAddr: leaderAddr,
	})
	if err != nil {
		b.Fatalf("open leader: %v", err)
	}
	b.Cleanup(func() { leader.Close() })

	follower, err := strata.Open(strata.Config{
		DataDir:        b.TempDir(),
		ObjectStore:    store,
		NodeID:         "bench-follower",
		PeerListenAddr: freeBenchAddr(b),
	})
	if err != nil {
		b.Fatalf("open follower: %v", err)
	}
	b.Cleanup(func() { follower.Close() })

	// wait for leader election
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if leader.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !leader.IsLeader() {
		b.Fatal("leader not elected within 10s")
	}

	rev, err := leader.Put(ctx, "/bench/get", []byte("value"), 0)
	if err != nil {
		b.Fatalf("leader Put: %v", err)
	}
	if err := follower.WaitForRevision(ctx, rev); err != nil {
		b.Fatalf("follower catch-up: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := follower.LinearizableGet(ctx, "/bench/get"); err != nil {
			b.Fatal(err)
		}
	}
}

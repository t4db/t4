package t4

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/t4db/t4/pkg/object"
)

// A range delete expands on the leader into one sub-op per key, so its WAL
// entry can exceed gRPC's default 4 MiB message; followers must still
// receive it.
func TestFollowerReceivesTxnOverDefaultMessageLimit(t *testing.T) {
	shared := object.NewMem()
	var nodes []*Node
	for i := range 2 {
		listen := freeAddrLocal(t)
		n, err := Open(Config{DataDir: t.TempDir(), ObjectStore: shared, NodeID: fmt.Sprintf("big-%d", i),
			PeerListenAddr: listen, AdvertisePeerAddr: listen})
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, n)
		t.Cleanup(func() { _ = n.Close() })
	}
	leader := waitForLeaderNodeLocal(t, nodes, 10*time.Second)
	follower := nodes[0]
	if follower == leader {
		follower = nodes[1]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 40k sub-ops of ~145 bytes each: about 6 MB, 8 MB as JSON.
	pad := strings.Repeat("x", 100)
	const n = 40000
	var ops []TxnOp
	for i := range n {
		ops = append(ops, TxnOp{Type: TxnPut, Key: fmt.Sprintf("/big/%s/%08d", pad, i)})
		if len(ops) == 5000 {
			if _, err := leader.Txn(ctx, TxnRequest{Success: ops}); err != nil {
				t.Fatal(err)
			}
			ops = nil
		}
	}
	resp, err := leader.Txn(ctx, TxnRequest{Success: []TxnOp{{Type: TxnDelete, Key: "/big/", RangeEnd: "/big0"}}})
	if err != nil {
		t.Fatal(err)
	}
	wctx, wcancel := context.WithTimeout(ctx, 20*time.Second)
	defer wcancel()
	if err := follower.WaitForRevision(wctx, resp.Revision); err != nil {
		t.Fatalf("follower did not apply the range delete: %v", err)
	}
	if kvs, err := follower.List("/big/"); err != nil || len(kvs) != 0 {
		t.Fatalf("follower still lists %d keys (err %v)", len(kvs), err)
	}
}

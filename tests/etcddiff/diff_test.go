// Package etcddiff runs the same workload against a real embedded etcd and
// against T4's etcd adapter and requires identical results: the same header
// revision after every operation, the same responses, the same key metadata
// (create/mod revision, version, lease) and the same watch history.
//
// Revision-exact replication between T4 and etcd depends on this: a
// replicator can only map T4 revision R to etcd revision R if both spend
// revisions on exactly the same operations.
//
// It lives in its own module so that etcd's server does not become a
// dependency of T4 itself.
package etcddiff

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/etcd/auth"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func startEtcd(t *testing.T) string {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	client := url.URL{Scheme: "http", Host: freePort(t)}
	peer := url.URL{Scheme: "http", Host: freePort(t)}
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{client}, []url.URL{client}
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{peer}, []url.URL{peer}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("start etcd: %v", err)
	}
	t.Cleanup(e.Close)
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		t.Fatal("etcd not ready")
	}
	return client.Host
}

func startT4(t *testing.T) string {
	t.Helper()
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("t4.Open: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })
	authStore, err := auth.NewStore(node)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tokens := auth.NewTokenStore(ctx, time.Hour, node)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(t4etcd.NewServerOptions(nil, nil)...)
	t4etcd.New(node, authStore, tokens).Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func dial(t *testing.T, endpoint string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// pair runs every operation against both backends.
type pair struct {
	t          *testing.T
	ctx        context.Context
	etcd, t4   *clientv3.Client
	step       int
	desc       string
	mismatches int
	lastRev    int64 // latest etcd header revision seen
}

func (p *pair) failf(format string, args ...any) {
	p.t.Helper()
	p.mismatches++
	p.t.Errorf("step %d (%s): "+format, append([]any{p.step, p.desc}, args...)...)
	if p.mismatches >= 40 {
		p.t.FailNow()
	}
}

func (p *pair) compareErr(eErr, tErr error) bool {
	p.t.Helper()
	if (eErr == nil) != (tErr == nil) {
		p.failf("error mismatch: etcd=%v t4=%v", eErr, tErr)
		return false
	}
	return eErr == nil
}

func (p *pair) compareHeader(e, t *etcdserverpb.ResponseHeader) {
	p.t.Helper()
	if e == nil || t == nil {
		if e != t {
			p.failf("response header: etcd=%v t4=%v", e, t)
		}
		return
	}
	if e.Revision != t.Revision {
		p.failf("header revision: etcd=%d t4=%d", e.Revision, t.Revision)
	}
	if e.Revision > p.lastRev {
		p.lastRev = e.Revision
	}
}

func kvString(kv *mvccpb.KeyValue) string {
	if kv == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s=%q c=%d m=%d v=%d l=%x", kv.Key, kv.Value, kv.CreateRevision, kv.ModRevision, kv.Version, kv.Lease)
}

func (p *pair) compareKVs(what string, e, t []*mvccpb.KeyValue) {
	p.t.Helper()
	if len(e) != len(t) {
		p.failf("%s: etcd has %d kvs, t4 has %d\n etcd=%v\n t4=%v", what, len(e), len(t), kvStrings(e), kvStrings(t))
		return
	}
	for i := range e {
		if kvString(e[i]) != kvString(t[i]) {
			p.failf("%s[%d]:\n etcd=%s\n t4=  %s", what, i, kvString(e[i]), kvString(t[i]))
		}
	}
}

func kvStrings(kvs []*mvccpb.KeyValue) []string {
	out := make([]string, len(kvs))
	for i, kv := range kvs {
		out[i] = kvString(kv)
	}
	return out
}

func (p *pair) put(key, val string, opts ...clientv3.OpOption) {
	p.t.Helper()
	e, eErr := p.etcd.Put(p.ctx, key, val, opts...)
	t, tErr := p.t4.Put(p.ctx, key, val, opts...)
	if p.compareErr(eErr, tErr) {
		p.compareHeader(e.Header, t.Header)
		p.compareKVs("prev kv", nilSafe(e.PrevKv), nilSafe(t.PrevKv))
	}
}

func nilSafe(kv *mvccpb.KeyValue) []*mvccpb.KeyValue {
	if kv == nil {
		return nil
	}
	return []*mvccpb.KeyValue{kv}
}

func (p *pair) del(key string, opts ...clientv3.OpOption) {
	p.t.Helper()
	e, eErr := p.etcd.Delete(p.ctx, key, opts...)
	t, tErr := p.t4.Delete(p.ctx, key, opts...)
	if p.compareErr(eErr, tErr) {
		p.compareHeader(e.Header, t.Header)
		if e.Deleted != t.Deleted {
			p.failf("deleted: etcd=%d t4=%d", e.Deleted, t.Deleted)
		}
		p.compareKVs("prev kvs", e.PrevKvs, t.PrevKvs)
	}
}

func (p *pair) get(key string, opts ...clientv3.OpOption) {
	p.t.Helper()
	e, eErr := p.etcd.Get(p.ctx, key, opts...)
	t, tErr := p.t4.Get(p.ctx, key, opts...)
	if p.compareErr(eErr, tErr) {
		p.compareHeader(e.Header, t.Header)
		if e.Count != t.Count || e.More != t.More {
			p.failf("count/more: etcd=%d/%v t4=%d/%v", e.Count, e.More, t.Count, t.More)
		}
		p.compareKVs("kvs", e.Kvs, t.Kvs)
	}
}

func (p *pair) txn(cmps []clientv3.Cmp, then, els []clientv3.Op) {
	p.t.Helper()
	e, eErr := p.etcd.Txn(p.ctx).If(cmps...).Then(then...).Else(els...).Commit()
	t, tErr := p.t4.Txn(p.ctx).If(cmps...).Then(then...).Else(els...).Commit()
	if p.compareErr(eErr, tErr) {
		p.compareHeader(e.Header, t.Header)
		if e.Succeeded != t.Succeeded {
			p.failf("succeeded: etcd=%v t4=%v", e.Succeeded, t.Succeeded)
		}
	}
}

func (p *pair) state() {
	p.t.Helper()
	p.get("", clientv3.WithPrefix())
}

// workload drives both backends with the same random operations.
type workload struct {
	*pair
	rnd       *rand.Rand
	leases    []clientv3.LeaseID
	nextID    clientv3.LeaseID
	compact   int64 // latest compaction revision
	noCompact bool  // skip compaction (it may overtake a replicator)
}

func (w *workload) key() string { return "/k/" + strconv.Itoa(w.rnd.Intn(16)) }
func (w *workload) val() string { return "v" + strconv.Itoa(w.rnd.Intn(1000)) }

func (w *workload) lease() (clientv3.LeaseID, bool) {
	if len(w.leases) == 0 {
		return 0, false
	}
	return w.leases[w.rnd.Intn(len(w.leases))], true
}

func (w *workload) dropLease(id clientv3.LeaseID) {
	for i, l := range w.leases {
		if l == id {
			w.leases = append(w.leases[:i], w.leases[i+1:]...)
			return
		}
	}
}

// randomRev returns a compare operand, favouring the edges of the revision
// space (below zero, absent, the empty store, the first write).
func (w *workload) randomRev() int64 {
	if w.rnd.Intn(2) == 0 {
		return int64(w.rnd.Intn(4)) - 1
	}
	return int64(w.rnd.Intn(int(w.lastRev) + 5))
}

func (w *workload) randomCmp(key string) (clientv3.Cmp, string) {
	op := []string{"=", "!=", "<", ">"}[w.rnd.Intn(4)]
	switch w.rnd.Intn(5) {
	case 0:
		v := w.rnd.Intn(4)
		return clientv3.Compare(clientv3.Version(key), op, v), fmt.Sprintf("version(%s) %s %d", key, op, v)
	case 1:
		v := w.randomRev()
		return clientv3.Compare(clientv3.CreateRevision(key), op, v), fmt.Sprintf("create(%s) %s %d", key, op, v)
	case 2:
		v := w.randomRev()
		return clientv3.Compare(clientv3.ModRevision(key), op, v), fmt.Sprintf("mod(%s) %s %d", key, op, v)
	case 3:
		id, _ := w.lease()
		return clientv3.Compare(clientv3.LeaseValue(key), "=", id), fmt.Sprintf("lease(%s) = %x", key, id)
	default:
		v := w.val()
		return clientv3.Compare(clientv3.Value(key), "=", v), fmt.Sprintf("value(%s) = %s", key, v)
	}
}

func (w *workload) step() {
	switch op := w.rnd.Intn(19); op {
	case 0, 1:
		w.desc = "put"
		w.put(w.key(), w.val())
	case 2:
		w.desc = "put prev kv"
		w.put(w.key(), w.val(), clientv3.WithPrevKV())
	case 3:
		w.desc = "delete prev kv"
		w.del(w.key(), clientv3.WithPrevKV())
	case 4:
		w.desc = "delete prefix"
		w.del("/k/1", clientv3.WithPrefix(), clientv3.WithPrevKV())
	case 5:
		k := w.key()
		cmp, desc := w.randomCmp(k)
		w.desc = "txn compare " + desc
		w.txn([]clientv3.Cmp{cmp},
			[]clientv3.Op{clientv3.OpPut(k, w.val())},
			[]clientv3.Op{clientv3.OpDelete(w.key())})
	case 6:
		w.desc = "txn multi put"
		a, b := w.key(), w.key()
		if a == b {
			return
		}
		w.txn(nil, []clientv3.Op{clientv3.OpPut(a, w.val()), clientv3.OpPut(b, w.val()), clientv3.OpDelete(w.key() + "/missing")}, nil)
	case 7:
		w.desc = "lease grant"
		w.nextID++
		w.grant(w.nextID)
	case 8:
		w.desc = "put with lease"
		if id, ok := w.lease(); ok {
			w.put(w.key(), w.val(), clientv3.WithLease(id))
		}
	case 9:
		w.desc = "keepalive"
		if id, ok := w.lease(); ok {
			w.keepAlive(id)
		}
	case 10:
		w.desc = "revoke"
		if id, ok := w.lease(); ok {
			w.revoke(id)
		}
	case 11:
		w.desc = "get range"
		w.get("/k/", clientv3.WithPrefix(), clientv3.WithLimit(int64(1+w.rnd.Intn(5))))
	case 12:
		w.desc = "delete missing"
		w.del("/missing/" + strconv.Itoa(w.rnd.Intn(5)))
	case 13:
		w.desc = "txn no-op"
		w.txn([]clientv3.Cmp{clientv3.Compare(clientv3.Version("/never"), ">", 0)}, []clientv3.Op{clientv3.OpPut("/never", "x")}, nil)
	case 14:
		w.desc = "auth user add"
		w.authUserAdd("user-" + strconv.Itoa(w.pair.step))
	case 15:
		rev := w.compact + 1 + int64(w.rnd.Intn(int(w.lastRev-w.compact)+1))
		w.desc = fmt.Sprintf("historical get rev=%d", rev)
		w.get("/k/", clientv3.WithPrefix(), clientv3.WithRev(rev))
	case 16:
		w.desc = "count only"
		w.get("/k/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	case 17:
		w.desc = "keys only from key"
		w.get(w.key(), clientv3.WithFromKey(), clientv3.WithKeysOnly())
	case 18:
		if w.noCompact || w.rnd.Intn(4) != 0 || w.lastRev-w.compact < 10 {
			return
		}
		rev := w.lastRev - 5
		w.desc = fmt.Sprintf("compact rev=%d", rev)
		w.compactTo(rev)
	}
}

func (w *workload) compactTo(rev int64) {
	w.t.Helper()
	e, eErr := w.etcd.Compact(w.ctx, rev)
	t, tErr := w.t4.Compact(w.ctx, rev)
	if w.compareErr(eErr, tErr) {
		w.compareHeader(e.Header, t.Header)
		w.compact = rev
	}
}

func (w *workload) grant(id clientv3.LeaseID) {
	w.t.Helper()
	// clientv3 cannot choose the lease ID; both sides need the same one.
	req := &etcdserverpb.LeaseGrantRequest{ID: int64(id), TTL: 300}
	e, eErr := etcdserverpb.NewLeaseClient(w.etcd.ActiveConnection()).LeaseGrant(w.ctx, req)
	t, tErr := etcdserverpb.NewLeaseClient(w.t4.ActiveConnection()).LeaseGrant(w.ctx, req)
	if w.compareErr(eErr, tErr) {
		w.compareHeader(e.Header, t.Header)
		w.leases = append(w.leases, id)
	}
}

func (w *workload) keepAlive(id clientv3.LeaseID) {
	w.t.Helper()
	e, eErr := w.etcd.KeepAliveOnce(w.ctx, id)
	t, tErr := w.t4.KeepAliveOnce(w.ctx, id)
	if w.compareErr(eErr, tErr) {
		w.compareHeader(e.ResponseHeader, t.ResponseHeader)
	}
}

func (w *workload) revoke(id clientv3.LeaseID) {
	w.t.Helper()
	e, eErr := w.etcd.Revoke(w.ctx, id)
	t, tErr := w.t4.Revoke(w.ctx, id)
	if w.compareErr(eErr, tErr) {
		w.compareHeader(e.Header, t.Header)
	}
	w.dropLease(id)
}

func (w *workload) authUserAdd(name string) {
	w.t.Helper()
	e, eErr := w.etcd.UserAdd(w.ctx, name, "pw")
	t, tErr := w.t4.UserAdd(w.ctx, name, "pw")
	if w.compareErr(eErr, tErr) {
		w.compareHeader(e.Header, t.Header)
	}
}

func seed(t *testing.T) int64 {
	if s := os.Getenv("ETCDDIFF_SEED"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	return time.Now().UnixNano()
}

func collect(ctx context.Context, cli *clientv3.Client) <-chan []*clientv3.Event {
	out := make(chan []*clientv3.Event, 1)
	wch := cli.Watch(ctx, "", clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(1))
	go func() {
		var events []*clientv3.Event
		for resp := range wch {
			events = append(events, resp.Events...)
		}
		out <- events
	}()
	return out
}

func eventString(e *clientv3.Event) string {
	return fmt.Sprintf("%s %s prev=%s", e.Type, kvString(e.Kv), kvString(e.PrevKv))
}

func TestDifferential(t *testing.T) {
	s := seed(t)
	t.Logf("seed %d (rerun with ETCDDIFF_SEED=%d)", s, s)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	p := &pair{t: t, ctx: ctx, etcd: dial(t, startEtcd(t)), t4: dial(t, startT4(t))}
	w := &workload{pair: p, rnd: rand.New(rand.NewSource(s)), nextID: 0x1000}

	p.desc = "initial state"
	p.state()

	watchCtx, stopWatch := context.WithCancel(ctx)
	eWatch, tWatch := collect(watchCtx, p.etcd), collect(watchCtx, p.t4)

	const steps = 400
	for p.step = 1; p.step <= steps; p.step++ {
		w.step()
		if p.step%20 == 0 {
			p.desc = "state"
			p.state()
		}
	}
	p.desc = "final state"
	p.state()

	time.Sleep(500 * time.Millisecond)
	stopWatch()
	eEvents, tEvents := <-eWatch, <-tWatch
	p.desc = "watch history"
	if len(eEvents) != len(tEvents) {
		p.failf("etcd sent %d events, t4 sent %d", len(eEvents), len(tEvents))
	}
	for i := 0; i < len(eEvents) && i < len(tEvents); i++ {
		if a, b := eventString(eEvents[i]), eventString(tEvents[i]); a != b {
			p.failf("event %d:\n etcd=%s\n t4=  %s", i, a, b)
		}
	}
}

// TestLeaseExpiry checks the timer-driven path the random workload does not
// cover: an expiring lease deletes its keys in exactly one revision, and an
// expiring lease without keys consumes none.
func TestLeaseExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := &pair{t: t, ctx: ctx, etcd: dial(t, startEtcd(t)), t4: dial(t, startT4(t))}
	w := &workload{pair: p, rnd: rand.New(rand.NewSource(1))}

	p.desc = "setup"
	p.put("/before", "x")
	const withKeys, noKeys = clientv3.LeaseID(0x2001), clientv3.LeaseID(0x2002)
	for _, id := range []clientv3.LeaseID{withKeys, noKeys} {
		req := &etcdserverpb.LeaseGrantRequest{ID: int64(id), TTL: 3}
		e, eErr := etcdserverpb.NewLeaseClient(p.etcd.ActiveConnection()).LeaseGrant(ctx, req)
		t4r, tErr := etcdserverpb.NewLeaseClient(p.t4.ActiveConnection()).LeaseGrant(ctx, req)
		if p.compareErr(eErr, tErr) {
			p.compareHeader(e.Header, t4r.Header)
		}
	}
	p.put("/leased/a", "1", clientv3.WithLease(withKeys))
	p.put("/leased/b", "2", clientv3.WithLease(withKeys))

	watchCtx, stopWatch := context.WithCancel(ctx)
	eWatch, tWatch := collect(watchCtx, p.etcd), collect(watchCtx, p.t4)

	// Wait until both have expired the leases and deleted the keys.
	deadline := time.Now().Add(20 * time.Second)
	for {
		e, eErr := p.etcd.Get(ctx, "/leased/", clientv3.WithPrefix(), clientv3.WithCountOnly())
		t4r, tErr := p.t4.Get(ctx, "/leased/", clientv3.WithPrefix(), clientv3.WithCountOnly())
		if eErr == nil && tErr == nil && e.Count == 0 && t4r.Count == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leased keys not expired: etcd err=%v t4 err=%v", eErr, tErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Let both finish revoking the key-less lease too.
	time.Sleep(2 * time.Second)

	p.desc = "after expiry"
	p.state()
	w.desc = "write after expiry"
	w.put("/after", "y")

	time.Sleep(500 * time.Millisecond) // let both watches deliver the last event
	stopWatch()
	eEvents, tEvents := <-eWatch, <-tWatch
	p.desc = "watch history"
	if len(eEvents) != len(tEvents) {
		p.failf("etcd sent %d events, t4 sent %d", len(eEvents), len(tEvents))
	}
	for i := 0; i < len(eEvents) && i < len(tEvents); i++ {
		if a, b := eventString(eEvents[i]), eventString(tEvents[i]); a != b {
			p.failf("event %d:\n etcd=%s\n t4=  %s", i, a, b)
		}
	}
}

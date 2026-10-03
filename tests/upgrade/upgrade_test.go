// Package upgrade_test checks that this release serves data written by an
// earlier one. The earlier release's t4 binary writes a varied dataset through
// the etcd API and is stopped; this release's binary then serves the same
// data, and every key, revision, lease and auth entry must read back
// unchanged, history included.
//
// T4_UPGRADE_FROM_BIN names the earlier release's binary; without it the test
// is skipped. T4_UPGRADE_TO_BIN names this release's binary; it is built when
// unset. With T4_UPGRADE_S3=1 and the S3_* variables (as in tests/e2e), the
// earlier release also writes to S3 and this release restores from there into
// an empty data directory.
package upgrade_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4/internal/testutil"
)

func TestUpgrade(t *testing.T) {
	from := os.Getenv("T4_UPGRADE_FROM_BIN")
	if from == "" {
		t.Skip("set T4_UPGRADE_FROM_BIN to an earlier release's t4 binary to run the upgrade test")
	}
	to := os.Getenv("T4_UPGRADE_TO_BIN")
	if to == "" {
		to = buildT4(t)
	}

	// The earlier release's data directory is opened by this release.
	t.Run("data-dir", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "data")
		want := writeWithOld(t, from, dir, nil)
		checkWithNew(t, to, dir, nil, want)
	})

	// This release restores the earlier release's checkpoint and WAL from S3.
	t.Run("object-store", func(t *testing.T) {
		if os.Getenv("T4_UPGRADE_S3") == "" {
			t.Skip("set T4_UPGRADE_S3=1 to run the S3 upgrade test")
		}
		s3 := s3Args(t)
		want := writeWithOld(t, from, filepath.Join(t.TempDir(), "old"), s3)
		checkWithNew(t, to, filepath.Join(t.TempDir(), "new"), s3, want)
	})
}

// written is what the earlier release left behind: its final state, and the
// revisions needed to check history.
type written struct {
	state      state
	compactRev int64 // revision the dataset was compacted at
	revV1      int64 // revision of /upgrade/updated = v1, compacted away
	revV2      int64 // revision of /upgrade/updated = v2, still readable
}

// Auth is enabled as the dataset's last step; from then on clients log in as
// root.
const rootPassword = "root-password"

func writeWithOld(t *testing.T, bin, dataDir string, extra []string) written {
	t.Helper()
	n := startNode(t, bin, dataDir, extra, "")
	w := writeDataset(t, n.client)
	w.state = snapshot(t, n.rootClient(t))
	n.stop(t)
	return w
}

func checkWithNew(t *testing.T, bin, dataDir string, extra []string, want written) {
	t.Helper()
	n := startNode(t, bin, dataDir, extra, rootPassword)
	defer n.stop(t)
	c := n.client
	ctx := context.Background()

	compareStates(t, want.state, snapshot(t, c))

	// Auth stays enabled, with its users and roles in force.
	anon := n.newClient(t, "", "")
	if _, err := anon.Get(ctx, "/upgrade/plain/000"); err == nil {
		t.Error("unauthenticated read succeeded: auth is no longer enabled")
	}
	alice := n.newClient(t, "alice", "alice-password")
	if _, err := alice.Get(ctx, "/upgrade/plain/000"); err != nil {
		t.Errorf("alice cannot read under /upgrade/ (role reader): %v", err)
	}
	if _, err := alice.Put(ctx, "/upgrade/plain/000", "x"); status.Code(err) != codes.PermissionDenied {
		t.Errorf("alice write under /upgrade/: err=%v, want permission denied", err)
	}

	// History survives: the compacted revision stays compacted, later
	// revisions stay readable and watchable.
	if _, err := c.Get(ctx, "/upgrade/updated", clientv3.WithRev(want.revV1)); !errors.Is(err, rpctypes.ErrCompacted) {
		t.Errorf("read at compacted revision %d: err=%v, want %v", want.revV1, err, rpctypes.ErrCompacted)
	}
	if got := getAt(t, c, "/upgrade/updated", want.revV2); got != "v2" {
		t.Errorf("/upgrade/updated at revision %d = %q, want v2", want.revV2, got)
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var seen []string
	for resp := range c.Watch(wctx, "/upgrade/updated", clientv3.WithRev(want.revV2)) {
		if err := resp.Err(); err != nil {
			t.Fatalf("watch from revision %d: %v", want.revV2, err)
		}
		for _, ev := range resp.Events {
			seen = append(seen, string(ev.Kv.Value))
		}
		if len(seen) >= 2 {
			break
		}
	}
	if strings.Join(seen, ",") != "v2,v3" {
		t.Errorf("watch from revision %d saw %v, want [v2 v3]", want.revV2, seen)
	}

	// Writes continue after the earlier release's revisions.
	resp, err := c.Put(ctx, "/upgrade/after", "after")
	if err != nil {
		t.Fatalf("put after upgrade: %v", err)
	}
	if resp.Header.Revision <= want.state.Revision {
		t.Errorf("first write after upgrade got revision %d, want above %d", resp.Header.Revision, want.state.Revision)
	}
}

// writeDataset writes data of every kind the earlier release supports.
func writeDataset(t *testing.T, c *clientv3.Client) written {
	t.Helper()
	ctx := context.Background()
	var w written
	put := func(key, value string, opts ...clientv3.OpOption) int64 {
		t.Helper()
		resp, err := c.Put(ctx, key, value, opts...)
		if err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
		return resp.Header.Revision
	}

	// Many keys of varied sizes.
	for i := range 200 {
		put(fmt.Sprintf("/upgrade/plain/%03d", i), strings.Repeat(string(rune('a'+i%26)), 1+i*37%4000))
	}

	// One key with history, compacted below its second version.
	w.revV1 = put("/upgrade/updated", "v1")
	w.revV2 = put("/upgrade/updated", "v2")
	put("/upgrade/updated", "v3")
	if _, err := c.Compact(ctx, w.revV2); err != nil {
		t.Fatalf("compact at %d: %v", w.revV2, err)
	}
	w.compactRev = w.revV2

	// Deletes, single and ranged.
	put("/upgrade/deleted", "gone")
	if _, err := c.Delete(ctx, "/upgrade/deleted"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	for i := range 3 {
		put(fmt.Sprintf("/upgrade/rangedel/%d", i), "gone")
	}
	if _, err := c.Delete(ctx, "/upgrade/rangedel/", clientv3.WithPrefix()); err != nil {
		t.Fatalf("range delete: %v", err)
	}

	// A transaction taking its success branch.
	txn, err := c.Txn(ctx).
		If(clientv3.Compare(clientv3.Value("/upgrade/updated"), "=", "v3")).
		Then(clientv3.OpPut("/upgrade/txn/then", "then"), clientv3.OpPut("/upgrade/txn/also", "also")).
		Else(clientv3.OpPut("/upgrade/txn/else", "else")).
		Commit()
	if err != nil || !txn.Succeeded {
		t.Fatalf("txn: succeeded=%v err=%v", txn != nil && txn.Succeeded, err)
	}

	// A large value, and a binary key and value.
	put("/upgrade/big", strings.Repeat("0123456789abcdef", 512*1024/16))
	put("/upgrade/binary/\x00\xffé", "\x00\x01\xfe\xff not utf8: \xc3\x28")

	// A lease with keys attached, and one without.
	lease, err := c.Grant(ctx, 3600)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	put("/upgrade/lease/a", "a", clientv3.WithLease(lease.ID))
	put("/upgrade/lease/b", "b", clientv3.WithLease(lease.ID))
	if _, err := c.Grant(ctx, 1800); err != nil {
		t.Fatalf("grant: %v", err)
	}

	// Auth users and roles (auth itself stays disabled).
	if _, err := c.RoleAdd(ctx, "reader"); err != nil {
		t.Fatalf("role add: %v", err)
	}
	if _, err := c.RoleGrantPermission(ctx, "reader", "/upgrade/", clientv3.GetPrefixRangeEnd("/upgrade/"), clientv3.PermissionType(clientv3.PermRead)); err != nil {
		t.Fatalf("role grant: %v", err)
	}
	if _, err := c.UserAdd(ctx, "alice", "alice-password"); err != nil {
		t.Fatalf("user add: %v", err)
	}
	if _, err := c.UserGrantRole(ctx, "alice", "reader"); err != nil {
		t.Fatalf("user grant role: %v", err)
	}

	// Auth enabled, last: every write must come before it.
	if _, err := c.RoleAdd(ctx, "root"); err != nil {
		t.Fatalf("add root role: %v", err)
	}
	if _, err := c.UserAdd(ctx, "root", rootPassword); err != nil {
		t.Fatalf("add root: %v", err)
	}
	if _, err := c.UserGrantRole(ctx, "root", "root"); err != nil {
		t.Fatalf("grant root: %v", err)
	}
	if _, err := c.AuthEnable(ctx); err != nil {
		t.Fatalf("auth enable: %v", err)
	}
	return w
}

// state is everything a client can read back. Revision is compared only for
// moving forward: in databases created before the meta keyspace, T4's own
// bookkeeping (such as the login token for reading the state) consumes
// revisions.
type state struct {
	Revision int64
	KVs      []kvState
	Leases   []leaseState
	Users    []string // "name:role1,role2"
	Roles    []string // "name:perm1;perm2"
}

type kvState struct {
	Key, Value                 string
	CreateRev, ModRev, Version int64
	Lease                      int64
}

type leaseState struct {
	ID, GrantedTTL int64
	Keys           string
}

func snapshot(t *testing.T, c *clientv3.Client) state {
	t.Helper()
	ctx := context.Background()
	var s state

	resp, err := c.Get(ctx, "/upgrade/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("read the dataset: %v", err)
	}
	s.Revision = resp.Header.Revision
	for _, kv := range resp.Kvs {
		s.KVs = append(s.KVs, kvState{
			Key: string(kv.Key), Value: string(kv.Value),
			CreateRev: kv.CreateRevision, ModRev: kv.ModRevision, Version: kv.Version,
			Lease: kv.Lease,
		})
	}

	leases, err := c.Leases(ctx)
	if err != nil {
		t.Fatalf("list leases: %v", err)
	}
	for _, l := range leases.Leases {
		ttl, err := c.TimeToLive(ctx, l.ID, clientv3.WithAttachedKeys())
		if err != nil {
			t.Fatalf("lease %d time to live: %v", l.ID, err)
		}
		if ttl.TTL <= 0 {
			t.Errorf("lease %d has expired (TTL %d)", l.ID, ttl.TTL)
		}
		var keys []string
		for _, k := range ttl.Keys {
			keys = append(keys, string(k))
		}
		sort.Strings(keys)
		s.Leases = append(s.Leases, leaseState{ID: int64(l.ID), GrantedTTL: ttl.GrantedTTL, Keys: strings.Join(keys, ",")})
	}
	sort.Slice(s.Leases, func(i, j int) bool { return s.Leases[i].ID < s.Leases[j].ID })

	users, err := c.UserList(ctx)
	if err != nil {
		t.Fatalf("list users: %v", err)
	}
	for _, name := range users.Users {
		u, err := c.UserGet(ctx, name)
		if err != nil {
			t.Fatalf("get user %q: %v", name, err)
		}
		roles := append([]string(nil), u.Roles...)
		sort.Strings(roles)
		s.Users = append(s.Users, name+":"+strings.Join(roles, ","))
	}
	sort.Strings(s.Users)

	roles, err := c.RoleList(ctx)
	if err != nil {
		t.Fatalf("list roles: %v", err)
	}
	for _, name := range roles.Roles {
		r, err := c.RoleGet(ctx, name)
		if err != nil {
			t.Fatalf("get role %q: %v", name, err)
		}
		var perms []string
		for _, p := range r.Perm {
			perms = append(perms, fmt.Sprintf("%s[%q,%q)", p.PermType, p.Key, p.RangeEnd))
		}
		sort.Strings(perms)
		s.Roles = append(s.Roles, name+":"+strings.Join(perms, ";"))
	}
	sort.Strings(s.Roles)
	return s
}

func compareStates(t *testing.T, want, got state) {
	t.Helper()
	if got.Revision < want.Revision {
		t.Errorf("revision went back: got %d, want at least %d", got.Revision, want.Revision)
	}
	wantKVs := map[string]kvState{}
	for _, kv := range want.KVs {
		wantKVs[kv.Key] = kv
	}
	for _, kv := range got.KVs {
		w, ok := wantKVs[kv.Key]
		switch {
		case !ok:
			t.Errorf("unexpected key %q", kv.Key)
		case w != kv:
			t.Errorf("key %q: got %s, want %s", kv.Key, describe(kv), describe(w))
		}
		delete(wantKVs, kv.Key)
	}
	for key := range wantKVs {
		t.Errorf("missing key %q", key)
	}
	if fmt.Sprint(got.Leases) != fmt.Sprint(want.Leases) {
		t.Errorf("leases: got %v, want %v", got.Leases, want.Leases)
	}
	if fmt.Sprint(got.Users) != fmt.Sprint(want.Users) {
		t.Errorf("users: got %v, want %v", got.Users, want.Users)
	}
	if fmt.Sprint(got.Roles) != fmt.Sprint(want.Roles) {
		t.Errorf("roles: got %v, want %v", got.Roles, want.Roles)
	}
}

func describe(kv kvState) string {
	v := kv.Value
	if len(v) > 32 {
		v = fmt.Sprintf("%s… (%d bytes)", v[:32], len(kv.Value))
	}
	return fmt.Sprintf("{value %q create %d mod %d version %d lease %d}", v, kv.CreateRev, kv.ModRev, kv.Version, kv.Lease)
}

func getAt(t *testing.T, c *clientv3.Client, key string, rev int64) string {
	t.Helper()
	resp, err := c.Get(context.Background(), key, clientv3.WithRev(rev))
	if err != nil {
		t.Fatalf("get %q at revision %d: %v", key, rev, err)
	}
	if len(resp.Kvs) == 0 {
		return ""
	}
	return string(resp.Kvs[0].Value)
}

// ── t4 processes ─────────────────────────────────────────────────────────────

type node struct {
	cmd    *exec.Cmd
	log    *bytes.Buffer
	addr   string
	client *clientv3.Client
	bin    string
}

// startNode runs bin and connects to it, as root when rootPass is set.
func startNode(t *testing.T, bin, dataDir string, extra []string, rootPass string) *node {
	t.Helper()
	addr := testutil.FreeAddr(t)
	args := append([]string{
		"run",
		"--data-dir", dataDir,
		"--listen", addr,
		"--metrics-addr", "127.0.0.1:0",
		"--log-level", "warn",
		"--auth-enabled", // serves the auth API; enforced once AuthEnable is called
	}, extra...)
	n := &node{log: &bytes.Buffer{}, addr: addr, bin: bin}
	n.cmd = exec.Command(bin, args...)
	n.cmd.Stdout = n.log
	n.cmd.Stderr = n.log
	if err := n.cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", bin, err)
	}
	t.Cleanup(func() {
		_ = n.cmd.Process.Kill()
		if t.Failed() {
			t.Logf("%s log:\n%s", bin, n.log)
		}
	})

	user := ""
	if rootPass != "" {
		user = "root"
	}
	c := n.newClient(t, user, rootPass)
	n.client = c
	deadline := time.Now().Add(90 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := c.Get(ctx, "/upgrade/ready-probe")
		cancel()
		if err == nil {
			return n
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not serve within 90s: %v\nlog:\n%s", bin, err, n.log)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// newClient connects to the node, logged in when user is set. It is closed
// when the test ends.
func (n *node) newClient(t *testing.T, user, pass string) *clientv3.Client {
	t.Helper()
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{n.addr},
		DialTimeout: 5 * time.Second,
		Username:    user,
		Password:    pass,
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func (n *node) rootClient(t *testing.T) *clientv3.Client {
	t.Helper()
	return n.newClient(t, "root", rootPassword)
}

// stop shuts the node down gracefully and fails the test if it exits with an
// error.
func (n *node) stop(t *testing.T) {
	t.Helper()
	_ = n.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- n.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("%s exited with %v\nlog:\n%s", n.bin, err, n.log)
		}
	case <-time.After(60 * time.Second):
		_ = n.cmd.Process.Kill()
		<-done
		t.Errorf("%s did not stop within 60s\nlog:\n%s", n.bin, n.log)
	}
}

func buildT4(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "t4")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/t4")
	cmd.Dir = filepath.Join("..", "..")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build t4: %v\n%s", err, out)
	}
	return bin
}

// s3Args creates the bucket if needed and returns flags for a fresh prefix in
// it, with a checkpoint every 50 entries so that a restore reads both a
// checkpoint and the WAL after it.
func s3Args(t *testing.T) []string {
	t.Helper()
	endpoint := envOr("S3_ENDPOINT", "http://127.0.0.1:9000")
	access := envOr("S3_ACCESS_KEY", "t4testadmin")
	secret := envOr("S3_SECRET_KEY", "t4testadmin")
	region := envOr("S3_REGION", "us-east-1")
	bucket := envOr("S3_BUCKET", "t4-upgrade")

	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse S3_ENDPOINT %q: %v", endpoint, err)
	}
	mc, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Secure: u.Scheme == "https",
		Region: region,
	})
	if err != nil {
		t.Fatalf("s3 client: %v", err)
	}
	ctx := context.Background()
	if err := mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: region}); err != nil {
		if ok, herr := mc.BucketExists(ctx, bucket); herr != nil || !ok {
			t.Fatalf("create bucket %q: %v", bucket, err)
		}
	}
	return []string{
		"--s3-bucket", bucket,
		"--s3-prefix", fmt.Sprintf("upgrade-%d", time.Now().UnixNano()),
		"--s3-endpoint", endpoint,
		"--s3-region", region,
		"--s3-access-key-id", access,
		"--s3-secret-access-key", secret,
		"--checkpoint-entries", "50",
	}
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

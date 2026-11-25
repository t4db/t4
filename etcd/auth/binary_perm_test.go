package auth_test

import (
	"context"
	"strings"
	"testing"

	"github.com/t4db/t4/etcd/auth"
)

// A permission range is arbitrary bytes — authpb declares it as bytes, and
// clients such as kine use keys that are not valid UTF-8. Persisting it
// through encoding/json as a Go string replaced those bytes with U+FFFD, so
// the stored range differed from the granted one. The in-memory cache kept
// the granted range and the load path read the stored one, which meant a
// restart silently started enforcing a different ACL.
func TestBinaryPermissionRangeSurvivesReload(t *testing.T) {
	const (
		role     = "binrole"
		user     = "binuser"
		startKey = "/juicefs\xfdsetting"
		endKey   = "/juicefs\xfdsettinh"
	)

	node := newNode(t)
	store := newStore(t, node)
	ctx := context.Background()

	if err := store.PutRole(ctx, auth.Role{Name: role}); err != nil {
		t.Fatalf("PutRole: %v", err)
	}
	if err := store.GrantPermission(ctx, role, auth.Permission{
		Key: startKey, RangeEnd: endKey, PermType: auth.READWRITE,
	}); err != nil {
		t.Fatalf("GrantPermission: %v", err)
	}
	if err := store.PutUser(ctx, auth.User{Name: user}, "pw"); err != nil {
		t.Fatalf("PutUser: %v", err)
	}
	if err := store.GrantRole(ctx, user, role); err != nil {
		t.Fatalf("GrantRole: %v", err)
	}
	// Permission checks pass for everyone while auth is off, so the
	// assertions below say nothing unless it is on.
	if err := store.PutUser(ctx, auth.User{Name: auth.RootUser}, "pw"); err != nil {
		t.Fatalf("PutUser root: %v", err)
	}
	if err := store.Enable(ctx); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	// Before a reload the cache holds exactly what was granted.
	if err := store.CheckPermission(user, startKey, auth.READ); err != nil {
		t.Fatalf("granted key denied before reload: %v", err)
	}

	// A fresh Store reads the persisted role, which is what a restart does.
	reloaded := newStore(t, node)

	got, err := reloaded.GetRole(role)
	if err != nil {
		t.Fatalf("GetRole after reload: %v", err)
	}
	if len(got.Permissions) != 1 {
		t.Fatalf("permissions after reload = %d, want 1", len(got.Permissions))
	}
	if k := got.Permissions[0].Key; k != startKey {
		t.Errorf("stored key = %x, want %x", k, startKey)
	}
	if e := got.Permissions[0].RangeEnd; e != endKey {
		t.Errorf("stored range end = %x, want %x", e, endKey)
	}
	if strings.Contains(got.Permissions[0].Key, "\xef\xbf\xbd") {
		t.Error("stored key holds an encoded U+FFFD: the bytes were replaced")
	}

	// And the reloaded ACL enforces the same range, neither wider nor
	// narrower than the one granted.
	if err := reloaded.CheckPermission(user, startKey, auth.READ); err != nil {
		t.Errorf("granted key denied after reload: %v", err)
	}
	for _, outside := range []string{
		"/juicefs�setting",    // what the mangled range used to cover
		"/juicefs\xfdsettinh", // the exclusive end
		"/juicefs\xfcsetting",
	} {
		if err := reloaded.CheckPermission(user, outside, auth.READ); err == nil {
			t.Errorf("key %x outside the granted range was allowed after reload", outside)
		}
	}
}

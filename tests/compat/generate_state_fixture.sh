#!/usr/bin/env bash
# Generates a fixture of a database holding etcd lease and auth state, as a
# release before the meta keyspace writes it: in reserved keys of the
# revisioned data keyspace.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="$ROOT/tests/compat/testdata"
BASELINE="${1:-v1.1.11}"
WORK_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/t4-compat-state.XXXXXX")"

cleanup() {
  if [[ -d "$WORK_ROOT/src" ]]; then
    git -C "$ROOT" worktree remove --force "$WORK_ROOT/src" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK_ROOT"
}
trap cleanup EXIT

mkdir -p "$OUT/$BASELINE"
git -C "$ROOT" worktree add --detach "$WORK_ROOT/src" "$BASELINE"

mkdir -p "$WORK_ROOT/src/.compatgen"
cat >"$WORK_ROOT/src/.compatgen/main.go" <<'GO'
package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/authpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/etcd/auth"
	"github.com/t4db/t4/pkg/object"
)

// Leases and tokens store absolute expiry times, so they are granted for a
// century to stay live in the fixture.
const (
	leaseTTL = 100 * 365 * 24 * 60 * 60
	tokenTTL = leaseTTL * time.Second
)

type fileStore struct {
	root string
}

func (s fileStore) Put(_ context.Context, key string, r io.Reader) error {
	path := filepath.Join(s.root, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (s fileStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	f, err := os.Open(filepath.Join(s.root, filepath.FromSlash(key)))
	if errors.Is(err, os.ErrNotExist) {
		return nil, object.ErrNotFound
	}
	return f, err
}

func (s fileStore) Delete(_ context.Context, key string) error {
	if err := os.Remove(filepath.Join(s.root, filepath.FromSlash(key))); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s fileStore) DeleteMany(ctx context.Context, keys []string) error {
	for _, key := range keys {
		if err := s.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (s fileStore) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	err := filepath.WalkDir(s.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return nil
	})
	sort.Strings(keys)
	return keys, err
}

func main() {
	if len(os.Args) != 2 {
		panic("usage: compatgen <output-dir>")
	}
	out := os.Args[1]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	work, err := os.MkdirTemp("", "t4-compat-state-*")
	must(err)
	defer os.RemoveAll(work)

	dataDir := filepath.Join(work, "local-data")
	objectDir := filepath.Join(work, "object-store")
	must(os.MkdirAll(objectDir, 0o755))
	store := fileStore{root: objectDir}

	// First session, all in a checkpoint: a lease with keys, a lease
	// without, users and a role.
	s := open(ctx, dataDir, store, t4.Config{CheckpointInterval: 25 * time.Millisecond, CheckpointEntries: 2})
	withKeys, err := s.cli.Grant(ctx, leaseTTL)
	must(err)
	_, err = s.cli.Put(ctx, "/compat/lease/a", "lease-a", clientv3.WithLease(withKeys.ID))
	must(err)
	_, err = s.cli.Put(ctx, "/compat/lease/b", "lease-b", clientv3.WithLease(withKeys.ID))
	must(err)
	_, err = s.cli.Put(ctx, "/compat/plain", "plain")
	must(err)
	noKeys, err := s.cli.Grant(ctx, leaseTTL)
	must(err)
	must(s.auth.PutUser(ctx, auth.User{Name: auth.RootUser}, "root-pw"))
	must(s.auth.PutRole(ctx, auth.Role{Name: auth.RootRole}))
	must(s.auth.GrantRole(ctx, auth.RootUser, auth.RootRole))
	must(s.auth.PutRole(ctx, auth.Role{Name: "reader", Permissions: []auth.Permission{
		{Key: "/compat/", RangeEnd: "/compat0", PermType: authpb.READ},
	}}))
	must(s.auth.PutUser(ctx, auth.User{Name: "alice"}, "alice-pw"))
	must(s.auth.GrantRole(ctx, "alice", "reader"))
	checkpointRev := s.node.CurrentRevision()
	must(waitManifestAtLeast(ctx, store, checkpointRev))
	s.close()

	// Second session, with no checkpoints, so all in the WAL: a lease
	// attached in a later write, a token, and auth enabled.
	s = open(ctx, dataDir, store, t4.Config{CheckpointInterval: time.Hour})
	later, err := s.cli.Grant(ctx, leaseTTL)
	must(err)
	_, err = s.cli.Put(ctx, "/compat/lease/c", "lease-c", clientv3.WithLease(later.ID))
	must(err)
	token, err := s.tokens.Generate("alice")
	must(err)
	must(waitTokenPersisted(s.node))
	must(s.auth.Enable(ctx))
	rev := s.node.CurrentRevision()
	s.close()
	must(checkManifest(ctx, store, checkpointRev))

	meta := map[string]any{
		"baseline":        os.Getenv("T4_COMPAT_BASELINE"),
		"checkpoint_revision": checkpointRev,
		"revision":            rev,
		"lease_with_keys": withKeys.ID,
		"lease_no_keys":   noKeys.ID,
		"lease_in_wal":    later.ID,
		"token":           token,
	}
	must(os.MkdirAll(out, 0o755))
	must(writeJSON(filepath.Join(out, "state.json"), meta))
	must(tarGz(filepath.Join(out, "state-local-data.tar.gz"), dataDir))
	must(tarGz(filepath.Join(out, "state-object-store.tar.gz"), objectDir))
}

// session is a node serving the etcd API.
type session struct {
	node   *t4.Node
	auth   *auth.Store
	tokens *auth.TokenStore
	gs     *grpc.Server
	cli    *clientv3.Client
}

func open(ctx context.Context, dataDir string, store fileStore, cfg t4.Config) *session {
	cfg.DataDir = dataDir
	cfg.ObjectStore = store
	cfg.SegmentMaxAge = 25 * time.Millisecond
	node, err := t4.Open(cfg)
	must(err)
	authStore, err := auth.NewStore(node)
	must(err)
	tokens := auth.NewTokenStore(ctx, tokenTTL, node)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	gs := grpc.NewServer(t4etcd.NewServerOptions(authStore, tokens)...)
	t4etcd.New(node, authStore, tokens).Register(gs)
	go func() { _ = gs.Serve(lis) }()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{lis.Addr().String()},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	must(err)
	return &session{node: node, auth: authStore, tokens: tokens, gs: gs, cli: cli}
}

func (s *session) close() {
	must(s.cli.Close())
	s.gs.Stop()
	must(s.node.Close())
}

// checkManifest fails unless the latest checkpoint is at rev, so that every
// later write is only in the WAL.
func checkManifest(ctx context.Context, store fileStore, rev int64) error {
	rc, err := store.Get(ctx, "manifest/latest")
	if err != nil {
		return err
	}
	defer rc.Close()
	var m struct {
		Revision int64 `json:"revision"`
	}
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return err
	}
	if m.Revision != rev {
		return fmt.Errorf("latest checkpoint at revision %d, want %d", m.Revision, rev)
	}
	return nil
}

// waitTokenPersisted waits for the token Generate writes in the background.
func waitTokenPersisted(node *t4.Node) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		kvs, err := node.List("\x00auth/tokens/")
		if err != nil {
			return err
		}
		if len(kvs) > 0 {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return errors.New("timed out waiting for the token to be persisted")
}

func waitManifestAtLeast(ctx context.Context, store fileStore, rev int64) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rc, err := store.Get(ctx, "manifest/latest")
		if err == nil {
			var m struct {
				Revision int64 `json:"revision"`
			}
			decodeErr := json.NewDecoder(rc).Decode(&m)
			_ = rc.Close()
			if decodeErr == nil && m.Revision >= rev {
				return nil
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for manifest revision >= %d", rev)
}

func writeJSON(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func tarGz(dst, src string) error {
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		hdr.Mode = 0o644
		hdr.ModTime = time.Unix(0, 0)
		hdr.AccessTime = time.Unix(0, 0)
		hdr.ChangeTime = time.Unix(0, 0)
		hdr.Uid = 0
		hdr.Gid = 0
		hdr.Uname = ""
		hdr.Gname = ""
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		if _, err := io.Copy(tw, in); err != nil {
			_ = in.Close()
			return err
		}
		return in.Close()
	})
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
GO

(
  cd "$WORK_ROOT/src"
  T4_COMPAT_BASELINE="$BASELINE" go run ./.compatgen "$OUT/$BASELINE"
)

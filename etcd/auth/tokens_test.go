package auth_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/t4db/t4/etcd/auth"
)

const tokensPrefix = "\x00auth/tokens/"

func hashedTokenKey(token string) string {
	sum := sha256.Sum256([]byte(token))
	return tokensPrefix + "sha256/" + hex.EncodeToString(sum[:])
}

// TestTokenStore_PersistsOnlyHashes pins that a token itself is never written
// to storage, which also reaches the WAL, checkpoints and object store: only
// its hash, under a key naming the hash algorithm.
func TestTokenStore_PersistsOnlyHashes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node := newNode(t)
	ts := auth.NewTokenStore(ctx, 5*time.Minute, node)

	tok, err := ts.Generate("alice")
	if err != nil {
		t.Fatal(err)
	}
	kvs, err := node.List(tokensPrefix)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 {
		t.Fatalf("persisted %d token keys, want 1", len(kvs))
	}
	if kvs[0].Key != hashedTokenKey(tok) {
		t.Errorf("token key = %q, want %q", kvs[0].Key, hashedTokenKey(tok))
	}
	if strings.Contains(kvs[0].Key, tok) || strings.Contains(string(kvs[0].Value), tok) {
		t.Error("the token itself was persisted")
	}
}

// TestTokenStore_UpgradesUnhashedTokens: releases before token hashing stored
// each token itself as the key. Such a token must stay valid after the
// upgrade, and its key is rewritten to the hashed form.
func TestTokenStore_UpgradesUnhashedTokens(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node := newNode(t)

	const tok = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	value, err := json.Marshal(map[string]any{"username": "alice", "expiry": time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.Put(ctx, tokensPrefix+tok, value, 0); err != nil {
		t.Fatal(err)
	}

	ts := auth.NewTokenStore(ctx, 5*time.Minute, node)
	if user, ok := ts.Lookup(tok); !ok || user != "alice" {
		t.Fatalf("Lookup of a token stored unhashed = %q, %v; want alice", user, ok)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		kvs, err := node.List(tokensPrefix)
		if err != nil {
			t.Fatal(err)
		}
		if len(kvs) == 1 && kvs[0].Key == hashedTokenKey(tok) {
			break
		}
		if time.Now().After(deadline) {
			var keys []string
			for _, kv := range kvs {
				keys = append(keys, kv.Key)
			}
			t.Fatalf("token keys %q, want only %q", keys, hashedTokenKey(tok))
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Still valid after another restart, from the hashed key.
	if user, ok := auth.NewTokenStore(ctx, 5*time.Minute, node).Lookup(tok); !ok || user != "alice" {
		t.Fatalf("Lookup after rehash and restart = %q, %v; want alice", user, ok)
	}
}

// TestTokenStore_SkipsUnknownHashAlgorithm: a key hashed with an algorithm
// this release does not know (written by a newer one) is left alone.
func TestTokenStore_SkipsUnknownHashAlgorithm(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node := newNode(t)

	value, err := json.Marshal(map[string]any{"username": "alice", "expiry": time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	const key = tokensPrefix + "sha3-256/abcdef"
	if _, err := node.Put(ctx, key, value, 0); err != nil {
		t.Fatal(err)
	}
	ts := auth.NewTokenStore(ctx, 5*time.Minute, node)
	if _, ok := ts.Lookup("sha3-256/abcdef"); ok {
		t.Error("a key hashed with an unknown algorithm was taken as a token")
	}
	if kv, err := node.Get(key); err != nil || kv == nil {
		t.Errorf("key hashed with an unknown algorithm was removed (err=%v)", err)
	}
}

package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// TokenStore manages short-lived bearer tokens backed by Pebble for persistence
// across restarts.  When n is nil, tokens are in-memory only (useful for tests).
//
// Tokens are never stored, in memory or persisted: only their hash is, as the
// key tokensPrefix + tokenHashAlgo + "/" + hex(hash). The algorithm is named in
// the key so that it can be changed later while still reading existing keys.
// A token is 256 random bits, so a fast unsalted hash leaves nothing to guess.
type TokenStore struct {
	mu     sync.RWMutex
	tokens map[string]tokenEntry // keyed by tokenHash
	ttl    time.Duration
	n      node
}

// tokenHashAlgo names the hash in persisted token keys.
const tokenHashAlgo = "sha256"

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func tokenKey(hash string) string {
	return tokensPrefix + tokenHashAlgo + "/" + hash
}

type tokenEntry struct {
	username string
	expiry   time.Time
}

// storedToken is the JSON-serializable form of tokenEntry persisted in Pebble.
type storedToken struct {
	Username string    `json:"username"`
	Expiry   time.Time `json:"expiry"`
}

// NewTokenStore creates a TokenStore with the given TTL, loads any persisted
// non-expired tokens from n (if non-nil), and starts a background eviction
// goroutine that runs until ctx is cancelled.
func NewTokenStore(ctx context.Context, ttl time.Duration, n node) *TokenStore {
	ts := &TokenStore{
		tokens: make(map[string]tokenEntry),
		ttl:    ttl,
		n:      n,
	}
	if n != nil {
		ts.load()
	}
	go ts.evictLoop(ctx)
	return ts
}

// load reads persisted tokens from Pebble, skipping any that have already
// expired. Releases before token hashing stored each token itself as the key;
// such keys are hashed and rewritten in the background, so their tokens stay
// valid across the upgrade.
func (ts *TokenStore) load() {
	kvs, err := ts.n.List(tokensPrefix)
	if err != nil {
		logrus.WithError(err).Warn("auth: failed to load persisted tokens")
		return
	}
	now := time.Now()
	for _, kv := range kvs {
		var st storedToken
		if err := json.Unmarshal(kv.Value, &st); err != nil {
			logrus.WithError(err).Warn("auth: skipping malformed persisted token")
			continue
		}
		hash, legacy := "", false
		switch name := strings.TrimPrefix(kv.Key, tokensPrefix); {
		case strings.HasPrefix(name, tokenHashAlgo+"/"):
			hash = strings.TrimPrefix(name, tokenHashAlgo+"/")
		case strings.Contains(name, "/"):
			// Hashed with an algorithm this release does not know.
			continue
		default:
			hash, legacy = tokenHash(name), true
		}
		if now.After(st.Expiry) {
			// Clean up expired token from Pebble in the background.
			go ts.n.Delete(context.Background(), kv.Key) //nolint:errcheck
			continue
		}
		ts.tokens[hash] = tokenEntry{username: st.Username, expiry: st.Expiry}
		if legacy {
			go ts.rehash(kv.Key, hash, kv.Value)
		}
	}
}

// rehash moves a token stored by an earlier release under its hashed key.
func (ts *TokenStore) rehash(legacyKey, hash string, value []byte) {
	ctx := context.Background()
	if _, err := ts.n.Put(ctx, tokenKey(hash), value, 0); err != nil {
		logrus.WithError(err).Warn("auth: failed to rehash persisted token")
		return
	}
	if _, err := ts.n.Delete(ctx, legacyKey); err != nil {
		logrus.WithError(err).Warn("auth: failed to delete unhashed persisted token")
	}
}

// Generate mints a new token for username, persists it to Pebble if a node is
// configured, and returns the token string.
func (ts *TokenStore) Generate(username string) (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	tok := hex.EncodeToString(raw)
	hash := tokenHash(tok)
	entry := tokenEntry{username: username, expiry: time.Now().Add(ts.ttl)}

	ts.mu.Lock()
	ts.tokens[hash] = entry
	ts.mu.Unlock()

	if ts.n != nil {
		data, err := json.Marshal(storedToken{Username: entry.username, Expiry: entry.expiry})
		if err == nil {
			if _, err := ts.n.Put(context.Background(), tokenKey(hash), data, 0); err != nil {
				logrus.WithError(err).Warn("auth: failed to persist token")
			}
		}
	}

	return tok, nil
}

// Lookup returns the username associated with token, or ("", false) if the
// token is unknown or expired.
func (ts *TokenStore) Lookup(token string) (string, bool) {
	ts.mu.RLock()
	e, ok := ts.tokens[tokenHash(token)]
	ts.mu.RUnlock()

	if !ok || time.Now().After(e.expiry) {
		return "", false
	}
	return e.username, true
}

// Revoke invalidates a token immediately and removes it from Pebble.
func (ts *TokenStore) Revoke(token string) {
	hash := tokenHash(token)
	ts.mu.Lock()
	delete(ts.tokens, hash)
	ts.mu.Unlock()

	if ts.n != nil {
		if _, err := ts.n.Delete(context.Background(), tokenKey(hash)); err != nil {
			logrus.WithError(err).Warn("auth: failed to delete revoked token from store")
		}
	}
}

func (ts *TokenStore) evictLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ts.evict()
		}
	}
}

func (ts *TokenStore) evict() {
	now := time.Now()
	ts.mu.Lock()
	var expired []string
	for hash, e := range ts.tokens {
		if now.After(e.expiry) {
			delete(ts.tokens, hash)
			expired = append(expired, hash)
		}
	}
	ts.mu.Unlock()

	if ts.n == nil {
		return
	}
	for _, hash := range expired {
		if _, err := ts.n.Delete(context.Background(), tokenKey(hash)); err != nil {
			logrus.WithError(err).Warn("auth: failed to delete expired token from store")
		}
	}
}

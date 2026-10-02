package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/t4db/t4/internal/sysstate"
)

// TokenStore manages short-lived bearer tokens backed by Pebble for persistence
// across restarts.  When n is nil, tokens are in-memory only (useful for tests).
type TokenStore struct {
	mu     sync.RWMutex
	tokens map[string]tokenEntry
	ttl    time.Duration
	n      node
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

// load reads persisted tokens from Pebble, skipping any that have already expired.
func (ts *TokenStore) load() {
	kvs, err := sysstate.List(ts.n, tokensPrefix)
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
		if now.After(st.Expiry) {
			// Clean up expired token from Pebble in the background.
			tok := strings.TrimPrefix(kv.Key, tokensPrefix)
			go sysstate.Delete(context.Background(), ts.n, tokensPrefix+tok) //nolint:errcheck
			continue
		}
		tok := strings.TrimPrefix(kv.Key, tokensPrefix)
		ts.tokens[tok] = tokenEntry{username: st.Username, expiry: st.Expiry}
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
	entry := tokenEntry{username: username, expiry: time.Now().Add(ts.ttl)}

	ts.mu.Lock()
	ts.tokens[tok] = entry
	ts.mu.Unlock()

	if ts.n != nil {
		data, err := json.Marshal(storedToken{Username: entry.username, Expiry: entry.expiry})
		if err == nil {
			if err := sysstate.Put(context.Background(), ts.n, tokensPrefix+tok, data); err != nil {
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
	e, ok := ts.tokens[token]
	ts.mu.RUnlock()

	if !ok || time.Now().After(e.expiry) {
		return "", false
	}
	return e.username, true
}

// Revoke invalidates a token immediately and removes it from Pebble.
func (ts *TokenStore) Revoke(token string) {
	ts.mu.Lock()
	delete(ts.tokens, token)
	ts.mu.Unlock()

	if ts.n != nil {
		if err := sysstate.Delete(context.Background(), ts.n, tokensPrefix+token); err != nil {
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
	for tok, e := range ts.tokens {
		if now.After(e.expiry) {
			delete(ts.tokens, tok)
			expired = append(expired, tok)
		}
	}
	ts.mu.Unlock()

	if ts.n == nil {
		return
	}
	for _, tok := range expired {
		if err := sysstate.Delete(context.Background(), ts.n, tokensPrefix+tok); err != nil {
			logrus.WithError(err).Warn("auth: failed to delete expired token from store")
		}
	}
}

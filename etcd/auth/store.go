package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/crypto/bcrypt"

	"github.com/strata-db/strata"
	"github.com/strata-db/strata/internal/metrics"
)

const (
	// authPrefix is the reserved Pebble key namespace for auth data.
	// The null byte ensures no normal etcd client key can collide.
	authPrefix   = "\x00auth/"
	enabledKey   = "\x00auth/enabled"
	usersPrefix  = "\x00auth/users/"
	rolesPrefix  = "\x00auth/roles/"
	tokensPrefix = "\x00auth/tokens/"

	bcryptCost = bcrypt.DefaultCost
)

// node is the subset of strata.Node used by Store.
type node interface {
	Put(ctx context.Context, key string, value []byte, lease int64) (int64, error)
	Get(key string) (*strata.KeyValue, error)
	List(prefix string) ([]*strata.KeyValue, error)
	Delete(ctx context.Context, key string) (int64, error)
}

// Store persists auth state (users, roles, enabled flag) in Pebble via a
// strata Node.  All writes flow through the WAL, so followers stay in sync
// and S3 checkpoints include auth data.
//
// Users and roles are also kept in an in-memory map under mu so that
// CheckPermission (hot path, called on every authenticated request) never
// touches Pebble.
type Store struct {
	mu      sync.RWMutex
	n       node
	enabled bool
	users   map[string]User
	roles   map[string]Role
	rateLim *rateLimiter
}

// NewStore creates a Store backed by n and loads the current auth state from
// Pebble.
func NewStore(n node) (*Store, error) {
	s := &Store{
		n:       n,
		users:   make(map[string]User),
		roles:   make(map[string]Role),
		rateLim: newRateLimiter(),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// IsAuthPrefix reports whether key is in the reserved auth namespace.
func IsAuthPrefix(key string) bool {
	return len(key) >= len(authPrefix) && key[:len(authPrefix)] == authPrefix
}

// IsEnabled returns true when auth is active.
func (s *Store) IsEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.enabled
}

// Enable turns on auth.  The root user must already exist.
func (s *Store) Enable(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.enabled {
		return nil
	}
	if _, err := s.getUser(RootUser); err != nil {
		return errors.New("root user must exist before enabling auth")
	}
	// Ensure root role exists.
	if _, err := s.getRole(RootRole); err != nil {
		root := Role{Name: RootRole}
		if err2 := s.putRole(ctx, root); err2 != nil {
			return fmt.Errorf("create root role: %w", err2)
		}
	}
	return s.setEnabled(ctx, true)
}

// Disable turns off auth.
func (s *Store) Disable(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.enabled {
		return nil
	}
	return s.setEnabled(ctx, false)
}

// ── Users ────────────────────────────────────────────────────────────────────

// GetUser returns the named user.
func (s *Store) GetUser(name string) (User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getUser(name)
}

// PutUser creates or replaces a user, hashing the plain-text password when
// provided (empty string keeps the existing hash).
func (s *Store) PutUser(ctx context.Context, u User, plainPassword string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if plainPassword != "" {
		h, err := bcrypt.GenerateFromPassword([]byte(plainPassword), bcryptCost)
		if err != nil {
			return fmt.Errorf("hash password: %w", err)
		}
		u.PasswordHash = string(h)
	}
	return s.putUser(ctx, u)
}

// DeleteUser removes a user. The root user cannot be deleted while auth is
// enabled.
func (s *Store) DeleteUser(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.enabled && name == RootUser {
		return errors.New("cannot delete root user while auth is enabled")
	}
	if _, err := s.getUser(name); err != nil {
		return fmt.Errorf("user %q not found", name)
	}
	if _, err := s.n.Delete(ctx, usersPrefix+name); err != nil {
		return err
	}
	delete(s.users, name)
	return nil
}

// ListUsers returns all users sorted by name.
func (s *Store) ListUsers() ([]User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.listUsers()
}

// CheckPassword returns nil when password matches the stored hash for name.
func (s *Store) CheckPassword(name, password string) error {
	if s.rateLim.IsLocked(name) {
		metrics.AuthAttemptsTotal.WithLabelValues("locked").Inc()
		return errors.New("authentication failed: too many attempts, try again later")
	}

	s.mu.RLock()
	u, err := s.getUser(name)
	s.mu.RUnlock()

	if err != nil {
		s.rateLim.RecordFailure(name)
		metrics.AuthAttemptsTotal.WithLabelValues("fail").Inc()
		return errors.New("authentication failed")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)); err != nil {
		s.rateLim.RecordFailure(name)
		metrics.AuthAttemptsTotal.WithLabelValues("fail").Inc()
		return errors.New("authentication failed")
	}
	s.rateLim.RecordSuccess(name)
	metrics.AuthAttemptsTotal.WithLabelValues("success").Inc()
	return nil
}

// GrantRole adds roleName to the user's role list (idempotent).
func (s *Store) GrantRole(ctx context.Context, userName, roleName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, err := s.getUser(userName)
	if err != nil {
		return fmt.Errorf("user %q not found", userName)
	}
	if _, err := s.getRole(roleName); err != nil {
		return fmt.Errorf("role %q not found", roleName)
	}
	if u.HasRole(roleName) {
		return nil
	}
	u.Roles = append(u.Roles, roleName)
	return s.putUser(ctx, u)
}

// RevokeRole removes roleName from the user's role list.
func (s *Store) RevokeRole(ctx context.Context, userName, roleName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, err := s.getUser(userName)
	if err != nil {
		return fmt.Errorf("user %q not found", userName)
	}
	roles := u.Roles[:0]
	for _, r := range u.Roles {
		if r != roleName {
			roles = append(roles, r)
		}
	}
	u.Roles = roles
	return s.putUser(ctx, u)
}

// ── Roles ────────────────────────────────────────────────────────────────────

// GetRole returns the named role.
func (s *Store) GetRole(name string) (Role, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getRole(name)
}

// PutRole creates or replaces a role.
func (s *Store) PutRole(ctx context.Context, r Role) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putRole(ctx, r)
}

// DeleteRole removes a role. The root role cannot be deleted while auth is
// enabled.
func (s *Store) DeleteRole(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.enabled && name == RootRole {
		return errors.New("cannot delete root role while auth is enabled")
	}
	if _, err := s.getRole(name); err != nil {
		return fmt.Errorf("role %q not found", name)
	}
	if _, err := s.n.Delete(ctx, rolesPrefix+name); err != nil {
		return err
	}
	delete(s.roles, name)
	return nil
}

// ListRoles returns all roles sorted by name.
func (s *Store) ListRoles() ([]Role, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.listRoles()
}

// GrantPermission adds p to role r (replaces existing entry for same key/rangeEnd).
func (s *Store) GrantPermission(ctx context.Context, roleName string, p Permission) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, err := s.getRole(roleName)
	if err != nil {
		return fmt.Errorf("role %q not found", roleName)
	}
	for i, existing := range r.Permissions {
		if existing.Key == p.Key && existing.RangeEnd == p.RangeEnd {
			r.Permissions[i] = p
			return s.putRole(ctx, r)
		}
	}
	r.Permissions = append(r.Permissions, p)
	return s.putRole(ctx, r)
}

// RevokePermission removes the permission matching key/rangeEnd from role.
func (s *Store) RevokePermission(ctx context.Context, roleName, key, rangeEnd string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, err := s.getRole(roleName)
	if err != nil {
		return fmt.Errorf("role %q not found", roleName)
	}
	perms := r.Permissions[:0]
	for _, p := range r.Permissions {
		if p.Key != key || p.RangeEnd != rangeEnd {
			perms = append(perms, p)
		}
	}
	r.Permissions = perms
	return s.putRole(ctx, r)
}

// ── Permission check ─────────────────────────────────────────────────────────

// CheckPermission returns nil when the user identified by userName has pt
// access to key, or when auth is disabled.
func (s *Store) CheckPermission(userName, key string, pt PermType) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.enabled {
		return nil
	}
	u, err := s.getUser(userName)
	if err != nil {
		return fmt.Errorf("user %q not found", userName)
	}
	for _, roleName := range u.Roles {
		r, err := s.getRole(roleName)
		if err != nil {
			continue
		}
		if r.HasPermission(key, pt) {
			return nil
		}
	}
	return fmt.Errorf("permission denied")
}

// ── Internal helpers (caller holds mu) ───────────────────────────────────────

func (s *Store) load() error {
	kv, err := s.n.Get(enabledKey)
	if err != nil {
		return fmt.Errorf("load auth state: %w", err)
	}
	if kv != nil {
		s.enabled = string(kv.Value) == "1"
	}

	// Warm the in-memory caches.
	userKVs, err := s.n.List(usersPrefix)
	if err != nil {
		return fmt.Errorf("load users: %w", err)
	}
	for _, ukv := range userKVs {
		var u User
		if err := json.Unmarshal(ukv.Value, &u); err != nil {
			return fmt.Errorf("decode user: %w", err)
		}
		s.users[u.Name] = u
	}

	roleKVs, err := s.n.List(rolesPrefix)
	if err != nil {
		return fmt.Errorf("load roles: %w", err)
	}
	for _, rkv := range roleKVs {
		var r Role
		if err := json.Unmarshal(rkv.Value, &r); err != nil {
			return fmt.Errorf("decode role: %w", err)
		}
		s.roles[r.Name] = r
	}

	return nil
}

func (s *Store) setEnabled(ctx context.Context, on bool) error {
	v := "0"
	if on {
		v = "1"
	}
	if _, err := s.n.Put(ctx, enabledKey, []byte(v), 0); err != nil {
		return err
	}
	s.enabled = on
	return nil
}

func (s *Store) getUser(name string) (User, error) {
	u, ok := s.users[name]
	if !ok {
		return User{}, fmt.Errorf("user %q not found", name)
	}
	return u, nil
}

func (s *Store) putUser(ctx context.Context, u User) error {
	data, err := json.Marshal(u)
	if err != nil {
		return fmt.Errorf("encode user: %w", err)
	}
	if _, err := s.n.Put(ctx, usersPrefix+u.Name, data, 0); err != nil {
		return err
	}
	s.users[u.Name] = u
	return nil
}

func (s *Store) listUsers() ([]User, error) {
	users := make([]User, 0, len(s.users))
	for _, u := range s.users {
		users = append(users, u)
	}
	return users, nil
}

func (s *Store) getRole(name string) (Role, error) {
	r, ok := s.roles[name]
	if !ok {
		return Role{}, fmt.Errorf("role %q not found", name)
	}
	return r, nil
}

func (s *Store) putRole(ctx context.Context, r Role) error {
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("encode role: %w", err)
	}
	if _, err := s.n.Put(ctx, rolesPrefix+r.Name, data, 0); err != nil {
		return err
	}
	s.roles[r.Name] = r
	return nil
}

func (s *Store) listRoles() ([]Role, error) {
	roles := make([]Role, 0, len(s.roles))
	for _, r := range s.roles {
		roles = append(roles, r)
	}
	return roles, nil
}

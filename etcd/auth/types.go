// Package auth implements etcd-compatible authentication and RBAC for T4.
package auth

import "go.etcd.io/etcd/api/v3/authpb"

// PermType mirrors authpb.Permission_Type for local use.
type PermType = authpb.Permission_Type

const (
	READ      = authpb.READ
	WRITE     = authpb.WRITE
	READWRITE = authpb.READWRITE
)

// Permission grants access to a key range.
type Permission struct {
	Key      string   `json:"key"`
	RangeEnd string   `json:"rangeEnd,omitempty"`
	PermType PermType `json:"permType"`
}

// covers reports whether p grants permType access to the given key.
// If RangeEnd is empty, only exact key match applies.
// If RangeEnd is "\x00", it matches all keys >= Key (etcd open-ended range convention).
func (p Permission) covers(key string, pt PermType) bool {
	if !permIncludes(p.PermType, pt) {
		return false
	}
	if p.RangeEnd == "" {
		return key == p.Key
	}
	if p.RangeEnd == "\x00" {
		return key >= p.Key
	}
	return key >= p.Key && key < p.RangeEnd
}

func permIncludes(have, want PermType) bool {
	return have == READWRITE || have == want
}

// Role is a named set of key permissions.
type Role struct {
	Name        string       `json:"name"`
	Permissions []Permission `json:"permissions,omitempty"`
}

// HasPermission reports whether the role grants pt access to key.
// The root role grants access to everything.
func (r Role) HasPermission(key string, pt PermType) bool {
	if r.Name == RootRole {
		return true
	}
	for _, p := range r.Permissions {
		if p.covers(key, pt) {
			return true
		}
	}
	return false
}

// User is a named principal with a bcrypt-hashed password and a list of role names.
type User struct {
	Name         string   `json:"name"`
	PasswordHash string   `json:"passwordHash"`
	Roles        []string `json:"roles,omitempty"`
}

// HasRole reports whether the user is assigned roleName.
func (u User) HasRole(roleName string) bool {
	for _, r := range u.Roles {
		if r == roleName {
			return true
		}
	}
	return false
}

const (
	// RootRole is the built-in super-role that grants full access.
	RootRole = "root"
	// RootUser is the built-in super-user; must exist before auth can be enabled.
	RootUser = "root"
)

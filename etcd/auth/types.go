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

// coversRange reports whether perms, together, grant pt access to every key
// in [key, rangeEnd), as etcd's IsRangePermitted does: an empty rangeEnd
// means key alone, and "\x00" every key from key on. Permissions combine, so
// a range two of them cover between them is covered.
func coversRange(perms []Permission, key, rangeEnd string, pt PermType) bool {
	if rangeEnd == "" {
		for _, p := range perms {
			if p.covers(key, pt) {
				return true
			}
		}
		return false
	}
	// The intervals perms grant, with an empty end standing for no end. A
	// point permission covers [Key, Key+"\x00").
	type interval struct{ start, end string }
	var ivs []interval
	for _, p := range perms {
		if !permIncludes(p.PermType, pt) {
			continue
		}
		switch p.RangeEnd {
		case "":
			ivs = append(ivs, interval{p.Key, p.Key + "\x00"})
		case "\x00":
			ivs = append(ivs, interval{p.Key, ""})
		default:
			ivs = append(ivs, interval{p.Key, p.RangeEnd})
		}
	}
	end := rangeEnd
	if end == "\x00" {
		end = ""
	}
	// Walk from key, each step extending as far as an interval that starts
	// at or before where the covered part ends.
	at := key
	for {
		if end != "" && at >= end {
			return true
		}
		reach, found := at, false
		for _, iv := range ivs {
			if iv.start > at {
				continue
			}
			if iv.end == "" {
				return true
			}
			if iv.end > reach {
				reach, found = iv.end, true
			}
		}
		if !found {
			return false
		}
		at = reach
	}
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

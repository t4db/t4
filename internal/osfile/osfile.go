//go:build !windows

// Package osfile opens files that may be removed while open, as T4 removes
// WAL segments and SSTs once they are in object storage.
package osfile

import "os"

// Create creates or truncates path for writing.
func Create(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
}

// Open opens path for reading.
func Open(path string) (*os.File, error) { return os.Open(path) }

//go:build !linux

package osfile

import "os"

// Datasync flushes f. Outside Linux it is a full fsync.
func Datasync(f *os.File) error { return f.Sync() }

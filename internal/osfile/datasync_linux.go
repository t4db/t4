package osfile

import (
	"os"

	"golang.org/x/sys/unix"
)

// Datasync flushes f's data, and only the metadata needed to read it back:
// overwriting blocks that are already allocated skips the inode update a
// full fsync would make.
func Datasync(f *os.File) error {
	for {
		err := unix.Fdatasync(int(f.Fd()))
		if err != unix.EINTR {
			if err != nil {
				return &os.PathError{Op: "fdatasync", Path: f.Name(), Err: err}
			}
			return nil
		}
	}
}

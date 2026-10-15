package osfile

import (
	"os"

	"golang.org/x/sys/windows"
)

// Files are opened sharing deletion, as files are on Unix: a WAL segment or
// SST is removed once it is in object storage, which can be while another
// handle still has it open. Go's os package opens files without
// FILE_SHARE_DELETE, and Windows then refuses the removal.
const shareAll = windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE | windows.FILE_SHARE_DELETE

// Create creates or truncates path for writing.
func Create(path string) (*os.File, error) {
	return openShared(path, windows.GENERIC_WRITE, windows.CREATE_ALWAYS)
}

// Open opens path for reading.
func Open(path string) (*os.File, error) {
	return openShared(path, windows.GENERIC_READ, windows.OPEN_EXISTING)
}

func openShared(path string, access, disposition uint32) (*os.File, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	h, err := windows.CreateFile(p, access, shareAll, nil, disposition, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(h), path), nil
}

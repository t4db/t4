package object

import "testing"

// SetGCSChunkSizeForTest shrinks the resumable-upload chunk size for the
// duration of a test, so the chunking boundaries can be exercised without
// moving tens of megabytes. It lives in an _test.go file, so it is not part of
// the package's API.
func SetGCSChunkSizeForTest(t *testing.T, size int64) {
	t.Helper()
	old := gcsChunkSize
	gcsChunkSize = size
	t.Cleanup(func() { gcsChunkSize = old })
}

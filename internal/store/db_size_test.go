package store

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/t4db/t4/internal/wal"
)

type dbSizeTestLogger struct{}

func (dbSizeTestLogger) Warnf(string, ...interface{}) {}

func TestDBSizeMillionKeys(t *testing.T) {
	if os.Getenv("T4_DB_SIZE_TEST") != "1" {
		t.Skip("set T4_DB_SIZE_TEST=1 to run the 1M-key DB size test")
	}

	keys := envInt(t, "T4_DB_SIZE_KEYS", 1_000_000)
	valueBytes := envInt(t, "T4_DB_SIZE_VALUE_BYTES", 256)
	batchSize := envInt(t, "T4_DB_SIZE_BATCH", 10_000)

	dir := filepath.Join(t.TempDir(), "db")
	s, err := Open(dir, dbSizeTestLogger{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	start := time.Now()
	for base := 0; base < keys; base += batchSize {
		n := batchSize
		if remaining := keys - base; remaining < n {
			n = remaining
		}
		entries := make([]wal.Entry, n)
		for i := 0; i < n; i++ {
			rev := int64(base + i + 1)
			key := fmt.Sprintf("/size/%09d", base+i)
			entries[i] = wal.Entry{
				Revision:       rev,
				Term:           1,
				Op:             wal.OpCreate,
				Key:            key,
				Value:          testValue(base+i, valueBytes),
				CreateRevision: rev,
			}
		}
		if err := s.Apply(entries); err != nil {
			t.Fatalf("Apply batch starting at key %d: %v", base, err)
		}
	}
	writeElapsed := time.Since(start)

	if got, err := s.Count("/size/"); err != nil {
		t.Fatalf("Count: %v", err)
	} else if got != int64(keys) {
		t.Fatalf("Count: want %d got %d", keys, got)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	flushedSize, err := dirSize(dir)
	if err != nil {
		t.Fatalf("dirSize after flush: %v", err)
	}

	compactStart := time.Now()
	if err := s.Pebble().Compact([]byte{}, []byte{0xff}, true); err != nil {
		t.Fatalf("Pebble compact: %v", err)
	}
	compactElapsed := time.Since(compactStart)
	compactedSize, err := dirSize(dir)
	if err != nil {
		t.Fatalf("dirSize after compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closedSize, err := dirSize(dir)
	if err != nil {
		t.Fatalf("dirSize after close: %v", err)
	}

	t.Logf("keys=%d value_bytes=%d batch=%d", keys, valueBytes, batchSize)
	t.Logf("write_elapsed=%s write_rate=%.0f keys/s", writeElapsed, float64(keys)/writeElapsed.Seconds())
	t.Logf("flushed_size=%s (%d bytes)", humanBytes(flushedSize), flushedSize)
	t.Logf("pebble_compact_elapsed=%s compacted_size=%s (%d bytes)", compactElapsed, humanBytes(compactedSize), compactedSize)
	t.Logf("closed_size=%s (%d bytes)", humanBytes(closedSize), closedSize)
	t.Logf("closed_bytes_per_key=%.1f", float64(closedSize)/float64(keys))
}

func envInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		t.Fatalf("%s must be a positive integer, got %q", name, raw)
	}
	return n
}

func testValue(i, n int) []byte {
	v := make([]byte, n)
	x := uint64(i) + 0x9e3779b97f4a7c15
	for j := range v {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		v[j] = byte(x)
	}
	return v
}

func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

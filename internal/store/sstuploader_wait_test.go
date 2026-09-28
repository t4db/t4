package store

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/t4db/t4/pkg/object"
)

// TestSSTUploaderWaitDuringQueue: Pebble reports new tables from its own
// goroutines at any time, including while a checkpoint is in Wait. Queuing a
// table then must not race with Wait (a sync.WaitGroup forbids an Add from
// zero concurrent with Wait), and Wait must still return.
func TestSSTUploaderWaitDuringQueue(t *testing.T) {
	// Files that do not exist upload as a no-op, so no object store is needed.
	u := NewSSTUploader(nil, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u.Start(ctx)
	listener := u.EventListener()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 20000; i++ {
				listener.FlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{FileNum: pebble.FileNum(g*10000 + i)}}})
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 20000; i++ {
				u.Wait()
			}
		}()
	}
	wg.Wait()
	u.Wait()
}

// TestSSTUploaderWaitAfterStopDuringQueue: a table queued while the uploader
// is stopping must not be stranded in the channel after Start's drain has
// run, or its upload is never counted done and Wait blocks forever.
func TestSSTUploaderWaitAfterStopDuringQueue(t *testing.T) {
	for i := 0; i < 20000; i++ {
		u := NewSSTUploader(nil, t.TempDir())
		ctx, cancel := context.WithCancel(context.Background())
		u.Start(ctx)
		listener := u.EventListener()

		queued := make(chan struct{})
		go func() {
			listener.FlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{FileNum: pebble.FileNum(i)}}})
			close(queued)
		}()
		cancel()
		<-queued

		waited := make(chan struct{})
		go func() {
			u.Wait()
			close(waited)
		}()
		select {
		case <-waited:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: Wait never returned: a queued upload was stranded at shutdown", i)
		}
	}
}

// TestSSTUploaderWaitBeforeStart: the startup checkpoint flushes Pebble and
// calls Wait before the node starts the uploader. Wait must cover the SST
// that flush queued, not return because no upload is running yet.
func TestSSTUploaderWaitBeforeStart(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "000007.sst"), []byte("table"), 0o644); err != nil {
		t.Fatal(err)
	}
	u := NewSSTUploader(object.NewMem(), dir)
	listener := u.EventListener()
	listener.FlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{FileNum: pebble.FileNum(7)}}})

	waited := make(chan struct{})
	go func() {
		u.Wait()
		close(waited)
	}()
	select {
	case <-waited:
		t.Fatal("Wait returned before the uploader started")
	case <-time.After(100 * time.Millisecond):
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u.Start(ctx)
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait never returned after Start")
	}
	if _, ok := u.Registry()["000007.sst"]; !ok {
		t.Fatalf("Wait returned before the queued SST was uploaded: registry %v", u.Registry())
	}
}

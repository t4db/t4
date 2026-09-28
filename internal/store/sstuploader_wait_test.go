package store

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
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
			for i := 0; i < 2000; i++ {
				listener.FlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{FileNum: pebble.FileNum(g*10000 + i)}}})
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				u.Wait()
			}
		}()
	}
	wg.Wait()
	u.Wait()
}

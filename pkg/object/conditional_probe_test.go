package object_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/t4db/t4/internal/testutil"
	"github.com/t4db/t4/pkg/object"
)

func TestProbeAcceptsAStoreThatEnforcesBothPreconditions(t *testing.T) {
	support, err := object.ProbeConditional(context.Background(), object.NewMem(), "node-a")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !support.Enforced() {
		t.Fatalf("Mem enforces both preconditions, got %+v", support)
	}
	if support.Detail != "" {
		t.Errorf("detail should be empty when both hold, got %q", support.Detail)
	}
}

func TestProbeCatchesAStoreThatIgnoresBothPreconditions(t *testing.T) {
	store := testutil.NewLyingStore(object.NewMem())

	support, err := object.ProbeConditional(context.Background(), store, "node-a")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if support.Enforced() {
		t.Fatal("a store that ignores both preconditions must not be reported as enforcing them")
	}
	if support.IfNoneMatch {
		t.Error("If-None-Match is ignored by this store")
	}
	if support.IfMatch {
		t.Error("If-Match is ignored by this store")
	}
	for _, want := range []string{"If-None-Match", "If-Match"} {
		if !strings.Contains(support.Detail, want) {
			t.Errorf("detail should name %s, got %q", want, support.Detail)
		}
	}
}

func TestProbeCatchesEachPreconditionSeparately(t *testing.T) {
	t.Run("only If-None-Match ignored", func(t *testing.T) {
		store := &testutil.LyingStore{ConditionalStore: object.NewMem(), IgnoreIfNoneMatch: true}
		support, err := object.ProbeConditional(context.Background(), store, "node-a")
		if err != nil {
			t.Fatalf("probe: %v", err)
		}
		if support.IfNoneMatch {
			t.Error("If-None-Match is ignored by this store")
		}
		if !support.IfMatch {
			t.Error("If-Match is enforced by this store")
		}
		if support.Enforced() {
			t.Error("one ignored precondition is still not enforced")
		}
	})

	t.Run("only If-Match ignored", func(t *testing.T) {
		store := &testutil.LyingStore{ConditionalStore: object.NewMem(), IgnoreIfMatch: true}
		support, err := object.ProbeConditional(context.Background(), store, "node-a")
		if err != nil {
			t.Fatalf("probe: %v", err)
		}
		if !support.IfNoneMatch {
			t.Error("If-None-Match is enforced by this store")
		}
		if support.IfMatch {
			t.Error("If-Match is ignored by this store")
		}
	})
}

// A store that refuses conditional writes outright is the case T4 could
// already see (#256, older radosgw). The probe must report it as not enforcing
// rather than mistake the error for enforcement.
func TestProbeReportsAStoreThatRejectsConditionalWrites(t *testing.T) {
	t.Run("rejects both", func(t *testing.T) {
		store := &testutil.RejectingStore{
			ConditionalStore: object.NewMem(),
			Err:              errors.New("501 NotImplemented"),
		}
		support, err := object.ProbeConditional(context.Background(), store, "node-a")
		// Step 1 cannot create the key, so the probe reaches no conclusion.
		if err == nil {
			t.Fatalf("a store that cannot create the probe key is inconclusive, got %+v", support)
		}
		if support.Enforced() {
			t.Fatal("must not report enforcement")
		}
	})

	t.Run("rejects If-Match only", func(t *testing.T) {
		store := &testutil.RejectingStore{
			ConditionalStore:  object.NewMem(),
			Err:               errors.New("501 NotImplemented"),
			RejectIfMatchOnly: true,
		}
		support, err := object.ProbeConditional(context.Background(), store, "node-a")
		if err != nil {
			t.Fatalf("probe: %v", err)
		}
		if !support.IfNoneMatch {
			t.Error("If-None-Match still works on this store")
		}
		if support.IfMatch {
			t.Error("If-Match is rejected by this store")
		}
		if !strings.Contains(support.Detail, "rejected rather than honored") {
			t.Errorf("detail should distinguish rejection from being ignored, got %q", support.Detail)
		}
	})
}

// An ETag that never moves cannot identify a version, so If-Match cannot mean
// anything even though a mismatched write is refused.
func TestProbeRejectsAStoreWhoseETagNeverChanges(t *testing.T) {
	store := &testutil.FrozenETagStore{ConditionalStore: object.NewMem()}

	support, err := object.ProbeConditional(context.Background(), store, "node-a")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if support.IfMatch {
		t.Fatal("an ETag that never changes cannot support compare-and-swap")
	}
	if !strings.Contains(support.Detail, "same ETag") {
		t.Errorf("detail should say the ETag did not move, got %q", support.Detail)
	}
}

func TestProbeReportsAStoreWithNoConditionalWritesAtAll(t *testing.T) {
	support, err := object.ProbeConditional(context.Background(), plainStore{object.NewMem()}, "node-a")
	if err != nil {
		t.Fatalf("a store without conditional writes is a conclusion, not an error: %v", err)
	}
	if support.Enforced() {
		t.Fatal("must not report enforcement")
	}
	if support.Detail == "" {
		t.Error("detail should explain why")
	}
}

// The probe must leave nothing behind, so a start does not add an object to the
// store on every boot.
func TestProbeCleansUpItsScratchObject(t *testing.T) {
	mem := object.NewMem()
	before, err := mem.List(context.Background(), "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if _, err := object.ProbeConditional(context.Background(), mem, "node-a"); err != nil {
		t.Fatalf("probe: %v", err)
	}

	after, err := mem.List(context.Background(), "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("probe left %d object(s) behind: %v", len(after)-len(before), after)
	}
}

// Two nodes probing the same store at the same time must not fail each other,
// which they would if they shared one scratch key: both would clear it, then
// one of the two creates would be refused and that probe would reach no
// conclusion. barrierStore forces exactly that interleaving, so this does not
// depend on timing.
func TestProbeDoesNotCollideBetweenNodes(t *testing.T) {
	store := newBarrierStore(object.NewMem(), 2)

	type result struct {
		support object.ConditionalSupport
		err     error
	}
	results := make(chan result, 2)
	for _, id := range []string{"node-a", "node-b"} {
		go func(id string) {
			s, err := object.ProbeConditional(context.Background(), store, id)
			results <- result{s, err}
		}(id)
	}
	for i := 0; i < 2; i++ {
		got := <-results
		if got.err != nil {
			t.Fatalf("concurrent probe: %v", got.err)
		}
		if !got.support.Enforced() {
			t.Fatalf("concurrent probe misreported the store: %+v", got.support)
		}
	}
}

// barrierStore makes the probes' create steps overlap, with no reliance on
// timing. The first waitFor callers into PutIfAbsent wait until all of them
// have arrived, each then performs its create, and all of them wait again
// before returning. The second rendezvous is the load-bearing one: without it
// a released caller can still find that another probe has already finished and
// deleted the key, which is what makes a shared key look safe. Later calls pass
// straight through, so the rest of each probe runs normally.
type barrierStore struct {
	object.ConditionalStore
	waitFor int
	mu      sync.Mutex
	arrived int
	done    int
	allHere chan struct{}
	allDone chan struct{}
}

func newBarrierStore(inner object.ConditionalStore, waitFor int) *barrierStore {
	return &barrierStore{
		ConditionalStore: inner,
		waitFor:          waitFor,
		allHere:          make(chan struct{}),
		allDone:          make(chan struct{}),
	}
}

func (s *barrierStore) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	s.mu.Lock()
	s.arrived++
	n := s.arrived
	s.mu.Unlock()
	if n > s.waitFor {
		return s.ConditionalStore.PutIfAbsent(ctx, key, r)
	}

	if n == s.waitFor {
		close(s.allHere)
	} else {
		select {
		case <-s.allHere:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	err := s.ConditionalStore.PutIfAbsent(ctx, key, r)

	s.mu.Lock()
	s.done++
	d := s.done
	s.mu.Unlock()
	if d == s.waitFor {
		close(s.allDone)
	} else {
		select {
		case <-s.allDone:
		case <-ctx.Done():
		}
	}
	return err
}

// plainStore hides the conditional methods of the store it wraps.
type plainStore struct{ inner object.ConditionalStore }

func (p plainStore) Put(ctx context.Context, key string, r io.Reader) error {
	return p.inner.Put(ctx, key, r)
}
func (p plainStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return p.inner.Get(ctx, key)
}
func (p plainStore) Delete(ctx context.Context, key string) error { return p.inner.Delete(ctx, key) }
func (p plainStore) DeleteMany(ctx context.Context, keys []string) error {
	return p.inner.DeleteMany(ctx, keys)
}
func (p plainStore) List(ctx context.Context, prefix string) ([]string, error) {
	return p.inner.List(ctx, prefix)
}

var _ object.Store = plainStore{}

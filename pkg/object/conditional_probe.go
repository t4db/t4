package object

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// ConditionalSupport is what a store was observed to enforce, as opposed to
// what its Go type claims. A store satisfies ConditionalStore by having the
// methods; whether the server behind it honors the preconditions those methods
// send is a separate question, and the answer is not always yes:
//
//   - Older radosgw releases reject conditional writes outright. T4 already
//     copes with that, because a rejection is an error it can see.
//   - Google Cloud Storage, reached through its S3-compatible XML API, accepts
//     If-None-Match and If-Match and ignores both: the write succeeds
//     unconditionally and returns 200. Its native generation preconditions are
//     enforced, so the gap is in the S3 shim rather than in GCS.
//
// The second kind is the dangerous one. PutIfAbsent and PutIfMatch report
// success exactly when the store does not return 412 or 409, so a store that
// ignores the header looks to T4 like one that granted the write. Two nodes
// racing for the leader lock would then both be told they won it.
type ConditionalSupport struct {
	// Offered is false when the store does not provide conditional writes at
	// all. That case is not this probe's concern: T4 sees it for itself, since
	// election.Lock's own type assertion fails and it falls back to an
	// unconditional write with a read-back, and checkpoint.putManifest takes
	// its plain-write branch. Only a store that offers the preconditions and
	// does not keep them is invisible without asking.
	Offered bool
	// IfNoneMatch is true when a PutIfAbsent to a key that already exists was
	// refused, as the leader lock and WAL segment uploads need.
	IfNoneMatch bool
	// IfMatch is true when a PutIfMatch carrying a superseded ETag was
	// refused, as leader-lock renewal and manifest/latest need.
	IfMatch bool
	// Detail explains what was observed when either is false. It is meant for
	// an operator, and is empty when both hold.
	Detail string
}

// Enforced reports whether both preconditions were kept, which is what a
// multi-node cluster requires. It says nothing about a store that offers
// none: that store reports neither as kept, so callers test Offered first and
// treat the two cases differently.
func (c ConditionalSupport) Enforced() bool { return c.IfNoneMatch && c.IfMatch }

// conditionalProbePrefix holds the scratch object the probe writes. The key
// within it is per node, so repeated starts reuse one object rather than
// accumulating them, and two nodes probing at once never touch each other's.
const conditionalProbePrefix = ".t4-conditional-probe/"

// probeCleanupTimeout bounds the deferred delete of the scratch object. It is
// detached from the probe's own context so it runs even when that context is
// why the probe stopped, which is exactly when an unbounded wait would be
// worst.
const probeCleanupTimeout = 10 * time.Second

// ProbeConditional writes to a scratch object to find out which conditional
// writes store actually enforces.
//
// It is deliberately a write test. Nothing about a store's identity, endpoint
// or client type predicts the answer, and the failure it looks for is a write
// that succeeds when it should have been refused — which cannot be observed
// without attempting one.
//
// The scratch object is deleted afterwards. A process killed mid-probe can
// leave one small object under conditionalProbePrefix; the next start by the
// same node overwrites it.
//
// An error means the probe could not reach a conclusion, which is not the same
// as the store failing it: callers should treat an error as "unknown" and say
// so, rather than as "unsafe". A store that does not implement ConditionalStore
// at all is reported as enforcing neither, with no error, since that is a
// conclusion.
func ProbeConditional(ctx context.Context, store Store, nodeID string) (ConditionalSupport, error) {
	cs, ok := store.(ConditionalStore)
	if !ok {
		return ConditionalSupport{Detail: "the store does not offer conditional writes at all"}, nil
	}
	// Everything below is about a store that does offer them.

	key := conditionalProbePrefix + probeName(nodeID)
	// Start from a known-absent key, so step 1 below tests what it means to.
	if err := store.Delete(ctx, key); err != nil {
		return ConditionalSupport{Offered: true}, fmt.Errorf("object: probe: clear %q: %w", key, err)
	}
	defer func() {
		// Detached from ctx, because the cleanup should still run when ctx
		// expiring is why we stopped — but bounded, so a dead store cannot
		// hang startup here after the probe itself gave up.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), probeCleanupTimeout)
		defer cancel()
		_ = store.Delete(cleanupCtx, key)
	}()

	support := ConditionalSupport{Offered: true}

	// 1. Create the key. This must succeed; a store that refuses here is
	//    telling us nothing about preconditions, so it is an inconclusive
	//    probe rather than a verdict.
	if err := cs.PutIfAbsent(ctx, key, bytes.NewReader([]byte("1"))); err != nil {
		return ConditionalSupport{Offered: true}, fmt.Errorf("object: probe: create %q: %w", key, err)
	}

	// 2. Create it again. If-None-Match: * must refuse this.
	switch err := cs.PutIfAbsent(ctx, key, bytes.NewReader([]byte("2"))); {
	case err == nil:
		support.Detail = "a create-if-absent write to a key that already exists succeeded, so If-None-Match: * is ignored"
	case errors.Is(err, ErrPreconditionFailed):
		support.IfNoneMatch = true
	default:
		support.Detail = fmt.Sprintf("a create-if-absent write to a key that already exists failed with %v rather than a precondition error, so If-None-Match: * is not usable", err)
	}

	etag1, err := probeETag(ctx, cs, key)
	if err != nil {
		return support, fmt.Errorf("object: probe: read ETag of %q: %w", key, err)
	}

	// 3. Overwrite with the current ETag. This must succeed, or the store
	//    rejects If-Match rather than ignoring it — the radosgw case.
	if err := cs.PutIfMatch(ctx, key, bytes.NewReader([]byte("3")), etag1); err != nil {
		support.Detail = joinDetail(support.Detail, fmt.Sprintf(
			"a compare-and-swap write carrying the object's current ETag failed with %v, so If-Match is rejected rather than honored", err))
		return support, nil
	}

	// 4. The ETag must have moved with the content, or it cannot identify a
	//    version and If-Match cannot mean anything.
	etag2, err := probeETag(ctx, cs, key)
	if err != nil {
		return support, fmt.Errorf("object: probe: re-read ETag of %q: %w", key, err)
	}
	if etag2 == etag1 {
		support.Detail = joinDetail(support.Detail,
			"the store returned the same ETag before and after the content changed, so an ETag cannot identify a version and If-Match cannot be relied on")
		return support, nil
	}

	// 5. Overwrite with the ETag that step 3 superseded. If-Match must refuse
	//    this. A store that accepts it would let two writers who read the same
	//    version both believe they won.
	switch err := cs.PutIfMatch(ctx, key, bytes.NewReader([]byte("4")), etag1); {
	case err == nil:
		support.Detail = joinDetail(support.Detail,
			"a compare-and-swap write carrying a superseded ETag succeeded, so If-Match is ignored")
	case errors.Is(err, ErrPreconditionFailed):
		support.IfMatch = true
	default:
		support.Detail = joinDetail(support.Detail, fmt.Sprintf(
			"a compare-and-swap write carrying a superseded ETag failed with %v rather than a precondition error, so If-Match is not usable", err))
	}
	return support, nil
}

// probeETag reads key's current ETag, discarding the body.
func probeETag(ctx context.Context, cs ConditionalStore, key string) (string, error) {
	got, err := cs.GetETag(ctx, key)
	if err != nil {
		return "", err
	}
	if got.Body != nil {
		_, _ = io.Copy(io.Discard, got.Body)
		_ = got.Body.Close()
	}
	if got.ETag == "" {
		return "", errors.New("the store returned an empty ETag")
	}
	return got.ETag, nil
}

// probeName makes nodeID safe to use as one key segment. Node IDs default to
// the hostname and are otherwise operator-supplied, so they may hold anything.
func probeName(nodeID string) string {
	var b strings.Builder
	for _, r := range nodeID {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	name := b.String()
	if name == "" {
		return "node"
	}
	if len(name) > 200 {
		name = name[:200]
	}
	return name
}

func joinDetail(existing, add string) string {
	if existing == "" {
		return add
	}
	return existing + "; " + add
}

// Package replicate copies an etcd v3 keyspace from a source to a target so
// that the target's revisions match the source's exactly: after source
// revision R is applied, the target is at revision R with the same keys,
// values, create/mod revisions, versions and leases.
//
// Both ends are plain etcd v3 endpoints (T4 or etcd), so the same code
// replicates T4 to etcd, etcd to T4, or etcd to etcd. Matching revisions let
// Kubernetes switch from the source to the target without its clients
// noticing: resourceVersions, watches and optimistic concurrency carry over.
//
// Each source revision is applied as one target transaction guarded by a
// cursor key stored on the target:
//
//	If:   ModRevision(cursor) == R-1
//	Then: the revision's events, Put(cursor, R)
//
// The guard makes retries idempotent and keeps two replicators from
// interleaving. After every transaction the replicator checks that the target
// is at exactly revision R; any divergence stops replication instead of
// being papered over.
package replicate

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DefaultStatePrefix is where the replicator keeps its state on the target.
// Source keys under it are not replicated.
const DefaultStatePrefix = "/__t4_replication/"

// Config configures a Replicator.
type Config struct {
	Source *clientv3.Client
	Target *clientv3.Client

	// StatePrefix holds the cursor key on the target. Default
	// DefaultStatePrefix.
	StatePrefix string

	// LeaseTTLMargin is added to a lease's TTL on the target. The target must
	// never expire a lease before the source does, since that would create a
	// revision the source does not have; the margin must exceed the longest
	// tolerated replication lag or replicator downtime. Default 10m.
	LeaseTTLMargin time.Duration

	// ReconcileInterval is how often target leases are kept alive and leases
	// gone from the source are revoked on the target. Default 5s.
	ReconcileInterval time.Duration

	// ProgressDelay is how long the last received revision may wait for
	// proof that it is complete before the source is asked for a progress
	// notification. Default 50ms.
	ProgressDelay time.Duration

	// RetryInterval is the pause before reconnecting after a transient
	// error. Default 1s.
	RetryInterval time.Duration

	Log     logrus.FieldLogger
	Metrics *Metrics
}

// HaltError reports a condition replication cannot recover from without an
// operator, such as a target that diverged from the source.
type HaltError struct{ msg string }

func (e *HaltError) Error() string { return "replication halted: " + e.msg }

func halt(format string, args ...any) error {
	return &HaltError{msg: fmt.Sprintf(format, args...)}
}

// Replicator replicates Source into Target.
type Replicator struct {
	cfg       Config
	cursorKey string
	log       logrus.FieldLogger

	applied      atomic.Int64 // source revision the target is at
	cursorExists bool

	leases    map[clientv3.LeaseID]struct{} // leases mirrored on the target
	grantedTT map[clientv3.LeaseID]int64    // source granted TTL, seconds
}

// New validates cfg and returns a Replicator.
func New(cfg Config) (*Replicator, error) {
	if cfg.Source == nil || cfg.Target == nil {
		return nil, errors.New("replicate: source and target clients are required")
	}
	if cfg.StatePrefix == "" {
		cfg.StatePrefix = DefaultStatePrefix
	}
	if !strings.HasSuffix(cfg.StatePrefix, "/") {
		return nil, fmt.Errorf("replicate: state prefix %q must end with /", cfg.StatePrefix)
	}
	if cfg.LeaseTTLMargin <= 0 {
		cfg.LeaseTTLMargin = 10 * time.Minute
	}
	if cfg.ReconcileInterval <= 0 {
		cfg.ReconcileInterval = 5 * time.Second
	}
	if cfg.ProgressDelay <= 0 {
		cfg.ProgressDelay = 50 * time.Millisecond
	}
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = time.Second
	}
	if cfg.Log == nil {
		cfg.Log = logrus.StandardLogger()
	}
	return &Replicator{
		cfg:       cfg,
		cursorKey: cfg.StatePrefix + "cursor",
		log:       cfg.Log,
	}, nil
}

// Applied returns the source revision the target is at.
func (r *Replicator) Applied() int64 { return r.applied.Load() }

// Run replicates until ctx is done or a HaltError occurs. Transient errors,
// such as a lost connection, are retried: every attempt restarts from the
// cursor stored on the target.
func (r *Replicator) Run(ctx context.Context) error {
	for {
		err := r.follow(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var h *HaltError
		if errors.As(err, &h) {
			if r.cfg.Metrics != nil {
				r.cfg.Metrics.Halted.Set(1)
			}
			return err
		}
		r.log.WithError(err).Warn("replicate: retrying")
		r.countError("follow")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.cfg.RetryInterval):
		}
	}
}

// follow loads the replication state from the target and applies source
// revisions until an error.
func (r *Replicator) follow(ctx context.Context) error {
	if err := r.loadCursor(ctx); err != nil {
		return err
	}
	if err := r.loadTargetLeases(ctx); err != nil {
		return err
	}
	r.log.WithField("revision", r.Applied()).Info("replicate: following source")

	wctx, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
	defer cancel()
	wch := r.cfg.Source.Watch(wctx, "", clientv3.WithPrefix(), clientv3.WithRev(r.Applied()+1),
		clientv3.WithProgressNotify())

	var (
		pending    []*clientv3.Event
		pendingRev int64
	)
	flush := func() error {
		err := r.apply(ctx, pendingRev, pending)
		pending, pendingRev = nil, 0
		return err
	}
	progress := time.NewTimer(r.cfg.ProgressDelay)
	progress.Stop()
	reconcile := time.NewTicker(r.cfg.ReconcileInterval)
	defer reconcile.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-reconcile.C:
			if err := r.reconcileLeases(ctx); err != nil {
				return err
			}
			// Keeps the observed source revision fresh while idle.
			_ = r.cfg.Source.RequestProgress(wctx)

		case <-progress.C:
			if pendingRev != 0 {
				if err := r.cfg.Source.RequestProgress(wctx); err != nil {
					return fmt.Errorf("request progress: %w", err)
				}
			}

		case resp, ok := <-wch:
			if !ok {
				return errors.New("source watch closed")
			}
			if resp.Canceled && resp.Err() == nil {
				return errors.New("source watch canceled")
			}
			if resp.CompactRevision != 0 {
				return halt("source compacted revision %d before it was replicated (target at %d); the target must be bootstrapped again",
					resp.CompactRevision, r.Applied())
			}
			if err := resp.Err(); err != nil {
				if errors.Is(err, rpctypes.ErrCompacted) {
					return halt("source history needed to resume from revision %d is compacted; the target must be bootstrapped again", r.Applied()+1)
				}
				return fmt.Errorf("source watch: %w", err)
			}
			r.observeSource(resp.Header.Revision)

			// A revision is complete once a later revision's events arrive
			// or a progress notification covers it; a response boundary is
			// not proof, as catch-up responses may end mid-history.
			if resp.IsProgressNotify() {
				if pendingRev != 0 && resp.Header.Revision >= pendingRev {
					if err := flush(); err != nil {
						return err
					}
				}
				continue
			}
			for _, ev := range resp.Events {
				rev := ev.Kv.ModRevision
				if rev != pendingRev {
					if pendingRev != 0 {
						if rev < pendingRev {
							return halt("source sent revision %d after %d", rev, pendingRev)
						}
						if err := flush(); err != nil {
							return err
						}
					}
					pendingRev = rev
				}
				pending = append(pending, ev)
			}
			if pendingRev != 0 {
				progress.Reset(r.cfg.ProgressDelay)
			}
		}
	}
}

// loadCursor sets the applied revision from the target's cursor. A target
// without a cursor must be empty: replication then starts from the source's
// first revision.
func (r *Replicator) loadCursor(ctx context.Context) error {
	resp, err := r.cfg.Target.Get(ctx, r.cursorKey)
	if err != nil {
		return fmt.Errorf("read target cursor: %w", err)
	}
	if len(resp.Kvs) == 1 {
		kv := resp.Kvs[0]
		rev, err := strconv.ParseInt(string(kv.Value), 10, 64)
		if err != nil {
			return halt("target cursor %s has invalid value %q", r.cursorKey, kv.Value)
		}
		if kv.ModRevision != rev {
			return halt("target cursor says revision %d but was written at revision %d", rev, kv.ModRevision)
		}
		if resp.Header.Revision != rev {
			return halt("target is at revision %d but its cursor says %d: something other than the replicator wrote to it", resp.Header.Revision, rev)
		}
		r.cursorExists = true
		r.setApplied(rev)
		return nil
	}

	count, err := r.cfg.Target.Get(ctx, "", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return fmt.Errorf("check target is empty: %w", err)
	}
	if count.Count != 0 || count.Header.Revision != 1 {
		return halt("target has %d keys at revision %d but no replication cursor (%s); replicate into an empty target or bootstrap it",
			count.Count, count.Header.Revision, r.cursorKey)
	}
	r.cursorExists = false
	r.setApplied(1)
	return nil
}

// apply commits source revision rev to the target.
func (r *Replicator) apply(ctx context.Context, rev int64, events []*clientv3.Event) error {
	applied := r.Applied()
	if rev != applied+1 {
		return halt("source revision %d follows %d: revisions %d..%d produced no events. The source spends revisions etcd would not, "+
			"e.g. a T4 database created before the meta keyspace; such a source cannot be replicated revision-exactly",
			rev, applied, applied+1, rev-1)
	}
	start := time.Now()
	if err := r.ensureLeases(ctx, events); err != nil {
		return err
	}

	ops := make([]clientv3.Op, 0, len(events)+1)
	for _, ev := range events {
		key := string(ev.Kv.Key)
		if strings.HasPrefix(key, r.cfg.StatePrefix) {
			continue
		}
		switch ev.Type {
		case mvccpb.PUT:
			var opts []clientv3.OpOption
			if ev.Kv.Lease != 0 {
				opts = append(opts, clientv3.WithLease(clientv3.LeaseID(ev.Kv.Lease)))
			}
			ops = append(ops, clientv3.OpPut(key, string(ev.Kv.Value), opts...))
		case mvccpb.DELETE:
			ops = append(ops, clientv3.OpDelete(key))
		}
	}
	ops = append(ops, clientv3.OpPut(r.cursorKey, strconv.FormatInt(rev, 10)))

	guard := clientv3.Compare(clientv3.CreateRevision(r.cursorKey), "=", 0)
	if r.cursorExists {
		guard = clientv3.Compare(clientv3.ModRevision(r.cursorKey), "=", applied)
	}
	resp, err := r.cfg.Target.Txn(ctx).If(guard).Then(ops...).Commit()
	if err != nil {
		if errors.Is(err, rpctypes.ErrTooManyOps) || errors.Is(err, rpctypes.ErrRequestTooLarge) {
			return halt("source revision %d (%d events) exceeds the target's transaction limits: %v; raise etcd's --max-txn-ops / --max-request-bytes",
				rev, len(events), err)
		}
		// The outcome is unknown; the next attempt re-reads the cursor.
		return fmt.Errorf("apply revision %d: %w", rev, err)
	}
	if !resp.Succeeded {
		return halt("target cursor moved away from revision %d: another replicator or a stray write changed the target", applied)
	}
	if resp.Header.Revision != rev {
		return halt("target is at revision %d after applying source revision %d: the target has diverged", resp.Header.Revision, rev)
	}
	r.cursorExists = true
	r.setApplied(rev)
	if m := r.cfg.Metrics; m != nil {
		m.AppliedTotal.Inc()
		m.ApplyDuration.Observe(time.Since(start).Seconds())
	}
	return nil
}

// ensureLeases grants on the target, with the same IDs, every lease the
// events attach that the target does not have yet. Granting a lease does not
// change the revision.
func (r *Replicator) ensureLeases(ctx context.Context, events []*clientv3.Event) error {
	for _, ev := range events {
		id := clientv3.LeaseID(ev.Kv.Lease)
		if ev.Type != mvccpb.PUT || id == 0 {
			continue
		}
		if _, ok := r.leases[id]; ok {
			continue
		}
		if err := r.grantTarget(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (r *Replicator) grantTarget(ctx context.Context, id clientv3.LeaseID) error {
	ttl, err := r.sourceGrantedTTL(ctx, id)
	if err != nil {
		return err
	}
	// clientv3 cannot choose the lease ID; the raw RPC can.
	_, err = etcdserverpb.NewLeaseClient(r.cfg.Target.ActiveConnection()).LeaseGrant(ctx,
		&etcdserverpb.LeaseGrantRequest{ID: int64(id), TTL: ttl + int64(r.cfg.LeaseTTLMargin/time.Second)})
	if err != nil && !leaseExists(err) {
		return fmt.Errorf("grant lease %x on target: %w", id, err)
	}
	r.leases[id] = struct{}{}
	r.updateLeaseMetric()
	return nil
}

// sourceGrantedTTL returns the TTL the source granted lease id with, or 0 if
// the source no longer has it (its keys are then about to be deleted).
func (r *Replicator) sourceGrantedTTL(ctx context.Context, id clientv3.LeaseID) (int64, error) {
	if ttl, ok := r.grantedTT[id]; ok {
		return ttl, nil
	}
	resp, err := r.cfg.Source.TimeToLive(ctx, id)
	if err != nil {
		if leaseNotFound(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("read lease %x on source: %w", id, err)
	}
	if resp.TTL < 0 { // etcd reports a missing lease as TTL -1
		return 0, nil
	}
	r.grantedTT[id] = resp.GrantedTTL
	return resp.GrantedTTL, nil
}

func (r *Replicator) loadTargetLeases(ctx context.Context) error {
	resp, err := r.cfg.Target.Leases(ctx)
	if err != nil {
		return fmt.Errorf("list target leases: %w", err)
	}
	r.leases = make(map[clientv3.LeaseID]struct{}, len(resp.Leases))
	r.grantedTT = make(map[clientv3.LeaseID]int64)
	for _, l := range resp.Leases {
		r.leases[l.ID] = struct{}{}
	}
	r.updateLeaseMetric()
	return nil
}

// reconcileLeases keeps target leases alive while the source has them,
// grants leases the source has before any key uses them, and revokes target
// leases the source no longer has once their keys are gone (revoking a lease
// without keys does not change the revision). Leases whose keys remain are
// left alone: the source's deletions of those keys are still on their way.
func (r *Replicator) reconcileLeases(ctx context.Context) error {
	src, err := r.cfg.Source.Leases(ctx)
	if err != nil {
		return fmt.Errorf("list source leases: %w", err)
	}
	onSource := make(map[clientv3.LeaseID]struct{}, len(src.Leases))
	for _, l := range src.Leases {
		onSource[l.ID] = struct{}{}
		if _, ok := r.leases[l.ID]; ok {
			if _, err := r.cfg.Target.KeepAliveOnce(ctx, l.ID); err != nil {
				if leaseNotFound(err) {
					return halt("lease %x expired on the target while the source still has it; the target has diverged (raise --lease-ttl-margin)", l.ID)
				}
				return fmt.Errorf("keep lease %x alive on target: %w", l.ID, err)
			}
			continue
		}
		if err := r.grantTarget(ctx, l.ID); err != nil {
			return err
		}
	}
	for id := range r.leases {
		if _, ok := onSource[id]; ok {
			continue
		}
		ttl, err := r.cfg.Target.TimeToLive(ctx, id, clientv3.WithAttachedKeys())
		if err != nil && !leaseNotFound(err) {
			return fmt.Errorf("read lease %x on target: %w", id, err)
		}
		if err == nil && ttl.TTL >= 0 && len(ttl.Keys) > 0 {
			continue
		}
		if err == nil && ttl.TTL >= 0 {
			if _, err := r.cfg.Target.Revoke(ctx, id); err != nil && !leaseNotFound(err) {
				return fmt.Errorf("revoke lease %x on target: %w", id, err)
			}
		}
		delete(r.leases, id)
		delete(r.grantedTT, id)
	}
	r.updateLeaseMetric()
	return nil
}

// grpcCode returns err's gRPC code, whether clientv3 converted it to an
// rpctypes.EtcdError or not.
func grpcCode(err error) codes.Code {
	var ee rpctypes.EtcdError
	if errors.As(err, &ee) {
		return ee.Code()
	}
	return status.Code(err)
}

// leaseNotFound matches etcd's and T4's "lease not found" errors.
func leaseNotFound(err error) bool { return grpcCode(err) == codes.NotFound }

// leaseExists matches etcd's (FailedPrecondition) and T4's (AlreadyExists)
// "lease already exists" errors.
func leaseExists(err error) bool {
	return errors.Is(rpctypes.Error(err), rpctypes.ErrLeaseExist) || grpcCode(err) == codes.AlreadyExists
}

func (r *Replicator) setApplied(rev int64) {
	r.applied.Store(rev)
	if m := r.cfg.Metrics; m != nil {
		m.AppliedRevision.Set(float64(rev))
	}
}

func (r *Replicator) observeSource(rev int64) {
	if m := r.cfg.Metrics; m != nil {
		m.SourceRevision.Set(float64(rev))
		lag := rev - r.Applied()
		if lag < 0 {
			lag = 0
		}
		m.LagRevisions.Set(float64(lag))
	}
}

func (r *Replicator) updateLeaseMetric() {
	if m := r.cfg.Metrics; m != nil {
		m.TargetLeases.Set(float64(len(r.leases)))
	}
}

func (r *Replicator) countError(op string) {
	if m := r.cfg.Metrics; m != nil {
		m.Errors.WithLabelValues(op).Inc()
	}
}

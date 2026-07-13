package t4

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/internal/metrics"
	"github.com/t4db/t4/internal/peer"
	istore "github.com/t4db/t4/internal/store"
	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// Sentinel errors.
var (
	ErrKeyExists      = errors.New("t4: key already exists")
	ErrNotLeader      = errors.New("t4: this node is not the leader; writes are rejected")
	ErrNoLeader       = errors.New("t4: no leader")
	ErrClosed         = errors.New("t4: node is closed")
	ErrCompacted      = istore.ErrCompacted
	ErrFutureRevision = istore.ErrFutureRevision
)

// TxnCondTarget identifies which field of a key's metadata is compared.
type TxnCondTarget uint8

const (
	TxnCondMod     TxnCondTarget = iota // compare ModRevision
	TxnCondVersion                      // compare Version (write count; 0 = does not exist)
	TxnCondCreate                       // compare CreateRevision
	TxnCondValue                        // compare Value bytes
	TxnCondLease                        // compare Lease ID
)

// TxnCondResult is the comparison operator for a TxnCondition.
type TxnCondResult uint8

const (
	TxnCondEqual    TxnCondResult = iota
	TxnCondNotEqual               //nolint:deadcode
	TxnCondGreater
	TxnCondLess
)

// TxnCondition is one predicate in a transaction's If clause.
// All conditions in a TxnRequest are evaluated atomically under the write
// lock; no revision can change between evaluating the first and last condition.
type TxnCondition struct {
	Key    string
	Target TxnCondTarget
	Result TxnCondResult
	// Exactly one of the following is used, depending on Target:
	ModRevision    int64
	CreateRevision int64
	Version        int64 // number of writes to key; 0 means key does not exist
	Value          []byte
	Lease          int64
}

// TxnOpType identifies the kind of operation within a transaction branch.
type TxnOpType uint8

const (
	TxnPut    TxnOpType = iota // upsert
	TxnDelete                  // unconditional delete
)

// TxnOp is one write operation in a transaction's Then or Else branch.
type TxnOp struct {
	Type  TxnOpType
	Key   string
	Value []byte
	Lease int64
}

// TxnRequest is the input to Node.Txn.
// If all Conditions are met the Success ops are applied atomically; otherwise
// the Failure ops are applied atomically (may be empty for a read-only else).
type TxnRequest struct {
	Conditions []TxnCondition
	Success    []TxnOp
	Failure    []TxnOp
}

// TxnResponse is returned by Node.Txn.
type TxnResponse struct {
	Succeeded   bool                // true if all Conditions were satisfied
	Revision    int64               // revision assigned to the write, or current revision if no-op
	DeletedKeys map[string]struct{} // set of keys actually removed by the txn's write ops
}

// nodeRole identifies whether the node is leader, follower, or single-node.
type nodeRole int32

const (
	roleSingle   nodeRole = iota // ObjectStore nil or PeerListenAddr empty
	roleLeader                   // elected leader
	roleFollower                 // following a remote leader
)

// Node is the top-level T4 instance.
//
// Single-node mode (PeerListenAddr == ""):
//
//	Writes: WAL.Append (fsync) → store.Apply → notify watchers
//	Background: WAL segments uploaded to S3, periodic checkpoints
//
// Leader mode:
//
//	Same as single-node, plus fan-out to followers via peer gRPC stream.
//	Holds the S3 leader lock; watches it infrequently for supersession.
//
// Follower mode:
//
//	Reads are served locally. Writes are forwarded to the leader via the
//	peer gRPC channel and the response is returned transparently to the caller.
//	After persistent stream failure, attempts a TakeOver election.
//
// writeReq is a single write request sent to the commit loop.
type writeReq struct {
	entry wal.Entry
	done  chan error
	ctx   context.Context
}

func newWriteReq(ctx context.Context, e wal.Entry) *writeReq {
	return &writeReq{entry: e, done: make(chan error, 1), ctx: ctx}
}

// pendingKV tracks an in-flight write that has been assigned a revision and
// queued to the commit loop but not yet applied to Pebble. Reads under n.mu
// check this map first so concurrent writes to the same key see each other's
// in-progress state.
type pendingKV struct {
	rev     int64
	deleted bool             // true for pending deletes
	kv      *istore.KeyValue // valid when !deleted
}

type Node struct {
	cfg  Config
	log  Logger
	cp   *checkpoint.Manager
	term uint64
	role atomic.Int32 // stores nodeRole values; use loadRole/storeRole

	db  atomic.Pointer[istore.Store]
	wal WALWriter // non-nil on leader/single; non-nil on follower (local WAL, no uploader)

	// mu serialises all leader writes for CAS safety and role transitions.
	mu sync.Mutex

	// fenceMu is a read-write mutex used to briefly pause leader writes while
	// the node verifies it is still the elected leader (after a follower
	// disconnects). Write methods hold RLock for their full duration; the
	// watchLoop holds Lock while performing the S3 lock check so that writes
	// are drained and no new ones start until the check completes.
	fenceMu sync.RWMutex

	// nextRev is the last revision assigned to a write. Incremented under mu
	// before the entry is sent to the commit loop. Replaces n.db.Load().CurrentRevision()+1
	// on the write path so that in-flight entries have distinct revisions.
	nextRev int64

	// nextSeq is the last WAL/peer-stream identity assigned to any entry.
	// Unlike nextRev, metadata-only entries such as Compact consume a sequence
	// number without consuming a user-visible revision.
	nextSeq int64

	// pending holds in-flight writes that have been assigned a revision but
	// not yet applied to Pebble. Protected by mu.
	pending map[string]pendingKV

	// writeC is the channel to the commit loop (group-commit WAL + Pebble apply).
	// Only used when the node is leader or single.
	writeC chan *writeReq

	// leader-only
	peerSrv  *peer.Server
	peerLis  net.Listener
	peerGRPC *grpc.Server

	// follower-only (WAL stream); owned exclusively by followLoop after startup.
	peerCli *peer.Client

	// follower-only (write forwarding); updated atomically when leader changes.
	leaderCli atomic.Pointer[peer.Client]

	entriesSinceCheckpoint int64
	checkpointTriggerC     chan struct{}       // non-nil when CheckpointEntries > 0; signals entry-count-based checkpoint
	sstUploader            *istore.SSTUploader // non-nil when ObjectStore is set; streams SSTs to S3
	lastRevisionSampleUnix int64               // unix nano timestamp of newest local revision/time sample

	// bgCtx is cancelled by cancelBg — either on Close() or when fencedCheck
	// detects that this node has been superseded as leader. When cancelled with
	// leaderCli still nil, the node is shutting down or has been fenced; reads
	// must return an error instead of serving data from stale local Pebble.
	bgCtx    context.Context
	cancelBg context.CancelFunc

	closeOnce sync.Once
	closed    atomic.Bool
	bgWg      sync.WaitGroup // tracks long-running background goroutines (followLoop, checkpointLoop)
	readMu    sync.RWMutex   // held RLock by in-flight reads; Lock taken by Close to drain them
}

func (n *Node) loadRole() nodeRole   { return nodeRole(n.role.Load()) }
func (n *Node) storeRole(r nodeRole) { n.role.Store(int32(r)) }

func (n *Node) observeTerm(term uint64) {
	n.mu.Lock()
	if term > n.term {
		n.term = term
	}
	n.mu.Unlock()
}

func (n *Node) currentTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.term
}

func applyObjectStoreEncryption(cfg *Config) error {
	if cfg.ObjectStoreEncryption == nil {
		return nil
	}
	if cfg.ObjectStoreEncryption.KeyProvider == nil {
		return fmt.Errorf("t4: object store encryption key provider is nil")
	}
	kp := cfg.ObjectStoreEncryption.KeyProvider
	wrap := func(s object.Store) object.Store {
		if s == nil {
			return nil
		}
		return object.NewEncryptedStore(s, kp)
	}
	cfg.ObjectStore = wrap(cfg.ObjectStore)
	cfg.AncestorStore = wrap(cfg.AncestorStore)
	if cfg.BranchPoint != nil {
		cfg.BranchPoint.SourceStore = wrap(cfg.BranchPoint.SourceStore)
	}
	if cfg.RestorePoint != nil && cfg.RestorePoint.Store != nil {
		wrapped := object.NewEncryptedStore(cfg.RestorePoint.Store, kp)
		vs, ok := wrapped.(object.VersionedStore)
		if !ok {
			return fmt.Errorf("t4: encrypted object-store restore source does not support versioned reads")
		}
		cfg.RestorePoint.Store = vs
	}
	return nil
}

// Open creates and starts a Node.
func Open(cfg Config) (*Node, error) {
	cfg.setDefaults()

	log := cfg.Logger
	cp := checkpoint.New(log)

	// Register all t4 metrics on the configured registerer.
	// When nil, metrics.Register falls back to prometheus.DefaultRegisterer.
	metrics.Register(cfg.MetricsRegisterer)

	if err := applyObjectStoreEncryption(&cfg); err != nil {
		return nil, err
	}

	// Wrap the object store with Prometheus instrumentation so every S3
	// operation is counted and timed without scattering metrics calls
	// throughout the codebase.
	if cfg.ObjectStore != nil {
		cfg.ObjectStore = object.NewInstrumentedStore(cfg.ObjectStore)
	}

	pebbleDir := filepath.Join(cfg.DataDir, "db")
	walDir := filepath.Join(cfg.DataDir, "wal")

	var (
		startRev int64
		term     uint64 = 1
	)
	// inheritedSSTs is populated during BranchPoint restore: maps SST filename
	// to the source store's S3 key.  Applied to the SSTUploader after open.
	var inheritedSSTs map[string]string

	// ── Restore checkpoint ───────────────────────────────────────────────────
	switch {
	case cfg.RestorePoint != nil:
		// Point-in-time restore from pinned S3 version IDs. Only applied on
		// first boot; subsequent restarts skip this block because pebbleDir
		// already exists.
		if _, err := os.Stat(pebbleDir); errors.Is(err, os.ErrNotExist) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			rp := cfg.RestorePoint
			if rp.CheckpointArchive.Key != "" {
				checkpointFiles := make(map[string]string, len(rp.CheckpointFiles))
				for i, obj := range rp.CheckpointFiles {
					if obj.Key == "" || obj.VersionID == "" {
						return nil, fmt.Errorf("t4: RestorePoint.CheckpointFiles[%d] has empty Key or VersionID (%+v); pin every referenced object or omit CheckpointFiles entirely", i, obj)
					}
					if existing, dup := checkpointFiles[obj.Key]; dup && existing != obj.VersionID {
						return nil, fmt.Errorf("t4: RestorePoint.CheckpointFiles has conflicting versions for key %q: %q and %q", obj.Key, existing, obj.VersionID)
					}
					checkpointFiles[obj.Key] = obj.VersionID
				}
				t, rev, err := cp.RestoreVersioned(ctx, rp.Store,
					rp.CheckpointArchive.Key, rp.CheckpointArchive.VersionID, checkpointFiles, pebbleDir)
				if err != nil {
					return nil, fmt.Errorf("t4: restore versioned checkpoint: %w", err)
				}
				term, startRev = t, rev
				log.Infof("t4: versioned checkpoint restored (term=%d rev=%d)", term, startRev)
			}
		}
	case cfg.BranchPoint != nil:
		if _, err := os.Stat(pebbleDir); errors.Is(err, os.ErrNotExist) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			bp := cfg.BranchPoint
			t, rev, err := cp.RestoreBranch(ctx, bp.SourceStore, nil, bp.CheckpointKey, pebbleDir)
			if err != nil {
				return nil, fmt.Errorf("t4: restore branch point: %w", err)
			}
			term, startRev = t, rev
			log.Infof("t4: branch checkpoint restored (term=%d rev=%d)", term, startRev)
			// Record the ancestor's SSTs so the uploader does not re-upload
			// them; they will be referenced via AncestorSSTFiles in the index.
			if cfg.ObjectStore != nil {
				if idx, idxErr := cp.ReadCheckpointIndex(ctx, bp.SourceStore, bp.CheckpointKey); idxErr == nil {
					inheritedSSTs = make(map[string]string, len(idx.SSTFiles))
					for _, key := range idx.SSTFiles {
						inheritedSSTs[filepath.Base(key)] = key
					}
				}
			}
		}
	case cfg.ObjectStore != nil:
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		manifest, err := cp.ReadManifest(ctx, cfg.ObjectStore)
		if err != nil {
			return nil, fmt.Errorf("t4: read manifest: %w", err)
		}
		if manifest != nil {
			log.Infof("t4: manifest found (rev=%d)", manifest.Revision)
			if _, err := os.Stat(pebbleDir); errors.Is(err, os.ErrNotExist) {
				// Retry loop: the checkpoint referenced by the manifest may
				// have been GC'd in the window between reading the manifest
				// and downloading it (most commonly when two leaders are
				// concurrently writing checkpoints during a leadership
				// transition). Re-reading the manifest gives us whichever
				// checkpoint the new leader most recently wrote.
				for attempt := 0; ; attempt++ {
					t, rev, rerr := cp.Restore(ctx, cfg.ObjectStore, manifest.CheckpointKey, pebbleDir)
					if rerr == nil {
						term, startRev = t, rev
						log.Infof("t4: checkpoint restored (term=%d rev=%d)", term, startRev)
						break
					}
					if !errors.Is(rerr, object.ErrNotFound) || attempt >= 4 {
						return nil, fmt.Errorf("t4: restore checkpoint: %w", rerr)
					}
					// Checkpoint was GC'd; sleep briefly so the leader can
					// write a new checkpoint and update the manifest before
					// we retry. (With a 400 ms checkpoint interval, 500 ms
					// gives a full interval of headroom.)
					log.Warnf("t4: checkpoint %q not found, re-reading manifest (attempt %d/5)", manifest.CheckpointKey, attempt+1)
					time.Sleep(500 * time.Millisecond)
					manifest, err = cp.ReadManifest(ctx, cfg.ObjectStore)
					if err != nil {
						return nil, fmt.Errorf("t4: read manifest: %w", err)
					}
					if manifest == nil {
						break // manifest disappeared; start fresh without checkpoint
					}
				}
			}
		}
	}

	// ── Open Pebble ──────────────────────────────────────────────────────────
	// Create the SST uploader before opening Pebble so its EventListener is
	// active from the first flush/compaction. Only used when S3 is configured.
	var sstUp *istore.SSTUploader
	var pebbleOpts []istore.PebbleOption
	pebbleOpts = append(pebbleOpts, func(o *pebble.Options) {
		o.Logger = &pebbleLogger{log: log}
	})
	pebbleOpts = append(pebbleOpts, cfg.PebbleOptions...)
	if cfg.ObjectStore != nil {
		sstUp = istore.NewSSTUploader(cfg.ObjectStore, pebbleDir)
		pebbleOpts = append(pebbleOpts, sstUp.PebbleOption())
	}

	db, err := istore.Open(pebbleDir, log, pebbleOpts...)
	if err != nil {
		return nil, fmt.Errorf("t4: open store: %w", err)
	}
	if sstUp != nil {
		if len(inheritedSSTs) > 0 {
			sstUp.SetInherited(inheritedSSTs)
		}
		reconcileCtx, reconcileCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer reconcileCancel()
		if err := sstUp.Reconcile(reconcileCtx); err != nil {
			log.Warnf("t4: SST reconcile: %v", err)
		}
	}
	// Always derive startRev from pebble's actual revision, not the checkpoint
	// header. A Compact entry does not write a pebble log key, so after a
	// restore loadMeta() returns a revision one less than the compact's revision
	// even though the checkpoint header records the compact's (higher) revision.
	// Using the checkpoint header as afterRev would cause replayRemote to skip
	// that compact entry. Local WAL replay (below) may advance pebble further,
	// so we re-read after replay as well.
	startRev = db.CurrentRevision()

	// ── Open WAL ─────────────────────────────────────────────────────────────
	var uploader wal.Uploader
	if cfg.ObjectStore != nil && cfg.PeerListenAddr == "" {
		uploader = makeUploader(cfg.ObjectStore, log)
	}

	opts := []wal.Option{
		wal.WithUploader(uploader),
		wal.WithSegmentMaxSize(cfg.SegmentMaxSize),
		wal.WithSegmentMaxAge(cfg.SegmentMaxAge),
		wal.WithLogger(log),
	}
	if cfg.ObjectStore != nil && *cfg.WALSyncUpload {
		opts = append(opts, wal.WithSyncUpload())
	}
	w := cfg.WAL
	if w == nil {
		w = wal.New(opts...)
	}
	// Seed nextSeq from the persisted lastSeq (set by Apply/Recover from
	// every WAL entry, including Compact). Falling back to startRev (which
	// is a user revision) would reuse sequence numbers after a checkpoint
	// taken post-Compact, breaking peer dedup. Take the max with the local
	// WAL's last sequence in case the local WAL is ahead of Pebble.
	nextSeq := db.LastSequence()
	if maxSeq, merr := wal.MaxSequence(walDir); maxSeq > nextSeq {
		// MaxSequence may return a partial max with an error when one local
		// segment is corrupt. Trust the partial value as a lower bound so the
		// next writer cannot reuse a sequence that was already present in WAL.
		nextSeq = maxSeq
		if merr != nil {
			log.Warnf("t4: scan local WAL sequence: %v", merr)
		}
	} else if merr != nil {
		log.Warnf("t4: scan local WAL sequence: %v", merr)
	}
	if err := w.Open(walDir, term, nextSeq+1); err != nil {
		db.Close()
		return nil, fmt.Errorf("t4: open wal: %w", err)
	}

	// ── Replay local WAL ─────────────────────────────────────────────────────
	// ReplayLocal filters by entry.Sequence(), so pass LastSequence — after a
	// post-compact checkpoint LastSequence > CurrentRevision and entries with
	// Sequence in (CurrentRevision, LastSequence] would otherwise be replayed
	// again (idempotent today, but inconsistent with the seq/rev split).
	if err := w.ReplayLocal(db, db.LastSequence()); err != nil {
		w.Close()
		db.Close()
		return nil, fmt.Errorf("t4: local WAL replay: %w", err)
	}
	// Re-read pebble's revision: local WAL replay may have advanced it.
	startRev = db.CurrentRevision()

	// ── Replay remote WAL (S3) ───────────────────────────────────────────────
	switch {
	case cfg.RestorePoint != nil:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := replayPinned(ctx, db, cfg.RestorePoint, db.LastSequence(), log); err != nil {
			w.Close()
			db.Close()
			return nil, fmt.Errorf("t4: pinned WAL replay: %w", err)
		}
	case cfg.BranchPoint != nil:
		// Branch nodes use their own ObjectStore for WAL replay after bootstrap.
		if cfg.ObjectStore != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			if err := replayRemote(ctx, db, cfg.ObjectStore, db.LastSequence(), log); err != nil {
				w.Close()
				db.Close()
				return nil, fmt.Errorf("t4: branch WAL replay: %w", err)
			}
		}
	case cfg.ObjectStore != nil:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		// Close the GC race: the leader checkpoints periodically (e.g. every
		// 400 ms in tests) and immediately GCs WAL segments covered by each
		// checkpoint. A bootstrapping node that started replaying WAL entries
		// from S3 can find mid-flight that some earlier segments were deleted
		// while it was reading later ones. The node then applies the later
		// segments (advancing currentRev past the gap) and later connects to
		// the leader with a fromRev that skips the missing range — those keys
		// are permanently absent.
		//
		// The fix: after each replayRemote call, re-read the manifest. If the
		// checkpoint advanced past startRevBeforeReplay, a GC could have
		// deleted segments we needed. Restore from the fresher checkpoint and
		// replay again. Repeat until the checkpoint no longer advances between
		// the start and end of replayRemote (meaning no GC occurred during the
		// replay). The loop converges quickly: in the worst case it runs once
		// per checkpoint interval that fires during replay.
		for range 10 { // cap at 10 iterations; converges in 1-2 on any real cluster
			startRevBeforeReplay := db.CurrentRevision()
			replayErr := replayRemote(ctx, db, cfg.ObjectStore, db.LastSequence(), log)

			// Always check manifest after replay, even on error: if a WAL
			// segment was GCed mid-read replayRemote returns ErrNotFound, but
			// that same GC event means a fresh checkpoint exists.  Re-restoring
			// from that checkpoint is the correct recovery; returning the error
			// directly would cause Open() to fail unnecessarily.
			freshManifest, merr := cp.ReadManifest(ctx, cfg.ObjectStore)

			if replayErr != nil {
				// If the checkpoint has not advanced there is no recovery path —
				// this is a genuine storage error.
				if merr != nil || freshManifest == nil || freshManifest.Revision <= startRevBeforeReplay {
					w.Close()
					db.Close()
					return nil, fmt.Errorf("t4: remote WAL replay: %w", replayErr)
				}
				log.Infof("t4: WAL replay error (%v), checkpoint advanced (%d→%d) — re-restoring to close GC gap",
					replayErr, startRevBeforeReplay, freshManifest.Revision)
			} else if merr != nil || freshManifest == nil || freshManifest.Revision <= db.CurrentRevision() {
				// Replay succeeded and the DB is at or ahead of the latest
				// checkpoint — no GC gap, done.  Using db.CurrentRevision()
				// (not startRevBeforeReplay) is important: when the last WAL
				// entry was an OpCompact, replayRemote advances currentRev but
				// does not write a log key, so startRevBeforeReplay stays at
				// the pre-compact value even after a successful replay.
				break
			} else {
				// Replay succeeded but checkpoint advanced — silent GC holes possible.
				log.Infof("t4: checkpoint advanced during WAL replay (%d→%d); re-restoring to close GC gap",
					startRevBeforeReplay, freshManifest.Revision)
			}
			db.Close()
			if rerr := os.RemoveAll(pebbleDir); rerr != nil {
				w.Close()
				return nil, fmt.Errorf("t4: remove stale pebble dir: %w", rerr)
			}
			var newTerm uint64
			var newRev int64
			manifest := freshManifest
			for attempt := range 5 {
				newTerm, newRev, err = cp.Restore(ctx, cfg.ObjectStore, manifest.CheckpointKey, pebbleDir)
				if err == nil {
					break
				}
				if !errors.Is(err, object.ErrNotFound) || attempt >= 4 {
					w.Close()
					return nil, fmt.Errorf("t4: re-restore checkpoint: %w", err)
				}
				time.Sleep(500 * time.Millisecond)
				manifest, err = cp.ReadManifest(ctx, cfg.ObjectStore)
				if err != nil || manifest == nil {
					w.Close()
					return nil, fmt.Errorf("t4: re-read manifest after GC: %w", err)
				}
			}
			_ = newRev // term drives WAL open; startRev is read from Pebble below
			freshDB, rerr := istore.Open(pebbleDir, log, pebbleOpts...)
			if rerr != nil {
				w.Close()
				return nil, fmt.Errorf("t4: reopen store after GC-gap fix: %w", rerr)
			}
			if sstUp != nil {
				reconcileCtx, reconcileCancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer reconcileCancel()
				if err := sstUp.Reconcile(reconcileCtx); err != nil {
					log.Warnf("t4: SST reconcile after GC-gap fix: %v", err)
				}
			}
			w.Close()
			reopenSeq := freshDB.LastSequence()
			if maxSeq, merr := wal.MaxSequence(walDir); maxSeq > reopenSeq {
				// See the startup scan above: a partial max is still a lower
				// bound that prevents WAL sequence reuse.
				reopenSeq = maxSeq
				if merr != nil {
					log.Warnf("t4: scan local WAL sequence after GC-gap fix: %v", merr)
				}
			} else if merr != nil {
				log.Warnf("t4: scan local WAL sequence after GC-gap fix: %v", merr)
			}
			if rerr := w.Open(walDir, newTerm, reopenSeq+1); rerr != nil {
				freshDB.Close()
				return nil, fmt.Errorf("t4: reopen wal after GC-gap fix: %w", rerr)
			}
			db, term, startRev, nextSeq = freshDB, newTerm, freshDB.CurrentRevision(), reopenSeq
			log.Infof("t4: checkpoint refreshed to rev=%d (term=%d)", startRev, term)
		}
	}

	// Refresh nextSeq from the persisted lastSeq after every replay path
	// (local WAL, branch, restore-point, and remote S3) has run. Any path
	// that advances db.LastSequence() past the value computed before replay
	// must not let the next live write reuse an already-applied sequence.
	if ls := db.LastSequence(); ls > nextSeq {
		nextSeq = ls
	}

	bgCtx, bgCancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:         cfg,
		term:        term,
		wal:         w,
		bgCtx:       bgCtx,
		cancelBg:    bgCancel,
		nextRev:     db.CurrentRevision(),
		nextSeq:     nextSeq,
		pending:     make(map[string]pendingKV),
		writeC:      make(chan *writeReq, 1024),
		sstUploader: sstUp,
	}
	n.log = log
	n.cp = cp
	n.db.Store(db)
	n.initRevisionSampler()
	if cfg.CheckpointEntries > 0 {
		n.checkpointTriggerC = make(chan struct{}, 1)
	}

	if starter, ok := w.(interface{ Start(context.Context) }); ok {
		starter.Start(bgCtx)
	}

	// ── Determine role ───────────────────────────────────────────────────────
	if cfg.PeerListenAddr == "" || cfg.ObjectStore == nil {
		n.storeRole(roleSingle)
	} else {
		if err := n.electAndStart(bgCtx); err != nil {
			bgCancel()
			w.Close()
			db.Close()
			return nil, err
		}
	}

	// ── Background jobs ──────────────────────────────────────────────────────
	if n.loadRole() != roleFollower {
		n.bgWg.Add(1)
		go func() { defer n.bgWg.Done(); n.commitLoop(bgCtx) }()
	}
	if n.loadRole() != roleFollower && cfg.ObjectStore != nil && cfg.CheckpointInterval > 0 {
		n.bgWg.Add(1)
		go func() { defer n.bgWg.Done(); n.checkpointLoop(bgCtx) }()
	}
	if n.loadRole() != roleFollower && n.autoCompactEnabled() {
		n.bgWg.Add(1)
		go func() { defer n.bgWg.Done(); n.autoCompactLoop(bgCtx) }()
	}
	if n.loadRole() == roleFollower {
		n.bgWg.Add(1)
		go func() { defer n.bgWg.Done(); n.followLoop(bgCtx) }()
	}
	// Only start background SST uploads on leader/single nodes. Followers
	// should not upload their SSTs to S3: the leader's GCOrphanSSTs would
	// delete them (they aren't referenced by any checkpoint), and when the
	// follower later becomes the leader its checkpoint would reference missing
	// keys. Instead, the follower-to-leader promotion path calls Reconcile
	// (see attemptPromotion) to upload all current SSTs before the first
	// checkpoint.
	if sstUp != nil && n.loadRole() != roleFollower {
		sstUp.Start(bgCtx)
	}

	// ── Observability ─────────────────────────────────────────────────────────
	n.updateMetrics()

	return n, nil
}

// updateMetrics refreshes the role and revision gauges.
func (n *Node) updateMetrics() {
	switch n.loadRole() {
	case roleLeader:
		metrics.SetRole("leader")
	case roleFollower:
		metrics.SetRole("follower")
	default:
		metrics.SetRole("single")
	}
	metrics.CurrentRevision.Set(float64(n.db.Load().CurrentRevision()))
	metrics.CompactRevision.Set(float64(n.db.Load().CompactRevision()))
}

// electAndStart runs leader election and configures the node as leader or follower.
func (n *Node) electAndStart(bgCtx context.Context) error {
	lock := election.NewLock(n.cfg.ObjectStore, n.cfg.NodeID, n.cfg.AdvertisePeerAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rec, won, err := lock.TryAcquire(ctx, n.term, n.db.Load().CurrentRevision())
	if err != nil {
		return fmt.Errorf("t4: election: %w", err)
	}

	if won {
		return n.becomeLeader(bgCtx, lock, rec)
	}

	n.observeTerm(rec.Term)
	n.storeRole(roleFollower)
	cli := peer.NewClient(rec.LeaderAddr, n.cfg.NodeID, n.cfg.FollowerMaxRetries, n.cfg.PeerClientTLS, n.log)
	n.peerCli = cli
	n.leaderCli.Store(cli)
	metrics.ElectionsTotal.WithLabelValues("lost").Inc()
	n.log.Infof("t4: following leader at %s (term=%d)", rec.LeaderAddr, rec.Term)
	return nil
}

// gracefulLeaderShutdown drains in-flight writes, uploads the WAL synchronously
// to object storage, and updates the election lock's CommittedRev so a future
// TakeOver cannot win at a revision below what we acknowledged to clients.
// Only after all of that completes does it broadcast shutdown to followers.
//
// Acquires n.fenceMu.Lock() to drain in-flight writes. Releases it before
// returning so that the subsequent n.cancelBg() can drive checkpointLoop /
// watchLoop to exit cleanly — those loops also acquire fenceMu, and would
// deadlock waiting for it if we held it indefinitely. After this function
// returns, n.closed is true so no new writes will start.
//
// Must be invoked BEFORE n.cancelBg(): commitLoop has to be alive to drain
// the in-flight batch and signal each Put's done channel.
//
// Best-effort: errors are logged, not propagated. The leader is going down
// regardless; the goal is to maximise durability before that happens.
func (n *Node) gracefulLeaderShutdown(peerSrv *peer.Server) {
	// 1. Fence in-flight writes. Put holds fenceMu.RLock for its entire
	//    duration (writes.go), so this Lock waits until commitLoop has
	//    signalled done for every batch already in writeC. New Puts are
	//    rejected by the n.closed.Load() check at the top of Put because
	//    closeOnce already set n.closed before calling us.
	n.fenceMu.Lock()
	defer n.fenceMu.Unlock()

	// 2. Seal and synchronously upload all WAL segments to S3, including
	//    the active segment containing our most recent acks. wal.Close
	//    drains the upload queue and uploads the final segment using a
	//    fresh 2-minute context so a cancelled bgCtx cannot cut it short.
	if werr := n.wal.Close(); werr != nil {
		n.log.Errorf("t4: graceful shutdown: wal close: %v", werr)
	}

	// 3. Touch the election lock with our true CurrentRevision so that
	//    election.TakeOver fences any candidate whose local revision is
	//    behind us. Without this the lock's CommittedRev reflects only
	//    the last periodic touch, which can lag the actual durable rev
	//    by an entire LeaderWatchInterval.
	if n.cfg.ObjectStore != nil {
		tCtx, tCancel := context.WithTimeout(context.Background(), 5*time.Second)
		lock := election.NewLock(n.cfg.ObjectStore, n.cfg.NodeID, n.cfg.AdvertisePeerAddr)
		if terr := lock.Touch(tCtx, n.term, n.cfg.AdvertisePeerAddr, n.db.Load().CurrentRevision()); terr != nil {
			n.log.Warnf("t4: graceful shutdown: lock touch: %v", terr)
		}
		tCancel()
	}

	// 4. Now safe for followers to take over: WAL is durable in S3 and
	//    the lock fences anyone behind us. BroadcastShutdown closes the
	//    streams' shutdown channel; each Follow loop sends a Shutdown
	//    msg and returns, then the follower runs attemptPromotion.
	if peerSrv != nil {
		peerSrv.BroadcastShutdown()
	}
}

// Close shuts down the node cleanly.
func (n *Node) Close() error {
	var err error
	n.closeOnce.Do(func() {
		n.closed.Store(true)

		// Snapshot peer handles under the mutex before using them. becomeLeader
		// and becomeFollower write these fields under n.mu, so reads in Close
		// must also be guarded to avoid a data race.
		n.mu.Lock()
		role := n.loadRole()
		peerCli := n.peerCli
		peerSrv := n.peerSrv
		n.mu.Unlock()

		// Graceful goodbye signals — sent before cancelling context so the RPCs
		// can complete on a still-running connection.
		//
		// Follower: tell the leader this disconnect is intentional so it skips
		// split-brain fencing machinery.
		//
		// Leader: drain in-flight writes, upload the WAL to S3, and stamp the
		// election lock with our final committed revision BEFORE telling
		// followers to take over. The order matters:
		//
		//   1. Without a fence-and-flush, a follower can win TakeOver while
		//      our last sealed segment is still uploading. The new leader
		//      starts writing term=N+1 at the same revision range our
		//      term=N segment will eventually occupy in S3, and walTermCutoffs
		//      drops our entries on every future replay → permanent data loss.
		//
		//   2. Without a fresh lock.CommittedRev, a TakeOver candidate can
		//      win election with a local revision below entries we already
		//      acknowledged to clients. Touching the lock here turns those
		//      entries into a fence (election.TakeOver backs off candidates
		//      whose committedRev is below the lock's value).
		switch role {
		case roleFollower:
			if peerCli != nil {
				gCtx, gCancel := context.WithTimeout(context.Background(), 3*time.Second)
				peerCli.GoodBye(gCtx)
				gCancel()
			}
		case roleLeader:
			n.gracefulLeaderShutdown(peerSrv)
		}

		n.cancelBg()
		if cli := n.leaderCli.Load(); cli != nil {
			cli.Close()
		}
		// Signal the store's closed channel now, before waiting on readWg.
		// This unblocks any goroutines blocked in store.WaitForRevision (which
		// hold a readWg count) so they can return ErrClosed immediately instead
		// of waiting until the context expires — which would cause readWg.Wait()
		// to block for tens of seconds and deadlock Close().
		n.db.Load().SignalClose()
		// Wait for followLoop / checkpointLoop to exit before closing WAL and
		// DB. cancelBg has already been called above, so the loops will drain
		// promptly; we just need to avoid closing DB under a concurrent Apply.
		n.bgWg.Wait()
		// Stop the peer gRPC server and listener AFTER background goroutines
		// have exited. becomeLeader (called from followLoop) may have written
		// a new peerGRPC/peerLis after our early snapshot above, so we must
		// re-read under the mutex here to capture the final value and ensure
		// the server is not left running against an already-closed DB.
		n.mu.Lock()
		peerGRPC := n.peerGRPC
		peerLis := n.peerLis
		n.mu.Unlock()
		if peerGRPC != nil {
			peerGRPC.Stop() // terminates all active streams immediately
		} else if peerLis != nil {
			peerLis.Close()
		}
		// Drain all in-flight read operations (Get, List, WaitForRevision).
		// Taking the write lock waits for every concurrent RLock holder to
		// release, and prevents new RLocks from being acquired, so the DB is
		// not closed under a live reader.  The store's closed channel was
		// signalled above, so any reader blocked in WaitForRevision will
		// return ErrClosed and release its RLock promptly.
		n.readMu.Lock()
		n.readMu.Unlock()
		if werr := n.wal.Close(); werr != nil {
			n.log.Errorf("t4: wal close: %v", werr)
			err = werr
		}
		// Intentionally do NOT delete the election lock on shutdown.
		// A stale lock is safe: followers use TakeOver (term bump) after stream
		// failure. Deleting here is unsafe because it is not conditional and can
		// erase a newer leader's lock during overlap/restart races.
		if dberr := n.db.Load().Close(); dberr != nil {
			err = dberr
		}
	})
	return err
}

// ── Read path (all roles serve locally) ──────────────────────────────────────

// ReadConsistency returns the configured read consistency mode.
func (n *Node) ReadConsistency() ReadConsistency { return n.cfg.ReadConsistency }

// WatchSendTimeout returns the configured per-watch send timeout used by the
// etcd Watch adapter to detect slow watchers. See Config.WatchSendTimeout.
func (n *Node) WatchSendTimeout() time.Duration { return n.cfg.WatchSendTimeout }

// syncWithLeader implements the ReadIndex pattern: ask the leader for its
// current revision, then wait until this node has applied at least that far.
// Returns nil immediately if the node is the leader or running single-node.
func (n *Node) syncWithLeader(ctx context.Context) error {
	cli := n.leaderCli.Load()
	if cli == nil {
		// If the background context has been cancelled the node is either
		// shutting down or has been fenced (leader superseded by a new term).
		// Serving a read from our local stale Pebble would violate
		// linearizability — return an error so the client retries elsewhere.
		if n.bgCtx.Err() != nil {
			return ErrClosed
		}
		if n.loadRole() == roleFollower {
			return ErrNoLeader
		}
		return nil // leader or single-node — already up-to-date
	}
	resp, err := cli.ForwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardGetRevision})
	if err != nil {
		if isLeaderUnavailable(err) {
			return ErrNoLeader
		}
		return fmt.Errorf("t4: read sync: %w", err)
	}
	if err := n.WaitForRevision(ctx, resp.Revision); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return fmt.Errorf("t4: read sync: leader reported rev=%d but local node only reached rev=%d before wait ended: %w",
				resp.Revision, n.db.Load().CurrentRevision(), err)
		}
		return fmt.Errorf("t4: read sync: wait for local revision %d: %w", resp.Revision, err)
	}
	return nil
}

func isLeaderUnavailable(err error) bool {
	return peer.IsLeaderUnreachable(err) || peer.IsLeaderShutdown(err) || status.Code(err) == codes.Unavailable
}

// ReadOption configures an optional read parameter on Get/Exists/List/Count
// and their Linearizable variants. Construct with WithRevision / WithFromKey
// / WithLimit.
type ReadOption func(*readOpts)

type readOpts struct {
	revision int64
	fromKey  string
	limit    int64
}

func (o readOpts) hasRevision() bool { return o.revision > 0 }
func (o readOpts) storeOptions() istore.ReadOptions {
	return istore.ReadOptions{
		Revision: o.revision,
		FromKey:  o.fromKey,
		Limit:    o.limit,
	}
}

func applyReadOpts(opts []ReadOption) readOpts {
	var o readOpts
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// WithRevision reads as of the given revision. A revision of 0 (the default)
// reads HEAD. Reads strictly below the store's CompactRevision return
// ErrCompacted; reads above CurrentRevision return ErrFutureRevision.
func WithRevision(rev int64) ReadOption {
	return func(o *readOpts) { o.revision = rev }
}

// WithFromKey filters List/Count results to keys whose name is
// lexicographically >= fromKey. fromKey == "" disables the filter.
//
// Used by paginated lists to compute "items remaining after this page"
// without double-counting keys before the continuation point.
func WithFromKey(k string) ReadOption {
	return func(o *readOpts) { o.fromKey = k }
}

// WithLimit caps List results to at most n entries. limit <= 0 (the default)
// returns all matching entries.
func WithLimit(n int64) ReadOption {
	return func(o *readOpts) { o.limit = n }
}

// LinearizableGet returns the value for key with linearizability guaranteed.
// On a follower it syncs to the leader's revision before serving locally.
func (n *Node) LinearizableGet(ctx context.Context, key string, opts ...ReadOption) (*KeyValue, error) {
	if err := n.syncWithLeader(ctx); err != nil {
		return nil, err
	}
	return n.Get(key, opts...)
}

// LinearizableExists reports whether key exists with linearizability guaranteed.
func (n *Node) LinearizableExists(ctx context.Context, key string, opts ...ReadOption) (bool, error) {
	if err := n.syncWithLeader(ctx); err != nil {
		return false, err
	}
	return n.Exists(key, opts...)
}

// LinearizableList returns keys with the given prefix with linearizability
// guaranteed. Use WithFromKey / WithLimit / WithRevision to refine.
func (n *Node) LinearizableList(ctx context.Context, prefix string, opts ...ReadOption) ([]*KeyValue, error) {
	if err := n.syncWithLeader(ctx); err != nil {
		return nil, err
	}
	return n.List(prefix, opts...)
}

// LinearizableCount returns the count of keys with the given prefix with
// linearizability guaranteed. Use WithFromKey / WithRevision to refine.
func (n *Node) LinearizableCount(ctx context.Context, prefix string, opts ...ReadOption) (int64, error) {
	if err := n.syncWithLeader(ctx); err != nil {
		return 0, err
	}
	return n.Count(prefix, opts...)
}

func (n *Node) Get(key string, opts ...ReadOption) (*KeyValue, error) {
	if n.closed.Load() {
		return nil, ErrClosed
	}
	n.readMu.RLock()
	defer n.readMu.RUnlock()
	if n.closed.Load() {
		return nil, ErrClosed
	}
	o := applyReadOpts(opts)
	var sv *istore.KeyValue
	var err error
	if o.hasRevision() {
		sv, err = n.db.Load().GetAt(key, o.revision)
	} else {
		sv, err = n.db.Load().Get(key)
	}
	if err != nil || sv == nil {
		return nil, err
	}
	return toKV(sv), nil
}

func (n *Node) Exists(key string, opts ...ReadOption) (bool, error) {
	if n.closed.Load() {
		return false, ErrClosed
	}
	n.readMu.RLock()
	defer n.readMu.RUnlock()
	if n.closed.Load() {
		return false, ErrClosed
	}
	o := applyReadOpts(opts)
	if o.hasRevision() {
		return n.db.Load().ExistsAt(key, o.revision)
	}
	return n.db.Load().Exists(key)
}

func (n *Node) List(prefix string, opts ...ReadOption) ([]*KeyValue, error) {
	if n.closed.Load() {
		return nil, ErrClosed
	}
	n.readMu.RLock()
	defer n.readMu.RUnlock()
	if n.closed.Load() {
		return nil, ErrClosed
	}
	o := applyReadOpts(opts)
	svs, err := n.db.Load().ListRange(prefix, o.storeOptions())
	if err != nil {
		return nil, err
	}
	out := make([]*KeyValue, len(svs))
	for i, sv := range svs {
		out[i] = toKV(sv)
	}
	return out, nil
}

func (n *Node) Count(prefix string, opts ...ReadOption) (int64, error) {
	if n.closed.Load() {
		return 0, ErrClosed
	}
	n.readMu.RLock()
	defer n.readMu.RUnlock()
	if n.closed.Load() {
		return 0, ErrClosed
	}
	o := applyReadOpts(opts)
	return n.db.Load().CountRange(prefix, o.storeOptions())
}

func (n *Node) CurrentRevision() int64 { return n.db.Load().CurrentRevision() }
func (n *Node) CompactRevision() int64 { return n.db.Load().CompactRevision() }
func (n *Node) Config() Config         { return n.cfg }
func (n *Node) IsLeader() bool         { return n.loadRole() != roleFollower }

func (n *Node) WaitForRevision(ctx context.Context, rev int64) error {
	if n.closed.Load() {
		return ErrClosed
	}
	n.readMu.RLock()
	defer n.readMu.RUnlock()
	if n.closed.Load() {
		return ErrClosed
	}
	if err := n.db.Load().WaitForRevision(ctx, rev); err != nil {
		if errors.Is(err, istore.ErrClosed) {
			return ErrClosed
		}
		return err
	}
	return nil
}

// Flush seals the current WAL segment and flushes Pebble's memtable to the
// local store. Checkpointing uses the same ordering before writing a checkpoint.
func (n *Node) Flush() error {
	if n.closed.Load() {
		return ErrClosed
	}
	n.fenceMu.Lock()
	defer n.fenceMu.Unlock()
	if n.closed.Load() {
		return ErrClosed
	}
	seq := n.db.Load().LastSequence()
	if err := n.wal.SealAndFlush(seq + 1); err != nil {
		return err
	}
	return n.db.Load().Flush()
}

// WatchOption configures a Watch call. Use the With* helpers.
type WatchOption func(*watchOpts)

type watchOpts struct {
	prevKV bool
}

// WithPrevKV requests that emitted events include the previous KV for updates
// and deletes. Off by default: populating PrevKV adds one Pebble lookup per
// non-create event, which is significant under high churn.
func WithPrevKV() WatchOption {
	return func(o *watchOpts) { o.prevKV = true }
}

// Watch streams prefix-matching events using etcd revision semantics:
// startRev=0 means "from now"; startRev=N means replay from revision N (inclusive).
func (n *Node) Watch(ctx context.Context, prefix string, startRev int64, opts ...WatchOption) (<-chan Event, error) {
	if n.closed.Load() {
		return nil, ErrClosed
	}
	n.readMu.RLock()
	defer n.readMu.RUnlock()
	if n.closed.Load() {
		return nil, ErrClosed
	}
	var o watchOpts
	for _, opt := range opts {
		opt(&o)
	}
	if startRev > 0 && startRev <= n.db.Load().CompactRevision() {
		return nil, ErrCompacted
	}
	// internal/store.Watch uses last-seen revision semantics (start at rev+1),
	// so adapt etcd-style startRev here.
	storeStartRev := startRev - 1
	if startRev == 0 {
		storeStartRev = n.db.Load().CurrentRevision()
	}
	ch, err := n.db.Load().Watch(ctx, prefix, storeStartRev, o.prevKV)
	if err != nil {
		if errors.Is(err, istore.ErrClosed) {
			return nil, ErrClosed
		}
		return nil, err
	}
	return ch, nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// toKV is now identity since t4.KeyValue is an alias of istore.KeyValue.
// Kept as a thin shim to preserve existing call sites.
func toKV(sv *istore.KeyValue) *KeyValue { return sv }

func makeUploader(obj object.Store, log Logger) wal.Uploader {
	return func(ctx context.Context, localPath, objectKey string) error {
		f, err := os.Open(localPath)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				metrics.WALUploadErrors.Inc()
				return fmt.Errorf("uploader: open %q: %w", localPath, err)
			}
			// Local file is gone. Verify the segment reached S3; if it did the
			// uploader already completed on a previous attempt (idempotent).
			// If it didn't, the data is irrecoverably lost — log loudly.
			chkCtx, chkCancel := context.WithTimeout(ctx, 10*time.Second)
			rc, s3Err := obj.Get(chkCtx, objectKey)
			chkCancel()
			if s3Err == nil {
				rc.Close()
				log.Debugf("uploader: local file %q already gone but %q exists in S3 — treating as success", localPath, objectKey)
				return nil
			}
			metrics.WALUploadErrors.Inc()
			log.Errorf("uploader: local file %q is gone AND %q is not in S3 — segment data is lost", localPath, objectKey)
			return fmt.Errorf("uploader: open %q: %w", localPath, err)
		}
		defer f.Close()
		start := time.Now()
		if err := obj.Put(ctx, objectKey, f); err != nil {
			metrics.WALUploadErrors.Inc()
			return err
		}
		metrics.WALUploadsTotal.Inc()
		metrics.WALUploadDuration.Observe(time.Since(start).Seconds())
		return os.Remove(localPath)
	}
}

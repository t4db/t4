package strata

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/makhov/strata/internal/checkpoint"
	"github.com/makhov/strata/internal/election"
	"github.com/makhov/strata/internal/metrics"
	"github.com/makhov/strata/internal/peer"
	istore "github.com/makhov/strata/internal/store"
	"github.com/makhov/strata/internal/wal"
	"github.com/makhov/strata/pkg/object"
)

// walWriter is the subset of wal.WAL used by Node. The interface decouples Node
// from the concrete WAL implementation and allows fault injection in tests.
type walWriter interface {
	Append(e *wal.Entry) error
	AppendBatch(ctx context.Context, entries []*wal.Entry) error
	SealAndFlush(nextRev int64) error
	Close() error
}

// Sentinel errors.
var (
	ErrKeyExists = errors.New("strata: key already exists")
	ErrNotLeader = errors.New("strata: this node is not the leader; writes are rejected")
	ErrClosed    = errors.New("strata: node is closed")
	ErrCompacted = errors.New("strata: required revision has been compacted")
)

// nodeRole identifies whether the node is leader, follower, or single-node.
type nodeRole int32

const (
	roleSingle   nodeRole = iota // ObjectStore nil or PeerListenAddr empty
	roleLeader                   // elected leader
	roleFollower                 // following a remote leader
)

// Node is the top-level Strata instance.
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
	term uint64
	role atomic.Int32 // stores nodeRole values; use loadRole/storeRole

	db  *istore.Store
	wal walWriter // non-nil on leader/single; non-nil on follower (local WAL, no uploader)

	// mu serialises all leader writes for CAS safety and role transitions.
	mu sync.Mutex

	// fenceMu is a read-write mutex used to briefly pause leader writes while
	// the node verifies it is still the elected leader (after a follower
	// disconnects). Write methods hold RLock for their full duration; the
	// watchLoop holds Lock while performing the S3 lock check so that writes
	// are drained and no new ones start until the check completes.
	fenceMu sync.RWMutex

	// nextRev is the last revision assigned to a write. Incremented under mu
	// before the entry is sent to the commit loop. Replaces n.db.CurrentRevision()+1
	// on the write path so that in-flight entries have distinct revisions.
	nextRev int64

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
	checkpointTriggerC     chan struct{} // non-nil when CheckpointEntries > 0; signals entry-count-based checkpoint
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

// Open creates and starts a Node.
func Open(cfg Config) (*Node, error) {
	cfg.setDefaults()

	pebbleDir := filepath.Join(cfg.DataDir, "db")
	walDir := filepath.Join(cfg.DataDir, "wal")

	var (
		startRev int64
		term     uint64 = 1
	)

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
				t, rev, err := checkpoint.RestoreVersioned(ctx, rp.Store,
					rp.CheckpointArchive.Key, rp.CheckpointArchive.VersionID, pebbleDir)
				if err != nil {
					return nil, fmt.Errorf("strata: restore versioned checkpoint: %w", err)
				}
				term, startRev = t, rev
				logrus.Infof("strata: versioned checkpoint restored (term=%d rev=%d)", term, startRev)
			}
		}
	case cfg.BranchPoint != nil:
		if _, err := os.Stat(pebbleDir); errors.Is(err, os.ErrNotExist) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			bp := cfg.BranchPoint
			t, rev, err := checkpoint.RestoreBranch(ctx, bp.SourceStore, nil, bp.CheckpointKey, pebbleDir)
			if err != nil {
				return nil, fmt.Errorf("strata: restore branch point: %w", err)
			}
			term, startRev = t, rev
			logrus.Infof("strata: branch checkpoint restored (term=%d rev=%d)", term, startRev)
		}
	case cfg.ObjectStore != nil:
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		manifest, err := checkpoint.ReadManifest(ctx, cfg.ObjectStore)
		if err != nil {
			return nil, fmt.Errorf("strata: read manifest: %w", err)
		}
		if manifest != nil {
			logrus.Infof("strata: manifest found (rev=%d)", manifest.Revision)
			if _, err := os.Stat(pebbleDir); errors.Is(err, os.ErrNotExist) {
				// Retry loop: the checkpoint referenced by the manifest may
				// have been GC'd in the window between reading the manifest
				// and downloading it (most commonly when two leaders are
				// concurrently writing checkpoints during a leadership
				// transition). Re-reading the manifest gives us whichever
				// checkpoint the new leader most recently wrote.
				for attempt := 0; ; attempt++ {
					t, rev, rerr := checkpoint.Restore(ctx, cfg.ObjectStore, manifest.CheckpointKey, pebbleDir)
					if rerr == nil {
						term, startRev = t, rev
						logrus.Infof("strata: checkpoint restored (term=%d rev=%d)", term, startRev)
						break
					}
					if !errors.Is(rerr, object.ErrNotFound) || attempt >= 4 {
						return nil, fmt.Errorf("strata: restore checkpoint: %w", rerr)
					}
					// Checkpoint was GC'd; sleep briefly so the leader can
					// write a new checkpoint and update the manifest before
					// we retry. (With a 400 ms checkpoint interval, 500 ms
					// gives a full interval of headroom.)
					logrus.Warnf("strata: checkpoint %q not found, re-reading manifest (attempt %d/5)", manifest.CheckpointKey, attempt+1)
					time.Sleep(500 * time.Millisecond)
					manifest, err = checkpoint.ReadManifest(ctx, cfg.ObjectStore)
					if err != nil {
						return nil, fmt.Errorf("strata: read manifest: %w", err)
					}
					if manifest == nil {
						break // manifest disappeared; start fresh without checkpoint
					}
				}
			}
		}
	}

	// ── Open Pebble ──────────────────────────────────────────────────────────
	db, err := istore.Open(pebbleDir)
	if err != nil {
		return nil, fmt.Errorf("strata: open store: %w", err)
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
		uploader = makeUploader(cfg.ObjectStore)
	}

	opts := []wal.Option{
		wal.WithUploader(uploader),
		wal.WithSegmentMaxSize(cfg.SegmentMaxSize),
		wal.WithSegmentMaxAge(cfg.SegmentMaxAge),
	}
	if cfg.ObjectStore != nil && *cfg.WALSyncUpload {
		opts = append(opts, wal.WithSyncUpload())
	}
	w, err := wal.Open(walDir, term, startRev+1, opts...)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("strata: open wal: %w", err)
	}

	// ── Replay local WAL ─────────────────────────────────────────────────────
	if err := replayLocal(db, walDir, startRev); err != nil {
		w.Close()
		db.Close()
		return nil, fmt.Errorf("strata: local WAL replay: %w", err)
	}
	// Re-read pebble's revision: local WAL replay may have advanced it.
	startRev = db.CurrentRevision()

	// ── Replay remote WAL (S3) ───────────────────────────────────────────────
	switch {
	case cfg.RestorePoint != nil:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := replayPinned(ctx, db, cfg.RestorePoint, startRev); err != nil {
			w.Close()
			db.Close()
			return nil, fmt.Errorf("strata: pinned WAL replay: %w", err)
		}
	case cfg.BranchPoint != nil:
		// Branch nodes use their own ObjectStore for WAL replay after bootstrap.
		if cfg.ObjectStore != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			if err := replayRemote(ctx, db, cfg.ObjectStore, startRev); err != nil {
				w.Close()
				db.Close()
				return nil, fmt.Errorf("strata: branch WAL replay: %w", err)
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
			replayErr := replayRemote(ctx, db, cfg.ObjectStore, startRevBeforeReplay)

			// Always check manifest after replay, even on error: if a WAL
			// segment was GCed mid-read replayRemote returns ErrNotFound, but
			// that same GC event means a fresh checkpoint exists.  Re-restoring
			// from that checkpoint is the correct recovery; returning the error
			// directly would cause Open() to fail unnecessarily.
			freshManifest, merr := checkpoint.ReadManifest(ctx, cfg.ObjectStore)

			if replayErr != nil {
				// If the checkpoint has not advanced there is no recovery path —
				// this is a genuine storage error.
				if merr != nil || freshManifest == nil || freshManifest.Revision <= startRevBeforeReplay {
					w.Close()
					db.Close()
					return nil, fmt.Errorf("strata: remote WAL replay: %w", replayErr)
				}
				logrus.Infof("strata: WAL replay error (%v), checkpoint advanced (%d→%d) — re-restoring to close GC gap",
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
				logrus.Infof("strata: checkpoint advanced during WAL replay (%d→%d); re-restoring to close GC gap",
					startRevBeforeReplay, freshManifest.Revision)
			}
			db.Close()
			if rerr := os.RemoveAll(pebbleDir); rerr != nil {
				w.Close()
				return nil, fmt.Errorf("strata: remove stale pebble dir: %w", rerr)
			}
			var newTerm uint64
			var newRev int64
			manifest := freshManifest
			for attempt := range 5 {
				newTerm, newRev, err = checkpoint.Restore(ctx, cfg.ObjectStore, manifest.CheckpointKey, pebbleDir)
				if err == nil {
					break
				}
				if !errors.Is(err, object.ErrNotFound) || attempt >= 4 {
					w.Close()
					return nil, fmt.Errorf("strata: re-restore checkpoint: %w", err)
				}
				time.Sleep(500 * time.Millisecond)
				manifest, err = checkpoint.ReadManifest(ctx, cfg.ObjectStore)
				if err != nil || manifest == nil {
					w.Close()
					return nil, fmt.Errorf("strata: re-read manifest after GC: %w", err)
				}
			}
			_ = newRev // term drives WAL open; startRev is read from Pebble below
			freshDB, rerr := istore.Open(pebbleDir)
			if rerr != nil {
				w.Close()
				return nil, fmt.Errorf("strata: reopen store after GC-gap fix: %w", rerr)
			}
			w.Close()
			freshW, rerr := wal.Open(walDir, newTerm, freshDB.CurrentRevision()+1, opts...)
			if rerr != nil {
				freshDB.Close()
				return nil, fmt.Errorf("strata: reopen wal after GC-gap fix: %w", rerr)
			}
			db, w, term, startRev = freshDB, freshW, newTerm, freshDB.CurrentRevision()
			logrus.Infof("strata: checkpoint refreshed to rev=%d (term=%d)", startRev, term)
		}
	}

	bgCtx, bgCancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:      cfg,
		term:     term,
		db:       db,
		wal:      w,
		bgCtx:    bgCtx,
		cancelBg: bgCancel,
		nextRev:  db.CurrentRevision(),
		pending:  make(map[string]pendingKV),
		writeC:   make(chan *writeReq, 1024),
	}
	if cfg.CheckpointEntries > 0 {
		n.checkpointTriggerC = make(chan struct{}, 1)
	}

	w.Start(bgCtx)

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
	if n.loadRole() == roleFollower {
		n.bgWg.Add(1)
		go func() { defer n.bgWg.Done(); n.followLoop(bgCtx) }()
	}

	// ── Observability ─────────────────────────────────────────────────────────
	n.updateMetrics()
	if cfg.MetricsAddr != "" {
		go n.serveMetrics(bgCtx, cfg.MetricsAddr)
	}

	return n, nil
}

// serveMetrics starts an HTTP server exposing /metrics, /healthz, /readyz.
func (n *Node) serveMetrics(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if n.db.CurrentRevision() >= 0 {
			w.WriteHeader(http.StatusOK)
		} else {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
		}
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutCtx)
	}()
	logrus.Infof("strata: metrics listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logrus.Warnf("strata: metrics server: %v", err)
	}
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
	metrics.CurrentRevision.Set(float64(n.db.CurrentRevision()))
	metrics.CompactRevision.Set(float64(n.db.CompactRevision()))
}

// electAndStart runs leader election and configures the node as leader or follower.
func (n *Node) electAndStart(bgCtx context.Context) error {
	lock := election.NewLock(n.cfg.ObjectStore, n.cfg.NodeID, n.cfg.AdvertisePeerAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rec, won, err := lock.TryAcquire(ctx, n.term, n.db.CurrentRevision())
	if err != nil {
		return fmt.Errorf("strata: election: %w", err)
	}

	if won {
		return n.becomeLeader(bgCtx, lock, rec)
	}

	n.storeRole(roleFollower)
	cli := peer.NewClient(rec.LeaderAddr, n.cfg.NodeID, n.cfg.FollowerMaxRetries, n.cfg.PeerClientTLS)
	n.peerCli = cli
	n.leaderCli.Store(cli)
	metrics.ElectionsTotal.WithLabelValues("lost").Inc()
	logrus.Infof("strata: following leader at %s (term=%d)", rec.LeaderAddr, rec.Term)
	return nil
}

// becomeLeader transitions this node to leader role.
// Re-opens the WAL with an S3 uploader, starts the peer gRPC server,
// and launches the watchLoop. Must NOT be called with n.mu held.
func (n *Node) becomeLeader(bgCtx context.Context, lock *election.Lock, rec *election.LockRecord) error {
	n.wal.Close()
	walDir := filepath.Join(n.cfg.DataDir, "wal")

	// Upload any local WAL segments that were not yet in S3 before taking on
	// writes. This covers the same-node re-election case where the previous WAL
	// had no uploader (follower WAL) or crashed before the upload completed.
	// After this point, new leader writes are uploaded async (SegmentMaxAge).
	upCtx, upCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	uploadLocalWALSegments(upCtx, walDir, n.cfg.ObjectStore)
	upCancel()

	// Replay any remote WAL entries not yet in our Pebble. A follower that wins
	// election may be behind the former leader if the former leader committed
	// entries during single-node mode (no quorum required) before crashing.
	// Check against the latest S3 checkpoint first: if this node is behind the
	// checkpoint, restore it before replaying WAL so replayRemote only needs
	// segments still present in S3.
	if n.cfg.ObjectStore != nil {
		cpCtx, cpCancel := context.WithTimeout(context.Background(), 5*time.Minute)
		if _, cpErr := n.restoreDBIfBehindCheckpoint(cpCtx); cpErr != nil {
			cpCancel()
			return fmt.Errorf("strata: leader checkpoint catch-up: %w", cpErr)
		}
		cpCancel()

		reCtx, reCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		if err := replayRemote(reCtx, n.db, n.cfg.ObjectStore, n.db.CurrentRevision()); err != nil {
			reCancel()
			return fmt.Errorf("strata: becomeLeader replay remote WAL: %w", err)
		}
		reCancel()
	}

	// With quorum commit, every committed entry exists on at least two nodes'
	// WALs before the caller sees success. S3 is disaster-recovery only (both
	// nodes fail simultaneously), so uploads can be async — driven by
	// SegmentMaxAge — without affecting write durability.
	w2, err := wal.Open(walDir, rec.Term, n.db.CurrentRevision()+1,
		wal.WithUploader(makeUploader(n.cfg.ObjectStore)),
		wal.WithSegmentMaxSize(n.cfg.SegmentMaxSize),
		wal.WithSegmentMaxAge(n.cfg.SegmentMaxAge),
	)
	if err != nil {
		return fmt.Errorf("strata: open WAL as leader: %w", err)
	}
	w2.Start(bgCtx)

	peerSrv := peer.NewServer(n.cfg.PeerBufferSize)
	lis, err := net.Listen("tcp", n.cfg.PeerListenAddr)
	if err != nil {
		w2.Close()
		return fmt.Errorf("strata: peer listen %s: %w", n.cfg.PeerListenAddr, err)
	}
	serverOpts := []grpc.ServerOption{grpc.ForceServerCodec(peer.Codec{})}
	if n.cfg.PeerServerTLS != nil {
		serverOpts = append(serverOpts, grpc.Creds(n.cfg.PeerServerTLS))
	}
	grpcSrv := grpc.NewServer(serverOpts...)
	peer.RegisterWalStreamServer(grpcSrv, peerSrv)

	// Commit state transition atomically before accepting connections.
	n.mu.Lock()
	n.wal = w2
	n.term = rec.Term
	n.peerSrv = peerSrv
	n.peerLis = lis
	n.peerGRPC = grpcSrv
	n.leaderCli.Store(nil) // leader does not forward writes
	n.storeRole(roleLeader)
	n.nextRev = n.db.CurrentRevision() // sync revision counter after any replay
	n.pending = make(map[string]pendingKV)
	n.mu.Unlock()

	// Install the forward handler after role is set to leader so that
	// HandleForward sees the correct role and executes writes directly.
	peerSrv.SetForwardHandler(n)
	// Tell the peer server what the first revision this leader will write is.
	// Followers connecting with a lower fromRev are missing entries that were
	// only replayed into Pebble from S3 (never in the ring buffer) and must
	// re-sync before consuming the live stream.
	peerSrv.SetStartRev(n.db.CurrentRevision() + 1)

	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			logrus.Warnf("strata: peer server: %v", err)
		}
	}()

	n.updateMetrics()
	metrics.ElectionsTotal.WithLabelValues("won").Inc()
	logrus.Infof("strata: elected leader (term=%d, peer=%s)", rec.Term, n.cfg.PeerListenAddr)
	go n.watchLoop(bgCtx, lock, rec.Term)
	return nil
}

// watchLoop periodically reads the lock from S3 to detect supersession.
// Steps down (cancelBg) if the lock's term or owner changes.
// On clean shutdown, releases the lock.
//
// Split-brain prevention strategy:
//
//  1. Periodic fallback: LeaderWatchInterval reads S3 to detect supersession.
//     Does not touch LastSeenNano — followers are healthy while connected.
//
//  2. On follower disconnect: immediately fence writes (~50ms), verify still
//     leader, touch LastSeenNano so the disconnected follower backs off from
//     TakeOver. Begin polling every peer.FollowerRetryInterval to keep
//     LastSeenNano fresh and detect any TakeOver that follows. Stop polling
//     once followers reconnect.
//
//  3. TakeOver safety: if a follower fails to reconnect after FollowerMaxRetries
//     it calls TakeOver. If LastSeenNano is older than LeaderLivenessTTL the
//     TakeOver proceeds (via atomic conditional PUT). The leader detects the
//     supersession at its next fencedCheck and steps down cleanly.
func (n *Node) watchLoop(ctx context.Context, lock *election.Lock, term uint64) {
	ticker := time.NewTicker(n.cfg.LeaderWatchInterval)
	defer ticker.Stop()

	// Disconnect channel from the peer server, if we're running in multi-node mode.
	var disconnectC <-chan struct{}
	if n.peerSrv != nil {
		disconnectC = n.peerSrv.DisconnectC
	}

	// pollC is non-nil while liveness-touch polling is active (nil blocks in select).
	var (
		activePollTicker *time.Ticker
		pollC            <-chan time.Time
	)

	stopPolling := func() {
		if activePollTicker != nil {
			activePollTicker.Stop()
			activePollTicker = nil
		}
		pollC = nil
	}

	startPolling := func() {
		if activePollTicker != nil {
			return // already polling
		}
		activePollTicker = time.NewTicker(peer.FollowerRetryInterval)
		pollC = activePollTicker.C
	}

	// Always start polling immediately in cluster mode so LastSeenNano is
	// kept fresh from the very first tick, even before any follower connects.
	// Without this, a leader with no followers never touches the lock and the
	// liveness record goes stale after LeaderLivenessTTL, letting any
	// recovering follower win TakeOver and create a split-brain.
	if n.peerSrv != nil {
		startPolling()
	}

	// fencedCheck fences all leader writes for the duration of one S3 GET (and
	// optional PUT). Returns false and steps down if the lock has been superseded.
	// When touch is true and still leader, also writes LastSeenNano to the lock
	// so disconnected followers see a fresh liveness signal and back off TakeOver.
	//
	// The Read and the Touch (when requested) are tied together via a conditional
	// PUT (If-Match: <etag>): if another node wins the lock between our Read and
	// our Touch, TouchIfMatch returns ErrPreconditionFailed and we step down
	// immediately — closing the Read→Touch split-brain race.
	//
	// NOTE: fenceMu is released explicitly (not via defer) so that grpcSrv.Stop()
	// can be called outside the lock.  grpc.Server.Stop waits for in-flight
	// handlers to finish; those handlers (Put/Create/…) acquire fenceMu.RLock()
	// themselves, so calling Stop() while holding the write lock would deadlock.
	fencedCheck := func(reason string, touch bool) bool {
		n.fenceMu.Lock()
		rCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		rec, etag, err := lock.ReadETag(rCtx)
		cancel()
		if err != nil {
			n.fenceMu.Unlock()
			logrus.Warnf("strata: leader watch (%s): read lock: %v", reason, err)
			return true // transient S3 error; keep running
		}
		if rec == nil || rec.Term != term || rec.NodeID != n.cfg.NodeID {
			logrus.Errorf("strata: leader watch (%s): lock superseded (current: %+v) — stepping down", reason, rec)
			n.cancelBg()
			n.fenceMu.Unlock()
			// Stop the peer gRPC server so followers immediately lose their
			// streams and detect the leadership change.  Without this, followers
			// keep forwarding ForwardGetRevision to this zombie leader and receive
			// a stale revision, causing linearizability violations.
			if grpcSrv := n.peerGRPC; grpcSrv != nil {
				grpcSrv.Stop()
			}
			return false
		}
		if touch && n.peerSrv != nil {
			tCtx, tCancel := context.WithTimeout(ctx, 5*time.Second)
			err := lock.TouchIfMatch(tCtx, term, n.cfg.AdvertisePeerAddr, etag, n.db.CurrentRevision())
			tCancel()
			if errors.Is(err, object.ErrPreconditionFailed) {
				// Another node wrote the lock between our Read and our Touch —
				// we have been superseded.  Step down immediately.
				logrus.Errorf("strata: leader watch (%s): touch precondition failed — lock taken, stepping down", reason)
				n.cancelBg()
				n.fenceMu.Unlock()
				if grpcSrv := n.peerGRPC; grpcSrv != nil {
					grpcSrv.Stop()
				}
				return false
			}
			if err != nil {
				logrus.Warnf("strata: leader watch (%s): touch lock: %v", reason, err)
			}
		}
		n.fenceMu.Unlock()
		return true
	}

	for {
		select {
		case <-ticker.C:
			if !fencedCheck("periodic", false) {
				return
			}

		case <-disconnectC:
			// Immediate check + touch: catches TakeOver that already happened,
			// and signals liveness to the disconnected follower.
			logrus.Infof("strata: leader watch: follower disconnected — fencing writes and checking lock")
			if !fencedCheck("disconnect", true) {
				return
			}
			// Resume polling so LastSeenNano stays fresh while the follower
			// is away.  startPolling is idempotent — if we were already polling
			// (e.g. started at launch with no followers), this is a no-op.
			startPolling()

		case <-pollC:
			// Touch the lock to signal liveness and detect supersession.
			if !fencedCheck("poll", true) {
				stopPolling()
				return
			}
			// If followers have (re)connected, liveness touches are no longer
			// necessary — stop polling and rely on the periodic ticker.
			if n.peerSrv != nil && n.peerSrv.ConnectedFollowers() > 0 {
				logrus.Debugf("strata: leader watch: followers connected — pausing liveness poll")
				stopPolling()
			}

		case <-ctx.Done():
			stopPolling()
			return
		}
	}
}

// followLoop receives WAL entries from the leader and applies them locally.
// On ErrLeaderUnreachable it attempts a TakeOver election.
func (n *Node) followLoop(bgCtx context.Context) {
	lock := election.NewLock(n.cfg.ObjectStore, n.cfg.NodeID, n.cfg.AdvertisePeerAddr)
	cli := n.peerCli
	fromRev := n.db.CurrentRevision() + 1

	for {
		err := cli.Follow(bgCtx, fromRev, func(entries []wal.Entry) error {
			// Followers must apply a contiguous revision stream. If the leader
			// stream skips (or rewinds) a revision, force a full resync rather
			// than silently advancing currentRev with holes.
			for i, e := range entries {
				if e.Revision != fromRev+int64(i) {
					return peer.ErrResyncRequired
				}
			}
			ptrs := make([]*wal.Entry, len(entries))
			for i := range entries {
				ptrs[i] = &entries[i]
			}
			if err := n.wal.AppendBatch(bgCtx, ptrs); err != nil {
				return err
			}
			if err := n.db.Apply(entries); err != nil {
				return err
			}
			// Track the leader's term so attemptPromotion uses the correct
			// floorTerm when calling TakeOver.  Without this, n.term stays at
			// its Open() value and TakeOver backs off because it sees the
			// current lock term as "already taken over at a higher term".
			// The last entry in the batch has the highest-or-equal term.
			if last := entries[len(entries)-1]; last.Term > n.term {
				n.mu.Lock()
				if last.Term > n.term {
					n.term = last.Term
				}
				n.mu.Unlock()
			}
			// Advance only after a successful apply so a reconnect retries
			// from the start of the failed batch rather than skipping it.
			fromRev = entries[len(entries)-1].Revision + 1
			return nil
		})

		if bgCtx.Err() != nil {
			return
		}

		if peer.IsResyncRequired(err) {
			if n.cfg.ObjectStore == nil {
				logrus.Error("strata: follower resync required but no object store — restart node")
				n.cancelBg()
				return
			}
			// Ring buffer miss: the follower has been offline long enough that
			// the leader's ring buffer no longer covers fromRev. Restore from
			// the latest S3 checkpoint (if the follower's Pebble is behind it),
			// then replay any remaining WAL entries from S3.
			logrus.Warn("strata: follower resync required — restoring from checkpoint")
			if cpErr := n.resyncFromCheckpoint(bgCtx); cpErr != nil {
				logrus.Errorf("strata: follower in-place resync failed: %v — cancelling", cpErr)
				n.cancelBg()
				return
			}
			reCtx, reCancel := context.WithTimeout(bgCtx, 5*time.Minute)
			rerr := replayRemote(reCtx, n.db, n.cfg.ObjectStore, n.db.CurrentRevision())
			reCancel()
			if rerr != nil {
				logrus.Errorf("strata: follower S3 resync failed: %v — retrying", rerr)
				select {
				case <-time.After(2 * time.Second):
				case <-bgCtx.Done():
					return
				}
			} else {
				fromRev = n.db.CurrentRevision() + 1
				// Wake any goroutines blocked in WaitForRevision that entered
				// their wait loop while replayRemote was running. Recover does
				// not broadcast, so without this they would sleep until the
				// next live Apply — causing unnecessary read latency.
				n.db.NotifyRevision()
				logrus.Infof("strata: follower resync complete (now at rev=%d)", n.db.CurrentRevision())
			}
			continue
		}

		if peer.IsLeaderUnreachable(err) || peer.IsLeaderShutdown(err) {
			if peer.IsLeaderShutdown(err) {
				logrus.Info("strata: leader shut down gracefully — attempting immediate election takeover")
			} else {
				logrus.Warn("strata: leader unreachable — attempting election takeover")
			}
			newCli, promoted := n.attemptPromotion(bgCtx, lock, peer.IsLeaderShutdown(err))
			if promoted {
				return
			}
			if newCli != nil {
				oldCli := cli
				cli = newCli
				n.leaderCli.Store(newCli)
				oldCli.Close()
				logrus.Infof("strata: following new leader")
			}
			continue
		}

		logrus.Warnf("strata: follow loop error (will retry): %v", err)
		select {
		case <-time.After(2 * time.Second):
		case <-bgCtx.Done():
			return
		}
	}
}

// attemptPromotion tries to take over the leader lock after the stream dies.
// Returns (nil, true) if promoted to leader.
// Returns (newClient, false) if another node won; newClient follows that node.
// Returns (nil, false) on S3 errors or when the current leader's liveness
// record is fresh enough that TakeOver would risk a split-brain.
//
// graceful should be true when the leader sent an explicit shutdown signal:
// in that case the liveness check is skipped because the leader intentionally
// vacated and the fresh LastSeenNano would otherwise block all followers.
func (n *Node) attemptPromotion(bgCtx context.Context, lock *election.Lock, graceful bool) (*peer.Client, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Read the current lock before attempting TakeOver. If the leader recently
	// wrote a liveness touch (LastSeenNano is fresh) AND the shutdown was not
	// graceful, the leader may still have other followers and we are an
	// isolated minority node — back off to prevent split-brain.
	// On graceful shutdown we skip this check: the leader intentionally left,
	// so its fresh LastSeenNano must not block election.
	existing, err := lock.Read(ctx)
	if err != nil {
		logrus.Errorf("strata: takeover: read lock: %v", err)
		return nil, false
	}
	if !graceful && existing != nil && existing.LastSeenNano > 0 {
		age := time.Since(time.Unix(0, existing.LastSeenNano))
		if age < peer.LeaderLivenessTTL {
			logrus.Infof("strata: takeover: leader liveness is fresh (%v ago) — backing off to avoid split-brain", age.Round(time.Millisecond))
			// Back off from election, but do not keep retrying a stale endpoint.
			// If the lock advertises a leader address, switch followLoop to it.
			if existing.LeaderAddr != "" {
				return peer.NewClient(existing.LeaderAddr, n.cfg.NodeID, n.cfg.FollowerMaxRetries, n.cfg.PeerClientTLS), false
			}
			return nil, false
		}
	}
	// Revision fence: refuse to become leader if we are behind the last known
	// committed revision. A node missing entries would either drop them (data
	// loss) or fail to serve reads that clients already observed.
	// If the lock already points to a current leader, return their address so
	// followLoop switches to following them — connecting will trigger an
	// in-place resync that catches up this node's revision.
	if existing != nil && existing.CommittedRev > n.db.CurrentRevision() {
		logrus.Infof("strata: takeover: node is behind leader committed rev (ours=%d, leader=%d) — following current leader to catch up",
			n.db.CurrentRevision(), existing.CommittedRev)
		if existing.LeaderAddr != "" {
			return peer.NewClient(existing.LeaderAddr, n.cfg.NodeID, n.cfg.FollowerMaxRetries, n.cfg.PeerClientTLS), false
		}
		return nil, false
	}

	rec, won, err := lock.TakeOver(ctx, n.term, n.db.CurrentRevision())
	if err != nil {
		logrus.Errorf("strata: takeover election error: %v", err)
		return nil, false
	}

	if won {
		if err := n.becomeLeader(bgCtx, lock, rec); err != nil {
			logrus.Errorf("strata: promotion failed: %v", err)
			return nil, false
		}
		n.bgWg.Add(1)
		go func() { defer n.bgWg.Done(); n.commitLoop(bgCtx) }()
		if n.cfg.ObjectStore != nil && n.cfg.CheckpointInterval > 0 {
			n.bgWg.Add(1)
			go func() { defer n.bgWg.Done(); n.checkpointLoop(bgCtx) }()
		}
		return nil, true
	}

	if rec != nil && rec.LeaderAddr != "" {
		logrus.Infof("strata: lost election to %s (term=%d) — following", rec.NodeID, rec.Term)
		return peer.NewClient(rec.LeaderAddr, n.cfg.NodeID, n.cfg.FollowerMaxRetries, n.cfg.PeerClientTLS), false
	}
	return nil, false
}

// ── Write forwarding ──────────────────────────────────────────────────────────

// HandleForward implements peer.ForwardHandler. Called by the peer gRPC server
// when a follower forwards a write. Dispatches to the appropriate Node method.
// Since HandleForward runs on the leader, all write methods execute directly.
func (n *Node) HandleForward(ctx context.Context, req *peer.ForwardRequest) (*peer.ForwardResponse, error) {
	switch req.Op {
	case peer.ForwardPut:
		rev, err := n.Put(ctx, req.Key, req.Value, req.Lease)
		code, msg := encodeErr(err)
		return &peer.ForwardResponse{Revision: rev, Succeeded: err == nil, ErrCode: code, ErrMsg: msg}, nil

	case peer.ForwardCreate:
		rev, err := n.Create(ctx, req.Key, req.Value, req.Lease)
		code, msg := encodeErr(err)
		return &peer.ForwardResponse{Revision: rev, Succeeded: err == nil, ErrCode: code, ErrMsg: msg}, nil

	case peer.ForwardUpdate:
		newRev, oldKV, updated, err := n.Update(ctx, req.Key, req.Value, req.Revision, req.Lease)
		code, msg := encodeErr(err)
		resp := &peer.ForwardResponse{Revision: newRev, Succeeded: updated, ErrCode: code, ErrMsg: msg}
		resp.OldKV = kvToMsg(oldKV)
		return resp, nil

	case peer.ForwardDeleteIfRevision:
		newRev, oldKV, deleted, err := n.DeleteIfRevision(ctx, req.Key, req.Revision)
		code, msg := encodeErr(err)
		resp := &peer.ForwardResponse{Revision: newRev, Succeeded: deleted, ErrCode: code, ErrMsg: msg}
		resp.OldKV = kvToMsg(oldKV)
		return resp, nil

	case peer.ForwardCompact:
		err := n.Compact(ctx, req.Revision)
		code, msg := encodeErr(err)
		return &peer.ForwardResponse{Succeeded: err == nil, ErrCode: code, ErrMsg: msg}, nil

	case peer.ForwardGetRevision:
		// Return nextRev (the highest *assigned* revision), not db.CurrentRevision()
		// (the last *applied* revision). A write increments nextRev under n.mu and
		// sends to writeC before the commit loop applies it to Pebble. If we returned
		// db.CurrentRevision() here, a follower could sync to a revision that precedes
		// an in-flight write whose acknowledgment is about to be sent to the client —
		// causing a stale read that violates linearizability.
		n.mu.Lock()
		rev := n.nextRev
		n.mu.Unlock()
		return &peer.ForwardResponse{Revision: rev, Succeeded: true}, nil
	}
	return nil, fmt.Errorf("strata: unknown forward op %d", req.Op)
}

// forwardWrite sends a write request to the leader and decodes the response.
func (n *Node) forwardWrite(ctx context.Context, req *peer.ForwardRequest) (*peer.ForwardResponse, error) {
	cli := n.leaderCli.Load()
	if cli == nil {
		return nil, ErrNotLeader
	}
	op := fwdOpLabel(req.Op)
	start := time.Now()
	resp, err := cli.ForwardWrite(ctx, req)
	metrics.ForwardedWritesTotal.WithLabelValues(op).Inc()
	metrics.ForwardDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	return resp, err
}

func fwdOpLabel(op peer.ForwardOp) string {
	switch op {
	case peer.ForwardPut:
		return "put"
	case peer.ForwardCreate:
		return "create"
	case peer.ForwardUpdate:
		return "update"
	case peer.ForwardDeleteIfRevision:
		return "delete"
	case peer.ForwardCompact:
		return "compact"
	case peer.ForwardGetRevision:
		return "get_revision"
	default:
		return "unknown"
	}
}

func encodeErr(err error) (code, msg string) {
	if err == nil {
		return "", ""
	}
	if errors.Is(err, ErrKeyExists) {
		return "key_exists", ""
	}
	return "error", err.Error()
}

func decodeErr(code, msg string) error {
	switch code {
	case "":
		return nil
	case "key_exists":
		return ErrKeyExists
	default:
		return errors.New(msg)
	}
}

func kvToMsg(kv *KeyValue) *peer.KVMsg {
	if kv == nil {
		return nil
	}
	return &peer.KVMsg{
		Key: kv.Key, Value: kv.Value, Revision: kv.Revision,
		CreateRevision: kv.CreateRevision, PrevRevision: kv.PrevRevision,
		Lease: kv.Lease,
	}
}

func msgToKV(m *peer.KVMsg) *KeyValue {
	if m == nil {
		return nil
	}
	return &KeyValue{
		Key: m.Key, Value: m.Value, Revision: m.Revision,
		CreateRevision: m.CreateRevision, PrevRevision: m.PrevRevision,
		Lease: m.Lease,
	}
}

// ReadConsistency returns the configured read consistency mode.
func (n *Node) ReadConsistency() ReadConsistency { return n.cfg.ReadConsistency }

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
		return nil // leader or single-node — already up-to-date
	}
	resp, err := cli.ForwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardGetRevision})
	if err != nil {
		return fmt.Errorf("strata: read sync: %w", err)
	}
	return n.WaitForRevision(ctx, resp.Revision)
}

// LinearizableGet returns the value for key with linearizability guaranteed.
// On a follower it syncs to the leader's revision before serving locally.
func (n *Node) LinearizableGet(ctx context.Context, key string) (*KeyValue, error) {
	if err := n.syncWithLeader(ctx); err != nil {
		return nil, err
	}
	return n.Get(key)
}

// LinearizableList returns all keys with the given prefix with linearizability guaranteed.
func (n *Node) LinearizableList(ctx context.Context, prefix string) ([]*KeyValue, error) {
	if err := n.syncWithLeader(ctx); err != nil {
		return nil, err
	}
	return n.List(prefix)
}

// LinearizableCount returns the count of keys with the given prefix with linearizability guaranteed.
func (n *Node) LinearizableCount(ctx context.Context, prefix string) (int64, error) {
	if err := n.syncWithLeader(ctx); err != nil {
		return 0, err
	}
	return n.Count(prefix)
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
		// Leader: tell all followers to start election immediately so they don't
		// wait FollowerMaxRetries × FollowerRetryInterval before taking over.
		switch role {
		case roleFollower:
			if peerCli != nil {
				gCtx, gCancel := context.WithTimeout(context.Background(), 3*time.Second)
				peerCli.GoodBye(gCtx)
				gCancel()
			}
		case roleLeader:
			if peerSrv != nil {
				peerSrv.BroadcastShutdown()
			}
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
		n.db.SignalClose()
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
			logrus.Errorf("strata: wal close: %v", werr)
			err = werr
		}
		// Intentionally do NOT delete the election lock on shutdown.
		// A stale lock is safe: followers use TakeOver (term bump) after stream
		// failure. Deleting here is unsafe because it is not conditional and can
		// erase a newer leader's lock during overlap/restart races.
		if dberr := n.db.Close(); dberr != nil {
			err = dberr
		}
	})
	return err
}

// ── Write path (leader / single-node execute; follower forwards) ──────────────

// Put creates or updates key with value. Returns the new revision.
func (n *Node) Put(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	if n.closed.Load() {
		return 0, ErrClosed
	}
	if n.loadRole() == roleFollower {
		resp, err := n.forwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardPut, Key: key, Value: value, Lease: lease})
		if err != nil {
			return 0, err
		}
		return resp.Revision, decodeErr(resp.ErrCode, resp.ErrMsg)
	}
	n.fenceMu.RLock()
	defer n.fenceMu.RUnlock()
	start := time.Now()
	n.mu.Lock()
	if n.closed.Load() {
		n.mu.Unlock()
		return 0, ErrClosed
	}
	e, err := n.preparePut(key, value, lease)
	if err != nil {
		n.mu.Unlock()
		return 0, err
	}
	req := newWriteReq(ctx, e)
	n.writeC <- req
	n.mu.Unlock()
	return n.await(ctx, req, opLabel(e.Op), start, key, e.Revision)
}

// preparePut builds the WAL entry for a Put. Must be called under n.mu.
func (n *Node) preparePut(key string, value []byte, lease int64) (wal.Entry, error) {
	existing, err := n.readKey(key)
	if err != nil {
		return wal.Entry{}, err
	}
	n.nextRev++
	newRev := n.nextRev
	var op wal.Op
	var createRev, prevRev int64
	if existing == nil {
		op, createRev = wal.OpCreate, newRev
	} else {
		op, createRev, prevRev = wal.OpUpdate, existing.CreateRevision, existing.Revision
	}
	n.pending[key] = pendingKV{
		rev: newRev,
		kv: &istore.KeyValue{
			Key: key, Value: value, Revision: newRev,
			CreateRevision: createRev, PrevRevision: prevRev,
			Lease: lease,
		},
	}
	return wal.Entry{
		Revision: newRev, Term: n.term, Op: op,
		Key: key, Value: value, Lease: lease,
		CreateRevision: createRev, PrevRevision: prevRev,
	}, nil
}

// Create creates key only if it does not already exist.
func (n *Node) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	if n.loadRole() == roleFollower {
		resp, err := n.forwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardCreate, Key: key, Value: value, Lease: lease})
		if err != nil {
			return 0, err
		}
		return resp.Revision, decodeErr(resp.ErrCode, resp.ErrMsg)
	}
	n.fenceMu.RLock()
	defer n.fenceMu.RUnlock()
	start := time.Now()
	n.mu.Lock()
	if n.closed.Load() {
		n.mu.Unlock()
		return 0, ErrClosed
	}
	existing, err := n.readKey(key)
	if err != nil {
		n.mu.Unlock()
		return 0, err
	}
	if existing != nil {
		n.mu.Unlock()
		return 0, ErrKeyExists
	}
	n.nextRev++
	newRev := n.nextRev
	n.pending[key] = pendingKV{
		rev: newRev,
		kv: &istore.KeyValue{
			Key: key, Value: value, Revision: newRev,
			CreateRevision: newRev, Lease: lease,
		},
	}
	e := wal.Entry{
		Revision: newRev, Term: n.term, Op: wal.OpCreate,
		Key: key, Value: value, Lease: lease, CreateRevision: newRev,
	}
	req := newWriteReq(ctx, e)
	n.writeC <- req
	n.mu.Unlock()
	return n.await(ctx, req, "create", start, key, newRev)
}

// Update updates key only if its current revision matches (CAS).
func (n *Node) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error) {
	if n.loadRole() == roleFollower {
		resp, err := n.forwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardUpdate, Key: key, Value: value, Revision: revision, Lease: lease})
		if err != nil {
			return 0, nil, false, err
		}
		return resp.Revision, msgToKV(resp.OldKV), resp.Succeeded, decodeErr(resp.ErrCode, resp.ErrMsg)
	}
	n.fenceMu.RLock()
	defer n.fenceMu.RUnlock()
	start := time.Now()
	n.mu.Lock()
	if n.closed.Load() {
		n.mu.Unlock()
		return 0, nil, false, ErrClosed
	}
	existing, err := n.readKey(key)
	if err != nil {
		n.mu.Unlock()
		return 0, nil, false, err
	}
	if existing == nil || existing.Revision != revision {
		curRev := n.db.CurrentRevision()
		n.mu.Unlock()
		return curRev, toKV(existing), false, nil
	}
	n.nextRev++
	newRev := n.nextRev
	n.pending[key] = pendingKV{
		rev: newRev,
		kv: &istore.KeyValue{
			Key: key, Value: value, Revision: newRev,
			CreateRevision: existing.CreateRevision, PrevRevision: existing.Revision,
			Lease: lease,
		},
	}
	e := wal.Entry{
		Revision: newRev, Term: n.term, Op: wal.OpUpdate,
		Key: key, Value: value, Lease: lease,
		CreateRevision: existing.CreateRevision, PrevRevision: existing.Revision,
	}
	oldKV := toKV(existing)
	req := newWriteReq(ctx, e)
	n.writeC <- req
	n.mu.Unlock()
	newRev, err = n.await(ctx, req, "update", start, key, newRev)
	if err != nil {
		return 0, nil, false, err
	}
	return newRev, oldKV, true, nil
}

// Delete removes key unconditionally.
func (n *Node) Delete(ctx context.Context, key string) (int64, error) {
	if n.loadRole() == roleFollower {
		resp, err := n.forwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardDeleteIfRevision, Key: key, Revision: 0})
		if err != nil {
			return 0, err
		}
		return resp.Revision, decodeErr(resp.ErrCode, resp.ErrMsg)
	}
	n.fenceMu.RLock()
	defer n.fenceMu.RUnlock()
	start := time.Now()
	n.mu.Lock()
	if n.closed.Load() {
		n.mu.Unlock()
		return 0, ErrClosed
	}
	e, err := n.prepareDelete(key)
	if err != nil || e.Key == "" {
		n.mu.Unlock()
		return 0, err // key not found — no-op
	}
	req := newWriteReq(ctx, e)
	n.writeC <- req
	n.mu.Unlock()
	return n.await(ctx, req, "delete", start, key, e.Revision)
}

// DeleteIfRevision deletes key only if its current revision matches (CAS).
func (n *Node) DeleteIfRevision(ctx context.Context, key string, revision int64) (int64, *KeyValue, bool, error) {
	if n.loadRole() == roleFollower {
		resp, err := n.forwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardDeleteIfRevision, Key: key, Revision: revision})
		if err != nil {
			return 0, nil, false, err
		}
		return resp.Revision, msgToKV(resp.OldKV), resp.Succeeded, decodeErr(resp.ErrCode, resp.ErrMsg)
	}
	n.fenceMu.RLock()
	defer n.fenceMu.RUnlock()
	start := time.Now()
	n.mu.Lock()
	if n.closed.Load() {
		n.mu.Unlock()
		return 0, nil, false, ErrClosed
	}
	existing, err := n.readKey(key)
	if err != nil {
		n.mu.Unlock()
		return 0, nil, false, err
	}
	curRev := n.db.CurrentRevision()
	if existing == nil {
		n.mu.Unlock()
		return curRev, nil, false, nil
	}
	if revision != 0 && existing.Revision != revision {
		n.mu.Unlock()
		return curRev, toKV(existing), false, nil
	}
	oldKV := toKV(existing)
	e, err := n.prepareDelete(key)
	if err != nil || e.Key == "" {
		n.mu.Unlock()
		return 0, nil, false, err
	}
	req := newWriteReq(ctx, e)
	n.writeC <- req
	n.mu.Unlock()
	newRev, err := n.await(ctx, req, "delete", start, key, e.Revision)
	if err != nil {
		return 0, nil, false, err
	}
	return newRev, oldKV, true, nil
}

// prepareDelete builds a WAL delete entry for key. Must be called under n.mu.
// Returns a zero-valued entry (Key=="") if the key does not exist.
func (n *Node) prepareDelete(key string) (wal.Entry, error) {
	existing, err := n.readKey(key)
	if err != nil || existing == nil {
		return wal.Entry{}, err
	}
	n.nextRev++
	newRev := n.nextRev
	n.pending[key] = pendingKV{rev: newRev, deleted: true}
	return wal.Entry{
		Revision: newRev, Term: n.term, Op: wal.OpDelete,
		Key: key, CreateRevision: existing.CreateRevision, PrevRevision: existing.Revision,
	}, nil
}

// readKey returns the current value of key, checking in-flight pending writes
// first, then falling back to Pebble. Must be called under n.mu.
func (n *Node) readKey(key string) (*istore.KeyValue, error) {
	if p, ok := n.pending[key]; ok {
		if p.deleted {
			return nil, nil
		}
		return p.kv, nil
	}
	return n.db.Get(key)
}

func opLabel(op wal.Op) string {
	switch op {
	case wal.OpCreate:
		return "create"
	case wal.OpUpdate:
		return "update"
	case wal.OpDelete:
		return "delete"
	case wal.OpCompact:
		return "compact"
	default:
		return "unknown"
	}
}

// await waits for a commit-loop result. req must have been sent to writeC
// before n.mu was released. key and rev identify the pending map entry to
// remove once the commit completes (pass key=="" for Compact which has none).
func (n *Node) await(ctx context.Context, req *writeReq, op string, start time.Time, key string, rev int64) (int64, error) {
	cleanPending := func() {
		if key != "" {
			n.mu.Lock()
			if p, ok := n.pending[key]; ok && p.rev == rev {
				delete(n.pending, key)
			}
			n.mu.Unlock()
		}
	}
	var err error
	select {
	case err = <-req.done:
	case <-ctx.Done():
		// Give the commit loop a brief window to react: it may have already
		// detected our cancellation (via batchCtx) and is about to signal us
		// with a meaningful error. If it does, use that result instead of
		// ctx.Err() so callers see a system-level error rather than their own
		// deadline expiry.
		timer := time.NewTimer(5 * time.Millisecond)
		defer timer.Stop()
		select {
		case err = <-req.done:
			// commit loop responded promptly; fall through to normal handling
		case <-timer.C:
			go func() { <-req.done; cleanPending() }()
			return 0, ctx.Err()
		}
	}
	cleanPending()
	if err != nil {
		metrics.WriteErrors.WithLabelValues(op).Inc()
		return 0, fmt.Errorf("strata: commit: %w", err)
	}
	metrics.WritesTotal.WithLabelValues(op).Inc()
	metrics.WriteDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	metrics.CurrentRevision.Set(float64(rev))
	count := atomic.AddInt64(&n.entriesSinceCheckpoint, 1)
	if n.checkpointTriggerC != nil && count >= n.cfg.CheckpointEntries {
		select {
		case n.checkpointTriggerC <- struct{}{}:
		default: // already a pending trigger; don't block
		}
	}
	return rev, nil
}

// commitLoop is the group-commit pipeline for leader/single-node writes.
// It drains writeC, writes all entries to WAL with a single fsync, applies
// them to Pebble as a batch, and signals each caller's done channel.
func (n *Node) commitLoop(ctx context.Context) {
	defer func() {
		// Fence the node first so new writers fail fast.
		n.closed.Store(true)

		// Drain requests immediately to free queue slots. This unblocks writers
		// that might be stuck on n.writeC <- req while holding n.mu.
	drain:
		for {
			select {
			case req := <-n.writeC:
				req.done <- ErrClosed
			default:
				break drain
			}
		}

		// Wait for any writer currently in the critical section (between closed
		// check and queue send) to finish, then perform a final drain pass.
		n.mu.Lock()
		n.mu.Unlock()
		for {
			select {
			case req := <-n.writeC:
				req.done <- ErrClosed
			default:
				return
			}
		}
	}()

	for {
		// Block until at least one request arrives.
		var batch []*writeReq
		select {
		case req := <-n.writeC:
			batch = append(batch, req)
		case <-ctx.Done():
			return
		}
		// Drain any additional requests that arrived while we were processing.
	drain:
		for {
			select {
			case req := <-n.writeC:
				batch = append(batch, req)
			default:
				break drain
			}
		}

		// Build a context that's cancelled when any batch caller gives up.
		// This lets the WAL abort early (in tests / context-aware WALs) if
		// all callers have abandoned the batch. Real fsyncs complete normally.
		batchCtx, batchCancel := context.WithCancel(ctx)
		for _, req := range batch {
			r := req
			go func() {
				select {
				case <-r.ctx.Done():
					batchCancel()
				case <-batchCtx.Done():
				}
			}()
		}

		// Write all entries to WAL with one fsync.
		entries := make([]*wal.Entry, len(batch))
		for i, req := range batch {
			entries[i] = &req.entry
		}

		var err error
		if n.peerSrv != nil {
			// Pipeline: run the leader WAL fsync concurrently with follower
			// replication. Broadcast entries to followers immediately so their
			// WAL writes overlap the leader's fsync. The leader fsync and the
			// follower quorum ACKs are both required before signalling callers,
			// preserving the same durability guarantee as the sequential path.
			//
			// Broadcasting before the leader fsync completes is safe: a client
			// is only ACKed after both the leader fsync and the quorum ACK
			// succeed. If the leader crashes before its fsync, the entry is
			// not yet committed, matching standard Raft behaviour (etcd uses
			// the same concurrent-write pattern).
			//
			// Ordering note: Broadcast must happen in revision order to avoid
			// the peer server's maxSent dedup filter dropping entries.
			walErrC := make(chan error, 1)
			go func() { walErrC <- n.wal.AppendBatch(batchCtx, entries) }()

			for _, req := range batch {
				n.peerSrv.Broadcast(&req.entry)
			}

			// Wait for follower ACKs according to the configured policy.
			// Use the commit loop's own context (node lifetime), NOT batchCtx:
			// batchCtx is cancelled after AppendBatch returns and passing it
			// here would cause WaitForFollowers to return instantly.
			//
			// Availability policy: if all followers disconnect mid-wait, we
			// proceed anyway — the entry is already durable in the leader's
			// WAL and will be replayed by followers when they reconnect.
			maxRev := batch[len(batch)-1].entry.Revision
			_ = n.peerSrv.WaitForFollowers(ctx, maxRev, peer.WaitMode(n.cfg.FollowerWaitMode))

			err = <-walErrC
		} else {
			err = n.wal.AppendBatch(batchCtx, entries)
		}
		batchCancel() // release watcher goroutines

		// Apply all entries to Pebble as one batch (in order).
		if err == nil {
			dbEntries := make([]wal.Entry, len(batch))
			for i, req := range batch {
				dbEntries[i] = req.entry
			}
			err = n.db.Apply(dbEntries)
		}

		// Signal all callers.
		for _, req := range batch {
			req.done <- err
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// Callers abandoned the batch; this is not a permanent fault.
				// Let the loop continue for the next batch.
				continue
			}
			// A WAL or Pebble error leaves the segment in an unknown state.
			// Stop accepting writes immediately; the defer will fence the node.
			return
		}
	}
}

// Compact removes log entries at or below revision.
func (n *Node) Compact(ctx context.Context, revision int64) error {
	if n.loadRole() == roleFollower {
		resp, err := n.forwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardCompact, Revision: revision})
		if err != nil {
			return err
		}
		return decodeErr(resp.ErrCode, resp.ErrMsg)
	}
	n.fenceMu.RLock()
	defer n.fenceMu.RUnlock()
	start := time.Now()
	n.mu.Lock()
	if n.closed.Load() {
		n.mu.Unlock()
		return ErrClosed
	}
	n.nextRev++
	e := wal.Entry{
		Revision: n.nextRev, Term: n.term, Op: wal.OpCompact,
		PrevRevision: revision,
	}
	req := newWriteReq(ctx, e)
	n.writeC <- req
	n.mu.Unlock()
	_, err := n.await(ctx, req, "compact", start, "", e.Revision)
	return err
}

// ── Read path (all roles serve locally) ──────────────────────────────────────

func (n *Node) Get(key string) (*KeyValue, error) {
	if n.closed.Load() {
		return nil, ErrClosed
	}
	n.readMu.RLock()
	defer n.readMu.RUnlock()
	if n.closed.Load() {
		return nil, ErrClosed
	}
	sv, err := n.db.Get(key)
	if err != nil || sv == nil {
		return nil, err
	}
	return toKV(sv), nil
}

func (n *Node) List(prefix string) ([]*KeyValue, error) {
	if n.closed.Load() {
		return nil, ErrClosed
	}
	n.readMu.RLock()
	defer n.readMu.RUnlock()
	if n.closed.Load() {
		return nil, ErrClosed
	}
	svs, err := n.db.List(prefix)
	if err != nil {
		return nil, err
	}
	out := make([]*KeyValue, len(svs))
	for i, sv := range svs {
		out[i] = toKV(sv)
	}
	return out, nil
}

func (n *Node) Count(prefix string) (int64, error) { return n.db.Count(prefix) }
func (n *Node) CurrentRevision() int64             { return n.db.CurrentRevision() }
func (n *Node) CompactRevision() int64             { return n.db.CompactRevision() }
func (n *Node) Config() Config                     { return n.cfg }
func (n *Node) IsLeader() bool                     { return n.loadRole() != roleFollower }

func (n *Node) WaitForRevision(ctx context.Context, rev int64) error {
	if n.closed.Load() {
		return ErrClosed
	}
	n.readMu.RLock()
	defer n.readMu.RUnlock()
	if n.closed.Load() {
		return ErrClosed
	}
	if err := n.db.WaitForRevision(ctx, rev); err != nil {
		if errors.Is(err, istore.ErrClosed) {
			return ErrClosed
		}
		return err
	}
	return nil
}

// Watch streams prefix-matching events using etcd revision semantics:
// startRev=0 means "from now"; startRev=N means replay from revision N (inclusive).
func (n *Node) Watch(ctx context.Context, prefix string, startRev int64) (<-chan Event, error) {
	if startRev > 0 && startRev <= n.db.CompactRevision() {
		return nil, ErrCompacted
	}
	// internal/store.Watch uses last-seen revision semantics (start at rev+1),
	// so adapt etcd-style startRev here.
	storeStartRev := startRev - 1
	if startRev == 0 {
		storeStartRev = n.db.CurrentRevision()
	}
	sch, err := n.db.Watch(ctx, prefix, storeStartRev)
	if err != nil {
		return nil, err
	}
	out := make(chan Event, 64)
	go func() {
		defer close(out)
		for ev := range sch {
			et := EventPut
			if ev.Deleted {
				et = EventDelete
			}
			ne := Event{Type: et, KV: toKV(ev.KV)}
			if ev.PrevKV != nil {
				ne.PrevKV = toKV(ev.PrevKV)
			}
			select {
			case out <- ne:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// uploadLocalWALSegments uploads any sealed local WAL segment files that are
// not yet present in S3. This is called when becoming leader so that local
// entries (recovered via replayLocal) are durable in object storage before
// followers can bootstrap.
func uploadLocalWALSegments(ctx context.Context, walDir string, store object.Store) {
	paths, err := wal.LocalSegments(walDir)
	if err != nil || len(paths) == 0 {
		return
	}

	// Build set of keys already in S3 to skip redundant uploads.
	s3Keys, _ := store.List(ctx, "wal/")
	inS3 := make(map[string]struct{}, len(s3Keys))
	for _, k := range s3Keys {
		inS3[k] = struct{}{}
	}

	up := makeUploader(store)
	for _, path := range paths {
		term, firstRev, ok := wal.ParseSegmentName(filepath.Base(path))
		if !ok {
			continue
		}
		objKey := wal.ObjectKey(term, firstRev)
		if _, exists := inS3[objKey]; exists {
			continue // already uploaded
		}
		if err := up(ctx, path, objKey); err != nil {
			logrus.Warnf("strata: pre-leader upload %q → %q: %v", path, objKey, err)
		}
	}
}

// ── Background checkpoint loop ────────────────────────────────────────────────

func (n *Node) checkpointLoop(ctx context.Context) {
	// Write an immediate checkpoint before entering the ticker so that any
	// entries recovered from local WAL segments (but not yet in S3) are
	// captured in the checkpoint. Without this, a crash after becoming leader
	// but before the first periodic checkpoint could leave new followers unable
	// to see those entries.
	n.forceCheckpoint(ctx)

	ticker := time.NewTicker(n.cfg.CheckpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.maybeCheckpoint(ctx)
		case <-n.checkpointTriggerC:
			n.maybeCheckpoint(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// forceCheckpoint writes a checkpoint unconditionally (bypassing the
// entriesSinceCheckpoint guard). Used on startup to capture local state.
func (n *Node) forceCheckpoint(ctx context.Context) {
	n.fenceMu.Lock()
	rev := n.db.CurrentRevision()
	if rev == 0 {
		n.fenceMu.Unlock()
		return
	}
	if err := n.wal.SealAndFlush(rev + 1); err != nil {
		n.fenceMu.Unlock()
		logrus.Errorf("strata: startup checkpoint seal WAL: %v", err)
		return
	}
	if err := n.db.Flush(); err != nil {
		n.fenceMu.Unlock()
		logrus.Errorf("strata: startup checkpoint flush pebble: %v", err)
		return
	}
	if err := checkpoint.Write(ctx, n.db.Pebble(), n.cfg.ObjectStore, n.term, rev, "", n.cfg.AncestorStore); err != nil {
		n.fenceMu.Unlock()
		logrus.Errorf("strata: startup checkpoint rev=%d: %v", rev, err)
		return
	}
	n.fenceMu.Unlock()
	atomic.StoreInt64(&n.entriesSinceCheckpoint, 0)
	metrics.CheckpointsTotal.Inc()
	logrus.Infof("strata: startup checkpoint written (rev=%d)", rev)
}

func (n *Node) maybeCheckpoint(ctx context.Context) {
	if atomic.LoadInt64(&n.entriesSinceCheckpoint) == 0 {
		return
	}
	n.fenceMu.Lock()
	rev := n.db.CurrentRevision()
	if rev == 0 {
		n.fenceMu.Unlock()
		return
	}
	if err := n.wal.SealAndFlush(rev + 1); err != nil {
		n.fenceMu.Unlock()
		logrus.Errorf("strata: checkpoint seal WAL: %v", err)
		return
	}
	if err := n.db.Flush(); err != nil {
		n.fenceMu.Unlock()
		logrus.Errorf("strata: checkpoint flush pebble: %v", err)
		return
	}
	if err := checkpoint.Write(ctx, n.db.Pebble(), n.cfg.ObjectStore, n.term, rev, "", n.cfg.AncestorStore); err != nil {
		n.fenceMu.Unlock()
		logrus.Errorf("strata: write checkpoint rev=%d: %v", rev, err)
		return
	}
	n.fenceMu.Unlock()
	atomic.StoreInt64(&n.entriesSinceCheckpoint, 0)
	metrics.CheckpointsTotal.Inc()
	logrus.Infof("strata: checkpoint written (rev=%d)", rev)

	// GC WAL segments from S3 that are fully covered by this checkpoint AND
	// that all connected followers have applied. Using min(leaderRev,
	// minFollowerAppliedRev) as the GC boundary ensures we never delete a
	// segment that a follower still needs to replay.
	gcCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	gcRev := rev
	if n.peerSrv != nil {
		if minFollower := n.peerSrv.MinFollowerAppliedRev(); minFollower < gcRev {
			gcRev = minFollower
		}
	}
	deleted, gcErr := wal.GCSegments(gcCtx, n.cfg.ObjectStore, gcRev)
	if gcErr != nil {
		logrus.Warnf("strata: wal gc: %v", gcErr)
	} else if deleted > 0 {
		metrics.WALGCTotal.Add(float64(deleted))
		logrus.Infof("strata: wal gc: deleted %d segments (covered by checkpoint rev=%d)", deleted, gcRev)
	}

	// GC old checkpoint archives from S3, keeping the 2 most recent so that
	// any in-flight bootstrap that read manifest/latest just before we
	// overwrote it can still fetch the previous checkpoint.
	cpDeleted, cpGCErr := checkpoint.GCCheckpoints(gcCtx, n.cfg.ObjectStore, 2)
	if cpGCErr != nil {
		logrus.Warnf("strata: checkpoint gc: %v", cpGCErr)
	} else if cpDeleted > 0 {
		logrus.Infof("strata: checkpoint gc: deleted %d old checkpoint(s)", cpDeleted)
	}

	sstDeleted, sstGCErr := checkpoint.GCOrphanSSTs(gcCtx, n.cfg.ObjectStore)
	if sstGCErr != nil {
		logrus.Warnf("strata: sst gc: %v", sstGCErr)
	} else if sstDeleted > 0 {
		logrus.Infof("strata: sst gc: deleted %d orphan sst(s)", sstDeleted)
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func toKV(sv *istore.KeyValue) *KeyValue {
	if sv == nil {
		return nil
	}
	return &KeyValue{
		Key: sv.Key, Value: sv.Value, Revision: sv.Revision,
		CreateRevision: sv.CreateRevision, PrevRevision: sv.PrevRevision,
		Lease: sv.Lease,
	}
}

func makeUploader(obj object.Store) wal.Uploader {
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
				logrus.Debugf("uploader: local file %q already gone but %q exists in S3 — treating as success", localPath, objectKey)
				return nil
			}
			metrics.WALUploadErrors.Inc()
			logrus.Errorf("uploader: local file %q is gone AND %q is not in S3 — segment data is lost", localPath, objectKey)
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

func replayLocal(db *istore.Store, walDir string, afterRev int64) error {
	paths, err := wal.LocalSegments(walDir)
	if err != nil {
		return err
	}
	for _, path := range paths {
		sr, closer, err := wal.OpenSegmentFile(path)
		if err != nil {
			return err
		}
		entries, readErr := sr.ReadAll()
		closer()
		if readErr != nil {
			logrus.Warnf("strata: partial local segment %q: %v", path, readErr)
		}
		var applicable []wal.Entry
		for _, e := range entries {
			if e.Revision > afterRev {
				applicable = append(applicable, *e)
			}
		}
		if len(applicable) > 0 {
			if err := db.Recover(applicable); err != nil {
				return err
			}
		}
	}
	return nil
}

// replayPinned replays the specific WAL segments listed in rp, applying
// entries with revision > afterRev. Used during RestorePoint bootstrap.
func replayPinned(ctx context.Context, db *istore.Store, rp *RestorePoint, afterRev int64) error {
	for _, seg := range rp.WALSegments {
		rc, err := rp.Store.GetVersioned(ctx, seg.Key, seg.VersionID)
		if err != nil {
			return fmt.Errorf("replayPinned get %q@%s: %w", seg.Key, seg.VersionID, err)
		}
		sr, err := wal.NewSegmentReader(rc)
		if err != nil {
			rc.Close()
			return fmt.Errorf("replayPinned segment %q: %w", seg.Key, err)
		}
		entries, readErr := sr.ReadAll()
		rc.Close()
		if readErr != nil {
			logrus.Warnf("strata: partial pinned segment %q: %v", seg.Key, readErr)
		}
		var applicable []wal.Entry
		for _, e := range entries {
			if e.Revision > afterRev {
				applicable = append(applicable, *e)
			}
		}
		if len(applicable) > 0 {
			if err := db.Recover(applicable); err != nil {
				return err
			}
		}
	}
	return nil
}

// restoreDBIfBehindCheckpoint checks whether the node's Pebble database is
// behind the latest S3 checkpoint. If so, it performs an in-place restore:
//
//  1. (No lock) Download checkpoint SSTs to a temp directory, retrying if the
//     checkpoint was GC'd between the manifest read and the download.
//  2. (fenceMu.Lock) Close the old Pebble, atomic-rename the temp dir to the
//     canonical db/ path, open the new Pebble. fenceMu blocks all concurrent
//     read/write handlers for the brief swap window.
//
// On success n.db and n.term are updated and the function returns true.
// Returns (false, nil) when no restore was needed (already at or above
// the latest checkpoint revision).
func (n *Node) restoreDBIfBehindCheckpoint(ctx context.Context) (bool, error) {
	manifest, err := checkpoint.ReadManifest(ctx, n.cfg.ObjectStore)
	if err != nil {
		return false, fmt.Errorf("read manifest: %w", err)
	}
	if manifest == nil || manifest.Revision <= n.db.CurrentRevision() {
		return false, nil // already at or above checkpoint
	}
	logrus.Infof("strata: node at rev=%d is behind checkpoint rev=%d — restoring in place",
		n.db.CurrentRevision(), manifest.Revision)

	pebbleDir := filepath.Join(n.cfg.DataDir, "db")
	tmpDir := pebbleDir + ".resync"

	// ── Phase 1: restore checkpoint SSTs to a temp dir (no lock held) ───────
	var newTerm uint64
	for attempt := 0; ; attempt++ {
		os.RemoveAll(tmpDir)
		t, _, rerr := checkpoint.Restore(ctx, n.cfg.ObjectStore, manifest.CheckpointKey, tmpDir)
		if rerr == nil {
			newTerm = t
			break
		}
		if !errors.Is(rerr, object.ErrNotFound) || attempt >= 4 {
			os.RemoveAll(tmpDir)
			return false, fmt.Errorf("restore checkpoint: %w", rerr)
		}
		// Checkpoint was GC'd between manifest read and restore — re-read
		// the manifest so we target whatever the leader most recently wrote.
		logrus.Warnf("strata: checkpoint %q not found during restore, re-reading manifest (attempt %d/5)",
			manifest.CheckpointKey, attempt+1)
		time.Sleep(500 * time.Millisecond)
		manifest, err = checkpoint.ReadManifest(ctx, n.cfg.ObjectStore)
		if err != nil {
			os.RemoveAll(tmpDir)
			return false, fmt.Errorf("re-read manifest: %w", err)
		}
		if manifest == nil {
			os.RemoveAll(tmpDir)
			return false, fmt.Errorf("manifest disappeared during restore")
		}
	}

	// ── Phase 2: swap Pebble directories under fenceMu.Lock ─────────────────
	// fenceMu.Lock pauses all in-flight reads and writes while we replace n.db.
	n.fenceMu.Lock()
	n.db.Close()
	if rerr := os.RemoveAll(pebbleDir); rerr != nil {
		n.fenceMu.Unlock()
		os.RemoveAll(tmpDir)
		return false, fmt.Errorf("remove old pebble dir: %w", rerr)
	}
	if rerr := os.Rename(tmpDir, pebbleDir); rerr != nil {
		n.fenceMu.Unlock()
		os.RemoveAll(tmpDir)
		return false, fmt.Errorf("rename resync dir: %w", rerr)
	}
	newDB, rerr := istore.Open(pebbleDir)
	if rerr != nil {
		n.fenceMu.Unlock()
		return false, fmt.Errorf("open new pebble after restore: %w", rerr)
	}
	n.db = newDB
	n.term = newTerm
	n.fenceMu.Unlock()
	return true, nil
}

// resyncFromCheckpoint is called from followLoop when IsResyncRequired fires.
// It uses restoreDBIfBehindCheckpoint to close the WAL gap, then (if a restore
// was actually performed) replaces the local WAL so subsequent Appends from the
// live stream start at the correct revision.
func (n *Node) resyncFromCheckpoint(bgCtx context.Context) error {
	ctx, cancel := context.WithTimeout(bgCtx, 5*time.Minute)
	defer cancel()

	restored, err := n.restoreDBIfBehindCheckpoint(ctx)
	if err != nil {
		return err
	}
	if !restored {
		return nil
	}

	// ── Phase 3: replace WAL and update node metadata ────────────────────────
	// followLoop is the sole WAL writer for a follower, so no concurrent
	// Append calls can race with this replacement.
	walDir := filepath.Join(n.cfg.DataDir, "wal")
	newRev := n.db.CurrentRevision()
	n.wal.Close()
	if rerr := os.RemoveAll(walDir); rerr != nil {
		logrus.Warnf("strata: remove old wal dir during resync: %v", rerr)
	}
	newWal, rerr := wal.Open(walDir, n.term, newRev+1,
		wal.WithSegmentMaxSize(n.cfg.SegmentMaxSize),
		wal.WithSegmentMaxAge(n.cfg.SegmentMaxAge),
	)
	if rerr != nil {
		return fmt.Errorf("open new wal after resync: %w", rerr)
	}
	newWal.Start(bgCtx)

	n.mu.Lock()
	n.wal = newWal
	n.nextRev = newRev
	n.mu.Unlock()

	logrus.Infof("strata: follower in-place resync complete (rev=%d term=%d)", newRev, n.term)
	return nil
}

func replayRemote(ctx context.Context, db *istore.Store, obj object.Store, afterRev int64) error {
	keys, err := obj.List(ctx, "wal/")
	if err != nil {
		return err
	}

	// Build per-term cutoffs to handle leader-change conflicts.
	//
	// When a new leader takes over at revision X, it starts writing a new term
	// from revision X+1. The old leader may have written S3 segments that cover
	// some of those same revisions (X+1, X+2, …). Applying both the old-term
	// and new-term entries at the same revision would corrupt the index.
	//
	// cutoff[term] is the minimum firstRev among all segments with term > term.
	// Old-term entries at revision >= cutoff are superseded by the new term and
	// must be skipped. For the highest term present, cutoff = math.MaxInt64 (no
	// upper bound — apply all entries).
	cutoff := walTermCutoffs(keys)

	var all []wal.Entry
	for _, key := range keys {
		term, _ := parseWALKey(key)
		termCutoff := cutoff[term]

		rc, err := obj.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("replayRemote get %q: %w", key, err)
		}
		sr, err := wal.NewSegmentReader(rc)
		if err != nil {
			rc.Close()
			return fmt.Errorf("replayRemote segment %q: %w", key, err)
		}
		entries, readErr := sr.ReadAll()
		rc.Close()
		if readErr != nil {
			logrus.Warnf("strata: partial remote segment %q: %v", key, readErr)
		}
		for _, e := range entries {
			if e.Revision <= afterRev {
				continue // already covered by checkpoint / local WAL
			}
			if e.Revision >= termCutoff {
				continue // superseded by a higher-term entry at this revision
			}
			all = append(all, *e)
		}
	}

	if len(all) == 0 {
		return nil
	}

	// Ensure deterministic order and resolve same-revision duplicates by
	// keeping the highest-term entry at each revision.
	sort.Slice(all, func(i, j int) bool {
		if all[i].Revision != all[j].Revision {
			return all[i].Revision < all[j].Revision
		}
		return all[i].Term < all[j].Term
	})
	merged := make([]wal.Entry, 0, len(all))
	for _, e := range all {
		if n := len(merged); n > 0 && merged[n-1].Revision == e.Revision {
			if e.Term >= merged[n-1].Term {
				merged[n-1] = e
			}
			continue
		}
		merged = append(merged, e)
	}

	// Fail closed on holes: a missing revision means we likely raced WAL GC
	// during bootstrap/resync and must restore from a fresher checkpoint.
	expected := afterRev + 1
	for _, e := range merged {
		if e.Revision != expected {
			return fmt.Errorf("replayRemote: missing revision(s): expected %d got %d", expected, e.Revision)
		}
		expected++
	}

	if err := db.Recover(merged); err != nil {
		return err
	}
	return nil
}

// walTermCutoffs returns a map from term → the minimum firstRev of any
// segment whose term is strictly greater. Entries from a given term at
// revisions >= cutoff are superseded by the newer term and should be skipped.
// The highest term maps to math.MaxInt64 (no cutoff).
func walTermCutoffs(keys []string) map[uint64]int64 {
	// Collect the minimum firstRev for each term.
	minFirstRev := map[uint64]int64{}
	for _, key := range keys {
		term, firstRev := parseWALKey(key)
		if existing, ok := minFirstRev[term]; !ok || firstRev < existing {
			minFirstRev[term] = firstRev
		}
	}
	cutoff := make(map[uint64]int64, len(minFirstRev))
	for term := range minFirstRev {
		c := int64(math.MaxInt64)
		for t, fr := range minFirstRev {
			if t > term && fr < c {
				c = fr
			}
		}
		cutoff[term] = c
	}
	return cutoff
}

// parseWALKey extracts term and firstRev from a WAL object key of the form
// "wal/{term:010d}/{firstRev:020d}".
func parseWALKey(key string) (term uint64, firstRev int64) {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) != 3 {
		return 0, 0
	}
	term64, _ := strconv.ParseUint(strings.TrimLeft(parts[1], "0"), 10, 64)
	rev, _ := strconv.ParseInt(strings.TrimLeft(parts[2], "0"), 10, 64)
	return term64, rev
}

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
	cancelBg               context.CancelFunc
	closeOnce              sync.Once
	closed                 atomic.Bool
	bgWg                   sync.WaitGroup // tracks long-running background goroutines (followLoop, checkpointLoop)
	readWg                 sync.WaitGroup // tracks in-flight read operations; waited before db.Close()
}

func (n *Node) loadRole() nodeRole   { return nodeRole(n.role.Load()) }
func (n *Node) storeRole(r nodeRole) { n.role.Store(int32(r)) }

// Open creates and starts a Node.
func Open(cfg Config) (*Node, error) {
	cfg.setDefaults()

	pebbleDir := filepath.Join(cfg.DataDir, "db")
	walDir := filepath.Join(cfg.DataDir, "wal")

	var (
		startRev  int64
		term      uint64 = 1
		freshNode bool   // true when pebble was restored from a checkpoint (not a restart)
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
				t, rev, err := checkpoint.Restore(ctx, cfg.ObjectStore, manifest.CheckpointKey, pebbleDir)
				if err != nil {
					return nil, fmt.Errorf("strata: restore checkpoint: %w", err)
				}
				term, startRev = t, rev
				logrus.Infof("strata: checkpoint restored (term=%d rev=%d)", term, startRev)
				freshNode = true
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

	w, err := wal.Open(walDir, term, startRev+1,
		wal.WithUploader(uploader),
		wal.WithSegmentMaxSize(cfg.SegmentMaxSize),
		wal.WithSegmentMaxAge(cfg.SegmentMaxAge),
	)
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
	case cfg.ObjectStore != nil:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		// Close the GC race: between the initial manifest read and now, the leader
		// may have written a newer checkpoint and GC'd the WAL segments that span
		// [startRev+1, newCheckpointRev]. Re-reading the manifest and re-restoring
		// from the fresher checkpoint means replayRemote only needs segments the
		// leader hasn't yet deleted.
		if freshNode {
			if freshManifest, merr := checkpoint.ReadManifest(ctx, cfg.ObjectStore); merr == nil &&
				freshManifest != nil && freshManifest.Revision > startRev {
				logrus.Infof("strata: fresher checkpoint available (rev=%d > startRev=%d); re-restoring to close GC race", freshManifest.Revision, startRev)
				db.Close()
				if rerr := os.RemoveAll(pebbleDir); rerr != nil {
					w.Close()
					return nil, fmt.Errorf("strata: remove stale pebble dir: %w", rerr)
				}
				newTerm, newRev, rerr := checkpoint.Restore(ctx, cfg.ObjectStore, freshManifest.CheckpointKey, pebbleDir)
				if rerr != nil {
					w.Close()
					return nil, fmt.Errorf("strata: re-restore checkpoint: %w", rerr)
				}
				freshDB, rerr := istore.Open(pebbleDir)
				if rerr != nil {
					w.Close()
					return nil, fmt.Errorf("strata: reopen store after checkpoint refresh: %w", rerr)
				}
				w.Close()
				freshW, rerr := wal.Open(walDir, newTerm, newRev+1,
					wal.WithUploader(uploader),
					wal.WithSegmentMaxSize(cfg.SegmentMaxSize),
					wal.WithSegmentMaxAge(cfg.SegmentMaxAge),
				)
				if rerr != nil {
					freshDB.Close()
					return nil, fmt.Errorf("strata: reopen wal after checkpoint refresh: %w", rerr)
				}
				// Use pebble's actual currentRev as startRev (same reasoning as
				// after the initial pebble open above).
				db, w, term, startRev = freshDB, freshW, newTerm, freshDB.CurrentRevision()
				logrus.Infof("strata: checkpoint refreshed to rev=%d (term=%d)", startRev, term)
			}
		}

		if err := replayRemote(ctx, db, cfg.ObjectStore, startRev); err != nil {
			w.Close()
			db.Close()
			return nil, fmt.Errorf("strata: remote WAL replay: %w", err)
		}
	}

	bgCtx, bgCancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:      cfg,
		term:     term,
		db:       db,
		wal:      w,
		cancelBg: bgCancel,
		nextRev:  db.CurrentRevision(),
		pending:  make(map[string]pendingKV),
		writeC:   make(chan *writeReq, 1024),
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

	rec, won, err := lock.TryAcquire(ctx, n.term)
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

	// Upload any local WAL segments that were not yet in S3. This covers the
	// same-node re-election case where the previous WAL had no uploader (or
	// crashed before the upload completed). The immediate startup checkpoint
	// written by checkpointLoop also covers these entries, but uploading them
	// first narrows the window where a crash could lose data.
	upCtx, upCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	uploadLocalWALSegments(upCtx, walDir, n.cfg.ObjectStore)
	upCancel()

	// Replay any remote WAL entries not yet in our Pebble. A follower that wins
	// election may be behind the former leader: the former leader may have
	// committed entries and uploaded them to S3 before it closed, but this node
	// never received them via the stream. Without this replay the new leader
	// would open its WAL at a firstRev that is lower than some committed entries
	// still in S3. The subsequent checkpoint + GC would then delete those S3
	// segments while the checkpoint only covers the new (lower) revision —
	// permanently losing the committed entries.
	if n.cfg.ObjectStore != nil {
		reCtx, reCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		if err := replayRemote(reCtx, n.db, n.cfg.ObjectStore, n.db.CurrentRevision()); err != nil {
			logrus.Warnf("strata: becomeLeader replay remote WAL: %v (proceeding)", err)
		}
		reCancel()
	}

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
func (n *Node) watchLoop(ctx context.Context, lock *election.Lock, term uint64) {
	ticker := time.NewTicker(n.cfg.LeaderWatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			rec, err := lock.Read(rCtx)
			cancel()
			if err != nil {
				logrus.Warnf("strata: leader watch: read lock: %v", err)
				continue
			}
			if rec == nil || rec.Term != term || rec.NodeID != n.cfg.NodeID {
				logrus.Errorf("strata: leader watch: lock superseded (current: %+v) — stepping down", rec)
				n.cancelBg()
				return
			}
		case <-ctx.Done():
			rCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			lock.Release(rCtx)
			cancel()
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
		err := cli.Follow(bgCtx, fromRev, func(e wal.Entry) error {
			if err := n.wal.Append(&e); err != nil {
				return err
			}
			if err := n.db.Apply([]wal.Entry{e}); err != nil {
				return err
			}
			// Advance only after a successful apply so a reconnect retries
			// the same revision rather than skipping it.
			fromRev = e.Revision + 1
			return nil
		})

		if bgCtx.Err() != nil {
			return
		}

		if peer.IsResyncRequired(err) {
			logrus.Error("strata: follower resync required — restart the node to re-bootstrap from S3")
			n.cancelBg()
			return
		}

		if peer.IsLeaderUnreachable(err) {
			logrus.Warn("strata: leader unreachable — attempting election takeover")
			newCli, promoted := n.attemptPromotion(bgCtx, lock)
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
// Returns (nil, false) on S3 errors.
func (n *Node) attemptPromotion(bgCtx context.Context, lock *election.Lock) (*peer.Client, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rec, won, err := lock.TakeOver(ctx, n.term)
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

// Close shuts down the node cleanly.
func (n *Node) Close() error {
	var err error
	n.closeOnce.Do(func() {
		n.closed.Store(true)
		n.cancelBg()
		if cli := n.leaderCli.Load(); cli != nil {
			cli.Close()
		}
		if n.peerGRPC != nil {
			n.peerGRPC.Stop() // terminates all active streams immediately
		} else if n.peerLis != nil {
			n.peerLis.Close()
		}
		// Wait for followLoop / checkpointLoop to exit before closing WAL and
		// DB. cancelBg has already been called above, so the loops will drain
		// promptly; we just need to avoid closing DB under a concurrent Apply.
		n.bgWg.Wait()
		// Wait for any in-flight read operations (Get, List, WaitForRevision)
		// that passed the n.closed check but haven't called into pebble yet.
		n.readWg.Wait()
		if werr := n.wal.Close(); werr != nil {
			logrus.Errorf("strata: wal close: %v", werr)
			err = werr
		}
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
	atomic.AddInt64(&n.entriesSinceCheckpoint, 1)
	return rev, nil
}

// commitLoop is the group-commit pipeline for leader/single-node writes.
// It drains writeC, writes all entries to WAL with a single fsync, applies
// them to Pebble as a batch, and signals each caller's done channel.
func (n *Node) commitLoop(ctx context.Context) {
	defer func() {
		// Fence the node so no new writes can enter the queue after we drain it.
		// Acquire n.mu so that any writer currently between the closed check and
		// the writeC send (still holding the lock) finishes before we mark closed.
		n.mu.Lock()
		n.closed.Store(true)
		n.mu.Unlock()
		// Drain any requests already buffered in writeC.
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
		err := n.wal.AppendBatch(batchCtx, entries)
		batchCancel() // release watcher goroutines

		// Apply all entries to Pebble as one batch (in order).
		if err == nil {
			dbEntries := make([]wal.Entry, len(batch))
			for i, req := range batch {
				dbEntries[i] = req.entry
			}
			err = n.db.Apply(dbEntries)
		}

		// Broadcast to followers in revision order before unblocking callers.
		// This must happen here (not in each caller's goroutine) because the
		// peer server's maxSent dedup filter drops entries with rev <= maxSent;
		// if callers broadcast concurrently they can send out of order.
		if err == nil && n.peerSrv != nil {
			for _, req := range batch {
				n.peerSrv.Broadcast(&req.entry)
			}
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
	n.readWg.Add(1)
	defer n.readWg.Done()
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
	n.readWg.Add(1)
	defer n.readWg.Done()
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
	n.readWg.Add(1)
	defer n.readWg.Done()
	if n.closed.Load() {
		return ErrClosed
	}
	return n.db.WaitForRevision(ctx, rev)
}

func (n *Node) Watch(ctx context.Context, prefix string, startRev int64) (<-chan Event, error) {
	if startRev > 0 && startRev < n.db.CompactRevision() {
		return nil, ErrCompacted
	}
	sch, err := n.db.Watch(ctx, prefix, startRev)
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
		case <-ctx.Done():
			return
		}
	}
}

// forceCheckpoint writes a checkpoint unconditionally (bypassing the
// entriesSinceCheckpoint guard). Used on startup to capture local state.
func (n *Node) forceCheckpoint(ctx context.Context) {
	rev := n.db.CurrentRevision()
	if rev == 0 {
		return
	}
	if err := n.wal.SealAndFlush(rev + 1); err != nil {
		logrus.Errorf("strata: startup checkpoint seal WAL: %v", err)
		return
	}
	if err := checkpoint.Write(ctx, n.db.Pebble(), n.cfg.ObjectStore, n.term, rev, ""); err != nil {
		logrus.Errorf("strata: startup checkpoint rev=%d: %v", rev, err)
		return
	}
	atomic.StoreInt64(&n.entriesSinceCheckpoint, 0)
	metrics.CheckpointsTotal.Inc()
	logrus.Infof("strata: startup checkpoint written (rev=%d)", rev)
}

func (n *Node) maybeCheckpoint(ctx context.Context) {
	if atomic.LoadInt64(&n.entriesSinceCheckpoint) == 0 {
		return
	}
	rev := n.db.CurrentRevision()
	if rev == 0 {
		return
	}
	if err := n.wal.SealAndFlush(rev + 1); err != nil {
		logrus.Errorf("strata: checkpoint seal WAL: %v", err)
		return
	}
	if err := checkpoint.Write(ctx, n.db.Pebble(), n.cfg.ObjectStore, n.term, rev, ""); err != nil {
		logrus.Errorf("strata: write checkpoint rev=%d: %v", rev, err)
		return
	}
	atomic.StoreInt64(&n.entriesSinceCheckpoint, 0)
	metrics.CheckpointsTotal.Inc()
	logrus.Infof("strata: checkpoint written (rev=%d)", rev)

	// GC WAL segments from S3 that are fully covered by this checkpoint.
	gcCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	deleted, gcErr := wal.GCSegments(gcCtx, n.cfg.ObjectStore, rev)
	if gcErr != nil {
		logrus.Warnf("strata: wal gc: %v", gcErr)
	} else if deleted > 0 {
		metrics.WALGCTotal.Add(float64(deleted))
		logrus.Infof("strata: wal gc: deleted %d segments (covered by checkpoint rev=%d)", deleted, rev)
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
		var applicable []wal.Entry
		for _, e := range entries {
			if e.Revision <= afterRev {
				continue // already covered by checkpoint / local WAL
			}
			if e.Revision >= termCutoff {
				continue // superseded by a higher-term entry at this revision
			}
			applicable = append(applicable, *e)
		}
		if len(applicable) > 0 {
			if err := db.Recover(applicable); err != nil {
				return err
			}
		}
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

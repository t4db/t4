package strata

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/makhov/strata/internal/checkpoint"
	"github.com/makhov/strata/internal/election"
	"github.com/makhov/strata/internal/metrics"
	"github.com/makhov/strata/internal/object"
	"github.com/makhov/strata/internal/peer"
	istore "github.com/makhov/strata/internal/store"
	"github.com/makhov/strata/internal/wal"
)

// Sentinel errors.
var (
	ErrKeyExists = errors.New("strata: key already exists")
	ErrNotLeader = errors.New("strata: this node is not the leader; writes are rejected")
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
type Node struct {
	cfg  Config
	term uint64
	role atomic.Int32 // stores nodeRole values; use loadRole/storeRole

	db  *istore.Store
	wal *wal.WAL // non-nil on leader/single; non-nil on follower (local WAL, no uploader)

	// mu serialises all leader writes for CAS safety and role transitions.
	mu sync.Mutex

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
	if cfg.ObjectStore != nil {
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
			}
		}
	}

	// ── Open Pebble ──────────────────────────────────────────────────────────
	db, err := istore.Open(pebbleDir)
	if err != nil {
		return nil, fmt.Errorf("strata: open store: %w", err)
	}
	if dbRev := db.CurrentRevision(); dbRev > startRev {
		startRev = dbRev
	}

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

	// ── Replay remote WAL (S3) ───────────────────────────────────────────────
	if cfg.ObjectStore != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
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
	if n.loadRole() != roleFollower && cfg.ObjectStore != nil && cfg.CheckpointInterval > 0 {
		go n.checkpointLoop(bgCtx)
	}
	if n.loadRole() == roleFollower {
		go n.followLoop(bgCtx)
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
			fromRev = e.Revision + 1
			return n.db.Apply([]wal.Entry{e})
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
		if n.cfg.ObjectStore != nil && n.cfg.CheckpointInterval > 0 {
			go n.checkpointLoop(bgCtx)
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
		n.cancelBg()
		if cli := n.leaderCli.Load(); cli != nil {
			cli.Close()
		}
		if n.peerGRPC != nil {
			n.peerGRPC.Stop() // terminates all active streams immediately
		} else if n.peerLis != nil {
			n.peerLis.Close()
		}
		if werr := n.wal.Close(); werr != nil {
			logrus.Errorf("strata: wal close: %v", werr)
		}
		err = n.db.Close()
	})
	return err
}

// ── Write path (leader / single-node execute; follower forwards) ──────────────

// Put creates or updates key with value. Returns the new revision.
func (n *Node) Put(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	if n.loadRole() == roleFollower {
		resp, err := n.forwardWrite(ctx, &peer.ForwardRequest{Op: peer.ForwardPut, Key: key, Value: value, Lease: lease})
		if err != nil {
			return 0, err
		}
		return resp.Revision, decodeErr(resp.ErrCode, resp.ErrMsg)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.putLocked(key, value, lease)
}

func (n *Node) putLocked(key string, value []byte, lease int64) (int64, error) {
	existing, err := n.db.Get(key)
	if err != nil {
		return 0, err
	}
	curRev := n.db.CurrentRevision()
	newRev := curRev + 1
	var op wal.Op
	var createRev, prevRev int64
	if existing == nil {
		op, createRev = wal.OpCreate, newRev
	} else {
		op, createRev, prevRev = wal.OpUpdate, existing.CreateRevision, existing.Revision
	}
	return n.appendAndApply(wal.Entry{
		Revision: newRev, Term: n.term, Op: op,
		Key: key, Value: value, Lease: lease,
		CreateRevision: createRev, PrevRevision: prevRev,
	})
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
	n.mu.Lock()
	defer n.mu.Unlock()
	existing, err := n.db.Get(key)
	if err != nil {
		return 0, err
	}
	if existing != nil {
		return 0, ErrKeyExists
	}
	curRev := n.db.CurrentRevision()
	newRev := curRev + 1
	return n.appendAndApply(wal.Entry{
		Revision: newRev, Term: n.term, Op: wal.OpCreate,
		Key: key, Value: value, Lease: lease, CreateRevision: newRev,
	})
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
	n.mu.Lock()
	defer n.mu.Unlock()
	existing, err := n.db.Get(key)
	if err != nil {
		return 0, nil, false, err
	}
	curRev := n.db.CurrentRevision()
	if existing == nil || existing.Revision != revision {
		return curRev, toKV(existing), false, nil
	}
	newRev, err := n.appendAndApply(wal.Entry{
		Revision: curRev + 1, Term: n.term, Op: wal.OpUpdate,
		Key: key, Value: value, Lease: lease,
		CreateRevision: existing.CreateRevision, PrevRevision: existing.Revision,
	})
	if err != nil {
		return 0, nil, false, err
	}
	return newRev, toKV(existing), true, nil
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
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.deleteLocked(key)
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
	n.mu.Lock()
	defer n.mu.Unlock()
	existing, err := n.db.Get(key)
	if err != nil {
		return 0, nil, false, err
	}
	curRev := n.db.CurrentRevision()
	if existing == nil {
		return curRev, nil, false, nil
	}
	if revision != 0 && existing.Revision != revision {
		return curRev, toKV(existing), false, nil
	}
	newRev, err := n.deleteLocked(key)
	if err != nil {
		return 0, nil, false, err
	}
	return newRev, toKV(existing), true, nil
}

func (n *Node) deleteLocked(key string) (int64, error) {
	existing, err := n.db.Get(key)
	if err != nil || existing == nil {
		return 0, err
	}
	curRev := n.db.CurrentRevision()
	return n.appendAndApply(wal.Entry{
		Revision: curRev + 1, Term: n.term, Op: wal.OpDelete,
		Key: key, CreateRevision: existing.CreateRevision, PrevRevision: existing.Revision,
	})
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

// appendAndApply writes e to WAL, applies to store, and broadcasts to followers.
func (n *Node) appendAndApply(e wal.Entry) (int64, error) {
	op := opLabel(e.Op)
	start := time.Now()
	if err := n.wal.Append(&e); err != nil {
		metrics.WriteErrors.WithLabelValues(op).Inc()
		return 0, fmt.Errorf("strata: wal append: %w", err)
	}
	if err := n.db.Apply([]wal.Entry{e}); err != nil {
		metrics.WriteErrors.WithLabelValues(op).Inc()
		return 0, fmt.Errorf("strata: apply: %w", err)
	}
	metrics.WritesTotal.WithLabelValues(op).Inc()
	metrics.WriteDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	metrics.CurrentRevision.Set(float64(e.Revision))
	atomic.AddInt64(&n.entriesSinceCheckpoint, 1)
	if n.peerSrv != nil {
		n.peerSrv.Broadcast(&e)
	}
	return e.Revision, nil
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
	curRev := n.db.CurrentRevision()
	e := wal.Entry{
		Revision: curRev + 1, Term: n.term, Op: wal.OpCompact,
		PrevRevision: revision,
	}
	if err := n.wal.Append(&e); err != nil {
		return fmt.Errorf("strata: compact wal append: %w", err)
	}
	if err := n.db.Apply([]wal.Entry{e}); err != nil {
		return err
	}
	if n.peerSrv != nil {
		n.peerSrv.Broadcast(&e)
	}
	return nil
}

// ── Read path (all roles serve locally) ──────────────────────────────────────

func (n *Node) Get(key string) (*KeyValue, error) {
	sv, err := n.db.Get(key)
	if err != nil || sv == nil {
		return nil, err
	}
	return toKV(sv), nil
}

func (n *Node) List(prefix string) ([]*KeyValue, error) {
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
	return n.db.WaitForRevision(ctx, rev)
}

func (n *Node) Watch(ctx context.Context, prefix string, startRev int64) (<-chan Event, error) {
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

// ── Background checkpoint loop ────────────────────────────────────────────────

func (n *Node) checkpointLoop(ctx context.Context) {
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
			metrics.WALUploadErrors.Inc()
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

func replayRemote(ctx context.Context, db *istore.Store, obj object.Store, afterRev int64) error {
	keys, err := obj.List(ctx, "wal/")
	if err != nil {
		return err
	}
	for _, key := range keys {
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

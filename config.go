package strata

import (
	"os"
	"time"

	"google.golang.org/grpc/credentials"

	"github.com/makhov/strata/pkg/object"
)

// ReadConsistency controls the consistency guarantee for read operations served
// by the etcd adapter. It acts as a server-side override on top of the per-request
// Serializable flag sent by etcd clients.
type ReadConsistency string

const (
	// ReadConsistencyLinearizable (default) respects each request's Serializable
	// flag: linearizable requests use the ReadIndex pattern (follower syncs to the
	// leader's revision before serving); serializable requests are served locally
	// without any leader contact.
	ReadConsistencyLinearizable ReadConsistency = "linearizable"

	// ReadConsistencySerializable forces all reads to be served from the local
	// Pebble store, bypassing the ReadIndex sync even when the client requests
	// linearizability. Reads are fast (~450 ns on a single node) and scale
	// horizontally, but a follower may return data that is slightly behind the
	// leader. Choose this when throughput and horizontal read scaling matter more
	// than strict linearizability (e.g., when each API server has a dedicated
	// strata leader).
	ReadConsistencySerializable ReadConsistency = "serializable"
)


// Config holds all configuration for a Node.
type Config struct {
	// ── Read consistency ─────────────────────────────────────────────────────

	// ReadConsistency controls the consistency guarantee for reads served
	// through the etcd adapter.
	// Default: ReadConsistencyLinearizable (etcd-compatible; free for
	// leaders and single-node deployments since the sync is a no-op).
	ReadConsistency ReadConsistency

	// ── Storage ──────────────────────────────────────────────────────────────

	// DataDir is the directory used for local Pebble data and WAL segments.
	// Required.
	DataDir string

	// ObjectStore is used to archive WAL segments and checkpoints and to run
	// leader election. If nil the node runs in single-node mode.
	ObjectStore object.Store

	// RestorePoint, if set, causes the node to bootstrap from a specific
	// point in time on first boot rather than reading the latest checkpoint
	// from ObjectStore. See RestorePoint for details.
	RestorePoint *RestorePoint

	// BranchPoint, if set, causes the node to bootstrap from a specific source
	// checkpoint on first boot. Unlike RestorePoint, it does not require S3
	// versioning. The source store's SST files are shared until the branch node
	// creates its own checkpoints and compacts away the inherited data.
	// Ignored on subsequent restarts (when local data directory already exists).
	BranchPoint *BranchPoint

	// AncestorStore is the object store of the source node, set for branch nodes.
	// When non-nil, checkpoint.Write skips uploading SST files already present in
	// AncestorStore and records them as AncestorSSTFiles instead.
	AncestorStore object.Store

	// SegmentMaxSize is the byte threshold that triggers WAL segment rotation.
	// Default: 50 MB.
	SegmentMaxSize int64

	// SegmentMaxAge is the time threshold that triggers WAL segment rotation
	// and, when WALSyncUpload is false, the maximum interval between async S3
	// uploads. Default: 10 s.
	SegmentMaxAge time.Duration

	// WALSyncUpload controls whether WAL segments are uploaded to S3
	// synchronously before a write is acknowledged in single-node mode.
	//
	// true (default): each write blocks until its WAL segment is durably in
	// S3. Safe even if local disk is ephemeral (e.g. emptyDir in Kubernetes).
	//
	// false: uploads happen asynchronously every SegmentMaxAge. Write latency
	// is much lower, but up to SegmentMaxAge of acknowledged writes can be lost
	// if local storage is destroyed before the upload completes. Use this when
	// local storage is already durable (e.g. a PVC).
	//
	// Has no effect in multi-node mode; quorum ACK provides durability without
	// blocking on S3, so uploads are always async there.
	WALSyncUpload *bool

	// CheckpointInterval controls how often the leader writes a checkpoint.
	// Default: 15 minutes.
	CheckpointInterval time.Duration

	// CheckpointEntries triggers a checkpoint after this many WAL entries
	// regardless of time. 0 means disabled.
	CheckpointEntries int64

	// ── Multi-node (leader election + replication) ────────────────────────────
	//
	// Multi-node mode is enabled when PeerListenAddr is non-empty.
	// ObjectStore must also be configured (it hosts the leader lock).

	// NodeID is a stable, unique identifier for this node.
	// Defaults to the machine hostname.
	NodeID string

	// PeerListenAddr is the address on which the peer WAL-streaming gRPC
	// server listens (e.g. "0.0.0.0:3380"). Empty → single-node mode.
	PeerListenAddr string

	// AdvertisePeerAddr is the address followers use to reach this node's peer
	// server. Defaults to PeerListenAddr.
	AdvertisePeerAddr string

	// LeaderWatchInterval is how often the leader reads the lock from S3 to
	// detect if it has been superseded. Read-only; no renewals.
	// Default: 5 minutes.
	LeaderWatchInterval time.Duration

	// FollowerMaxRetries is the number of consecutive stream failures a follower
	// tolerates before attempting a TakeOver election.
	// Default: 5.
	FollowerMaxRetries int

	// PeerBufferSize is the number of WAL entries the leader buffers for
	// follower catch-up. Default: 10 000.
	PeerBufferSize int

	// PeerServerTLS is the transport credentials used by the leader's peer
	// gRPC server. Nil means plaintext (only safe inside a trusted network).
	PeerServerTLS credentials.TransportCredentials

	// PeerClientTLS is the transport credentials used by a follower's peer
	// gRPC client. Must be set when PeerServerTLS is set on the leader.
	PeerClientTLS credentials.TransportCredentials

	// ── Observability ────────────────────────────────────────────────────────

	// MetricsAddr is the TCP address for the Prometheus /metrics, /healthz,
	// and /readyz HTTP endpoints (e.g. "0.0.0.0:9090"). Empty means disabled.
	MetricsAddr string
}

func (c *Config) setDefaults() {
	if c.ReadConsistency == "" {
		c.ReadConsistency = ReadConsistencyLinearizable
	}
	if c.SegmentMaxSize == 0 {
		c.SegmentMaxSize = 50 << 20
	}
	if c.SegmentMaxAge == 0 {
		c.SegmentMaxAge = 10 * time.Second
	}
	if c.CheckpointInterval == 0 {
		c.CheckpointInterval = 15 * time.Minute
	}
	if c.WALSyncUpload == nil {
		t := true
		c.WALSyncUpload = &t // default: sync for single-node safety
	}
	if c.NodeID == "" {
		if h, err := os.Hostname(); err == nil {
			c.NodeID = h
		} else {
			c.NodeID = "node-0"
		}
	}
	if c.AdvertisePeerAddr == "" {
		c.AdvertisePeerAddr = c.PeerListenAddr
	}
	if c.LeaderWatchInterval == 0 {
		c.LeaderWatchInterval = 5 * time.Minute
	}
	if c.FollowerMaxRetries == 0 {
		c.FollowerMaxRetries = 5
	}
	if c.PeerBufferSize == 0 {
		c.PeerBufferSize = 10_000
	}
}

package t4

import (
	"context"
	"os"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/credentials"

	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
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
	// t4 leader).
	ReadConsistencySerializable ReadConsistency = "serializable"
)

// FollowerWaitMode controls how many follower ACKs a leader waits for before
// applying a batch locally and acknowledging the write to the client.
type FollowerWaitMode string

const (
	// FollowerWaitNone skips follower ACK waiting entirely.
	FollowerWaitNone FollowerWaitMode = "none"

	// FollowerWaitQuorum waits for a majority of the cluster. Since the leader
	// already has the entry durably in its WAL, this means waiting for enough
	// followers to reach quorum with the leader included.
	FollowerWaitQuorum FollowerWaitMode = "quorum"

	// FollowerWaitAll waits for every connected follower present when the batch
	// starts waiting.
	FollowerWaitAll FollowerWaitMode = "all"
)

// AutoCompactMode selects the automatic history compaction strategy.
type AutoCompactMode string

const (
	// AutoCompactOff disables automatic history compaction.
	AutoCompactOff AutoCompactMode = "off"

	// AutoCompactTime keeps history newer than AutoCompactRetention.
	AutoCompactTime AutoCompactMode = "time"

	// AutoCompactRevision keeps the last AutoCompactRevisionRetention revisions.
	AutoCompactRevision AutoCompactMode = "revision"
)

// WAL is the write-ahead log implementation used by Node. The default opens
// T4's filesystem WAL. Advanced embedders can provide an alternate
// implementation for constrained runtimes such as browser WASM demos.
type WALWriter interface {
	Open(dir string, term uint64, startRev int64) error
	ReplayLocal(db wal.RecoveryStore, afterRev int64) error
	Append(e *wal.Entry) error
	AppendBatch(ctx context.Context, entries []*wal.Entry) error
	SealAndFlush(nextSeq int64) error
	Close() error
}

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

	// PebbleOptions are appended to T4's Pebble options before opening the
	// local state machine. This is an expert hook; production deployments
	// normally leave it empty.
	PebbleOptions []func(*pebble.Options)

	// WAL overrides the filesystem WAL. Open is called during Node startup.
	// This is an expert hook for alternate runtimes; production deployments
	// normally leave it nil.
	WAL WALWriter

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

	// AutoCompactMode selects the automatic history compaction strategy.
	// Empty means infer from the retention fields: time when
	// AutoCompactRetention > 0, revision when AutoCompactRevisionRetention > 0,
	// otherwise off.
	AutoCompactMode AutoCompactMode

	// AutoCompactRetention configures time-window autocompaction. The leader
	// periodically compacts history older than this duration while preserving
	// current key values.
	// Default: 0 (disabled).
	AutoCompactRetention time.Duration

	// AutoCompactRevisionRetention enables revision-window autocompaction when
	// AutoCompactMode is revision. The leader keeps approximately this many
	// recent revisions and compacts older history.
	// Default: 0 (disabled).
	AutoCompactRevisionRetention int64

	// AutoCompactInterval controls how often the leader checks whether the
	// autocompaction watermark can advance. Defaults to AutoCompactSampleInterval
	// in time mode and 1 minute in revision mode.
	AutoCompactInterval time.Duration

	// AutoCompactSampleInterval controls how often T4 records revision/time
	// samples used to map a retention cutoff to a revision. Coarser values
	// retain up to roughly this much extra history. Default:
	// clamp(AutoCompactRetention/7, 1 minute, 24 hours).
	AutoCompactSampleInterval time.Duration

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

	// FollowerWaitMode controls how many follower ACKs the leader waits for
	// before applying a batch to Pebble and acknowledging it to the client.
	// Default: FollowerWaitQuorum.
	FollowerWaitMode FollowerWaitMode

	// PeerBufferSize is the number of WAL entries the leader buffers for
	// follower catch-up. Default: 10 000.
	PeerBufferSize int

	// WatchSendTimeout is the maximum time the etcd Watch surface waits for
	// a single WatchResponse to enter the per-stream send buffer before the
	// watch is considered slow and canceled. On cancellation the server
	// best-effort sends `Canceled=true, CancelReason="mvcc: watcher is
	// slow"` and releases the per-watch goroutine; other watches on the
	// same stream are unaffected. Default: 30 s.
	WatchSendTimeout time.Duration

	// PeerServerTLS is the transport credentials used by the leader's peer
	// gRPC server. Nil means plaintext (only safe inside a trusted network).
	PeerServerTLS credentials.TransportCredentials

	// PeerClientTLS is the transport credentials used by a follower's peer
	// gRPC client. Must be set when PeerServerTLS is set on the leader.
	PeerClientTLS credentials.TransportCredentials

	// ── Observability ────────────────────────────────────────────────────────

	// Logger, if set, receives all t4 log output.  When nil the global
	// logrus logger is used (logrus.StandardLogger()).
	//
	// In embedded mode, supply your own Logger to control destination, level,
	// and format independently from t4's internals.  This also silences the
	// noisy Pebble storage-engine messages (WAL recovery, compaction) which
	// are downgraded to DEBUG by the built-in Pebble adapter – so setting
	// your logger's level to INFO or above is enough to hide them.
	//
	// *logrus.Logger already implements Logger.  For other libraries:
	//
	//	// Silence all t4 output:
	//	cfg.Logger = t4.NoopLogger
	//
	//	// Logrus at warn level:
	//	l := logrus.New()
	//	l.SetLevel(logrus.WarnLevel)
	//	cfg.Logger = l
	Logger Logger

	// MetricsRegisterer is the Prometheus registerer used to register all
	// t4 metrics. When nil, prometheus.DefaultRegisterer is used.
	// Pass a *prometheus.Registry to isolate t4 metrics from the global
	// registry (useful when embedding t4 in applications that manage their
	// own Prometheus registries). The first registration also sets the gatherer
	// served by Node.ServeMetrics when the registerer implements
	// prometheus.Gatherer; otherwise /metrics falls back to
	// prometheus.DefaultGatherer.
	MetricsRegisterer prometheus.Registerer

	// ObjectStoreEncryption, when non-nil, encrypts all object-store data at
	// rest using AES-256-GCM. Local Pebble files and local WAL files remain
	// plaintext.
	ObjectStoreEncryption *ObjectStoreEncryptionConfig
}

// ObjectStoreEncryptionConfig holds object-store encryption settings.
type ObjectStoreEncryptionConfig struct {
	// KeyProvider supplies the AES-256 key used to encrypt and decrypt object
	// bodies. It must be non-nil when ObjectStoreEncryption is set.
	KeyProvider object.KeyProvider
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
	if c.AutoCompactMode == "" {
		switch {
		case c.AutoCompactRetention > 0:
			c.AutoCompactMode = AutoCompactTime
		case c.AutoCompactRevisionRetention > 0:
			c.AutoCompactMode = AutoCompactRevision
		default:
			c.AutoCompactMode = AutoCompactOff
		}
	}
	switch c.AutoCompactMode {
	case AutoCompactTime:
		if c.AutoCompactSampleInterval <= 0 {
			c.AutoCompactSampleInterval = defaultAutoCompactSampleInterval(c.AutoCompactRetention)
		}
		if c.AutoCompactInterval <= 0 {
			c.AutoCompactInterval = c.AutoCompactSampleInterval
		}
	case AutoCompactRevision:
		if c.AutoCompactInterval <= 0 {
			c.AutoCompactInterval = time.Minute
		}
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
	if c.FollowerWaitMode == "" {
		c.FollowerWaitMode = FollowerWaitQuorum
	}
	if c.PeerBufferSize == 0 {
		c.PeerBufferSize = 10_000
	}
	if c.WatchSendTimeout == 0 {
		c.WatchSendTimeout = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = defaultLogger()
	}
}

func defaultAutoCompactSampleInterval(retention time.Duration) time.Duration {
	interval := retention / 7
	if interval < time.Minute {
		return time.Minute
	}
	if interval > 24*time.Hour {
		return 24 * time.Hour
	}
	return interval
}

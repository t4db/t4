package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/etcd/auth"
	"github.com/t4db/t4/internal/metrics"
	"github.com/t4db/t4/pkg/object"
)

func runCmd() *cobra.Command {
	var (
		s3  *s3Flags
		enc *objectStoreEncryptionFlags

		dataDir               string
		listenAddr            string
		segmentMaxSizeMB      int64
		segmentMaxAgeSec      int
		checkpointIntervalMin int
		checkpointEntries     int64
		autoCompactMode       string
		autoCompactRetention  time.Duration
		autoCompactRevisions  int64
		autoCompactInterval   time.Duration
		autoCompactSampleInt  time.Duration
		readConsistency       string
		logLevel              string
		// multi-node
		nodeID                 string
		walSyncUpload          string // "true", "false", or "" (default)
		peerListenAddr         string
		advertisePeerAddr      string
		leaderWatchIntervalSec int
		followerMaxRetries     int
		followerWaitMode       string
		watchSendTimeout       time.Duration
		// peer mTLS
		peerTLSCA   string
		peerTLSCert string
		peerTLSKey  string
		// client TLS
		clientTLSCert string
		clientTLSKey  string
		clientTLSCA   string
		// auth
		authEnabled bool
		tokenTTLSec int
		// observability
		metricsAddr string
		// branch node
		branchPrefix     string
		branchCheckpoint string
		// gRPC keepalive (etcd-compatible defaults)
		grpcKeepaliveMinTime             time.Duration
		grpcKeepaliveTime                time.Duration
		grpcKeepaliveTimeout             time.Duration
		grpcKeepalivePermitWithoutStream bool
	)

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run a t4 node",
		RunE: func(cmd *cobra.Command, _ []string) error {
			lvl, err := logrus.ParseLevel(logLevel)
			if err != nil {
				return fmt.Errorf("invalid log level %q: %w", logLevel, err)
			}
			logrus.SetLevel(lvl)

			if walSyncUpload != "" && walSyncUpload != "true" && walSyncUpload != "false" {
				return fmt.Errorf("--wal-sync-upload must be \"true\" or \"false\", got %q", walSyncUpload)
			}
			switch t4.FollowerWaitMode(followerWaitMode) {
			case "", t4.FollowerWaitNone, t4.FollowerWaitQuorum, t4.FollowerWaitAll:
			default:
				return fmt.Errorf("--follower-wait-mode must be one of \"none\", \"quorum\", or \"all\", got %q", followerWaitMode)
			}
			switch t4.AutoCompactMode(autoCompactMode) {
			case "", t4.AutoCompactOff, t4.AutoCompactTime, t4.AutoCompactRevision:
			default:
				return fmt.Errorf("--auto-compact-mode must be one of \"off\", \"time\", or \"revision\", got %q", autoCompactMode)
			}
			if autoCompactRetention < 0 || autoCompactInterval < 0 || autoCompactSampleInt < 0 {
				return fmt.Errorf("auto compact durations must be non-negative")
			}
			if autoCompactRevisions < 0 {
				return fmt.Errorf("--auto-compact-retain-revisions must be non-negative")
			}

			logrus.WithFields(startupLogFields(
				dataDir,
				listenAddr,
				s3,
				readConsistency,
				logLevel,
				nodeID,
				walSyncUpload,
				peerListenAddr,
				advertisePeerAddr,
				leaderWatchIntervalSec,
				followerMaxRetries,
				clientTLSCert,
				clientTLSCA,
				peerTLSCert,
				peerTLSCA,
				authEnabled,
				tokenTTLSec,
				metricsAddr,
				autoCompactMode,
				autoCompactRetention,
				autoCompactRevisions,
				autoCompactInterval,
				autoCompactSampleInt,
				branchPrefix,
				branchCheckpoint,
				enc,
			)).Info("starting t4 server")

			cfg := t4.Config{
				DataDir:                      dataDir,
				ReadConsistency:              t4.ReadConsistency(readConsistency),
				SegmentMaxSize:               segmentMaxSizeMB << 20,
				SegmentMaxAge:                time.Duration(segmentMaxAgeSec) * time.Second,
				CheckpointInterval:           time.Duration(checkpointIntervalMin) * time.Minute,
				CheckpointEntries:            checkpointEntries,
				AutoCompactMode:              t4.AutoCompactMode(autoCompactMode),
				AutoCompactRetention:         autoCompactRetention,
				AutoCompactRevisionRetention: autoCompactRevisions,
				AutoCompactInterval:          autoCompactInterval,
				AutoCompactSampleInterval:    autoCompactSampleInt,
				NodeID:                       nodeID,
				PeerListenAddr:               peerListenAddr,
				AdvertisePeerAddr:            advertisePeerAddr,
				LeaderWatchInterval:          time.Duration(leaderWatchIntervalSec) * time.Second,
				FollowerMaxRetries:           followerMaxRetries,
				FollowerWaitMode:             t4.FollowerWaitMode(followerWaitMode),
				WatchSendTimeout:             watchSendTimeout,
			}
			encCfg, err := enc.config()
			if err != nil {
				return fmt.Errorf("encryption key: %w", err)
			}
			cfg.ObjectStoreEncryption = encCfg
			if encCfg != nil {
				logrus.Info("object-store encryption at rest enabled (AES-256-GCM)")
			}

			if walSyncUpload != "" {
				b := walSyncUpload == "true"
				cfg.WALSyncUpload = &b
			}

			if peerTLSCA != "" || peerTLSCert != "" {
				serverCreds, clientCreds, err := buildPeerTLS(peerTLSCA, peerTLSCert, peerTLSKey)
				if err != nil {
					return fmt.Errorf("peer TLS: %w", err)
				}
				cfg.PeerServerTLS = serverCreds
				cfg.PeerClientTLS = clientCreds
				logrus.Info("peer mTLS enabled for node replication")
			}

			if s3.Bucket != "" {
				obj, err := object.NewS3StoreFromConfig(cmd.Context(), s3.config())
				if err != nil {
					return fmt.Errorf("init S3: %w", err)
				}
				cfg.ObjectStore = obj
				logrus.Infof("using S3 bucket %q prefix %q", s3.Bucket, s3.Prefix)
			} else {
				logrus.Warn("no S3 bucket configured — durability is local-only")
			}

			if branchPrefix != "" || branchCheckpoint != "" {
				if branchCheckpoint == "" {
					return fmt.Errorf("--branch-checkpoint is required when --branch-prefix is set")
				}
				if s3.Bucket == "" {
					return fmt.Errorf("--s3-bucket is required for a branch node")
				}
				srcCfg := s3.config()
				srcCfg.Prefix = branchPrefix
				sourceStore, err := object.NewS3StoreFromConfig(cmd.Context(), srcCfg)
				if err != nil {
					return fmt.Errorf("init branch source S3: %w", err)
				}
				cfg.BranchPoint = &t4.BranchPoint{
					SourceStore:   sourceStore,
					CheckpointKey: branchCheckpoint,
				}
				cfg.AncestorStore = sourceStore
				logrus.Infof("branch node: source prefix %q checkpoint %q",
					branchPrefix, branchCheckpoint)
			}

			node, err := t4.Open(cfg)
			if err != nil {
				return fmt.Errorf("open node: %w", err)
			}
			defer node.Close()
			logrus.WithFields(logrus.Fields{
				"listen_addr": listenAddr,
				"mode":        runMode(s3.Bucket, peerListenAddr, branchCheckpoint),
				"node_id":     resolvedNodeID(nodeID),
				"revision":    node.CurrentRevision(),
			}).Info("t4 node opened")

			// ── Observability ─────────────────────────────────────────────────
			go serveMetrics(cmd.Context(), metricsAddr, node)

			// ── Auth setup ───────────────────────────────────────────────────
			var (
				authStore *auth.Store
				tokens    *auth.TokenStore
			)
			if authEnabled {
				authStore, err = auth.NewStore(node)
				if err != nil {
					return fmt.Errorf("init auth store: %w", err)
				}
				tokens = auth.NewTokenStore(cmd.Context(), time.Duration(tokenTTLSec)*time.Second, node)
				logrus.Infof("auth enabled (token TTL %ds)", tokenTTLSec)
			}

			// ── gRPC server ──────────────────────────────────────────────────
			var grpcOpts []grpc.ServerOption

			if clientTLSCert != "" {
				creds, err := buildClientTLS(clientTLSCert, clientTLSKey, clientTLSCA)
				if err != nil {
					return fmt.Errorf("client TLS: %w", err)
				}
				grpcOpts = append(grpcOpts, grpc.Creds(creds))
				if clientTLSCA != "" {
					logrus.Info("client mTLS enabled")
				} else {
					logrus.Info("client TLS enabled (server-only)")
				}
			}

			grpcOpts = append(grpcOpts, t4etcd.NewServerOptions(authStore, tokens,
				t4etcd.WithKeepalive(t4etcd.KeepaliveOptions{
					MinTime:             grpcKeepaliveMinTime,
					PermitWithoutStream: grpcKeepalivePermitWithoutStream,
					Time:                grpcKeepaliveTime,
					Timeout:             grpcKeepaliveTimeout,
				}),
			)...)

			lis, err := net.Listen("tcp", listenAddr)
			if err != nil {
				return fmt.Errorf("listen %s: %w", listenAddr, err)
			}
			logrus.WithFields(logrus.Fields{
				"listen_addr": listenAddr,
				"client_tls":  tlsMode(clientTLSCert, clientTLSCA),
				"auth":        authEnabled,
			}).Info("etcd gRPC server listening")

			srv := grpc.NewServer(grpcOpts...)
			t4etcd.New(node, authStore, tokens).Register(srv)

			go func() {
				if err := srv.Serve(lis); err != nil {
					logrus.Errorf("gRPC serve: %v", err)
				}
			}()

			quit := make(chan os.Signal, 1)
			signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
			sig := <-quit
			logrus.WithField("signal", sig.String()).Info("shutdown signal received")
			srv.GracefulStop()
			logrus.Info("gRPC server stopped")
			return nil
		},
	}

	s3 = addS3Flags(cmd, false)
	enc = addObjectStoreEncryptionFlags(cmd)
	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/lib/t4", "directory for Pebble data and local WAL segments (env: T4_DATA_DIR)")
	cmd.Flags().StringVar(&listenAddr, "listen", "0.0.0.0:3379", "gRPC listen address (kine/etcd protocol) (env: T4_LISTEN)")
	cmd.Flags().Int64Var(&segmentMaxSizeMB, "segment-max-size-mb", 50, "WAL segment rotation size threshold in MiB (env: T4_SEGMENT_MAX_SIZE_MB)")
	cmd.Flags().IntVar(&segmentMaxAgeSec, "segment-max-age-sec", 10, "WAL segment rotation age threshold in seconds (env: T4_SEGMENT_MAX_AGE_SEC)")
	cmd.Flags().StringVar(&walSyncUpload, "wal-sync-upload", "", "upload WAL segments synchronously before ack (true/false; default true for safety, set false when local storage is durable) (env: T4_WAL_SYNC_UPLOAD)")
	cmd.Flags().IntVar(&checkpointIntervalMin, "checkpoint-interval-min", 15, "checkpoint interval in minutes (requires --s3-bucket) (env: T4_CHECKPOINT_INTERVAL_MIN)")
	cmd.Flags().Int64Var(&checkpointEntries, "checkpoint-entries", 0, "triggers a checkpoint after this many WAL entries regardless of time. 0 means disabled (requires --s3-bucket) (env: T4_CHECKPOINT_ENTRIES)")
	cmd.Flags().StringVar(&autoCompactMode, "auto-compact-mode", "", "automatic history compaction strategy: off, time, or revision. Empty infers from retention flags (env: T4_AUTO_COMPACT_MODE)")
	cmd.Flags().DurationVar(&autoCompactRetention, "auto-compact-retention", 0, "time strategy: retain revision history for this duration and compact older changes (env: T4_AUTO_COMPACT_RETENTION)")
	cmd.Flags().Int64Var(&autoCompactRevisions, "auto-compact-retain-revisions", 0, "revision strategy: retain this many recent revisions and compact older changes (env: T4_AUTO_COMPACT_RETAIN_REVISIONS)")
	cmd.Flags().DurationVar(&autoCompactInterval, "auto-compact-interval", 0, "how often to check autocompaction; 0 defaults by strategy (env: T4_AUTO_COMPACT_INTERVAL)")
	cmd.Flags().DurationVar(&autoCompactSampleInt, "auto-compact-sample-interval", 0, "time strategy: revision/time sampling interval; 0 defaults to clamp(retention/7, 1m, 24h) when enabled (env: T4_AUTO_COMPACT_SAMPLE_INTERVAL)")
	cmd.Flags().StringVar(&readConsistency, "read-consistency", "linearizable", "read consistency for follower nodes: linearizable (ReadIndex, etcd-compatible) or serializable (local, ~115x faster but may be slightly stale) (env: T4_READ_CONSISTENCY)")
	cmd.Flags().StringVar(&logLevel, "log-level", "info", "log level (trace/debug/info/warn/error) (env: T4_LOG_LEVEL)")
	// multi-node
	cmd.Flags().StringVar(&nodeID, "node-id", "", "stable unique node identifier (default: hostname) (env: T4_NODE_ID)")
	cmd.Flags().StringVar(&peerListenAddr, "peer-listen", "", "address for leader→follower WAL stream (e.g. 0.0.0.0:3380); enables multi-node mode (env: T4_PEER_LISTEN)")
	cmd.Flags().StringVar(&advertisePeerAddr, "advertise-peer", "", "address followers use to reach this node's peer stream (default: --peer-listen) (env: T4_ADVERTISE_PEER)")
	cmd.Flags().IntVar(&leaderWatchIntervalSec, "leader-watch-interval-sec", 300, "how often (seconds) the leader reads the lock to detect supersession (env: T4_LEADER_WATCH_INTERVAL_SEC)")
	cmd.Flags().IntVar(&followerMaxRetries, "follower-max-retries", 5, "consecutive stream failures before a follower attempts a leader takeover (env: T4_FOLLOWER_MAX_RETRIES)")
	cmd.Flags().StringVar(&followerWaitMode, "follower-wait-mode", "quorum", "leader wait policy for follower ACKs before commit: none, quorum, or all (env: T4_FOLLOWER_WAIT_MODE)")
	cmd.Flags().DurationVar(&watchSendTimeout, "watch-send-timeout", 30*time.Second, "cancel a watch whose server-side send queue blocks for longer than this; clients receive a 'mvcc: watcher is slow' cancellation when possible (env: T4_WATCH_SEND_TIMEOUT)")
	// peer mTLS
	cmd.Flags().StringVar(&peerTLSCA, "peer-tls-ca", "", "CA certificate file for peer mTLS (PEM) (env: T4_PEER_TLS_CA)")
	cmd.Flags().StringVar(&peerTLSCert, "peer-tls-cert", "", "node certificate file for peer mTLS (PEM) (env: T4_PEER_TLS_CERT)")
	cmd.Flags().StringVar(&peerTLSKey, "peer-tls-key", "", "node private key file for peer mTLS (PEM) (env: T4_PEER_TLS_KEY)")
	// client TLS
	cmd.Flags().StringVar(&clientTLSCert, "client-tls-cert", "", "server certificate file for client-facing TLS (PEM) (env: T4_CLIENT_TLS_CERT)")
	cmd.Flags().StringVar(&clientTLSKey, "client-tls-key", "", "server private key file for client-facing TLS (PEM) (env: T4_CLIENT_TLS_KEY)")
	cmd.Flags().StringVar(&clientTLSCA, "client-tls-ca", "", "CA certificate for client mTLS (PEM); omit for server-only TLS (env: T4_CLIENT_TLS_CA)")
	// auth
	cmd.Flags().BoolVar(&authEnabled, "auth-enabled", false, "enable etcd-compatible authentication and RBAC (env: T4_AUTH_ENABLED)")
	cmd.Flags().IntVar(&tokenTTLSec, "token-ttl", 300, "bearer token TTL in seconds (env: T4_TOKEN_TTL)")
	// observability
	cmd.Flags().StringVar(&metricsAddr, "metrics-addr", "0.0.0.0:9090", "HTTP address for /metrics, /healthz, /readyz (e.g. 0.0.0.0:9090) (env: T4_METRICS_ADDR)")
	// branch node
	cmd.Flags().StringVar(&branchPrefix, "branch-prefix", "", "S3 key prefix of the source node to branch from (uses --s3-bucket) (env: T4_BRANCH_PREFIX)")
	cmd.Flags().StringVar(&branchCheckpoint, "branch-checkpoint", "", "checkpoint index key returned by 't4 branch fork' (required with --branch-prefix) (env: T4_BRANCH_CHECKPOINT)")
	// gRPC keepalive — match etcd CLI flag names + defaults so etcd v3 clients
	// (kube-apiserver, etcdctl) don't get GOAWAY too_many_pings.
	defaultsKA := t4etcd.DefaultKeepaliveOptions()
	cmd.Flags().DurationVar(&grpcKeepaliveMinTime, "grpc-keepalive-min-time", defaultsKA.MinTime, "minimum interval the server demands between client keepalive pings (env: T4_GRPC_KEEPALIVE_MIN_TIME)")
	cmd.Flags().DurationVar(&grpcKeepaliveTime, "grpc-keepalive-interval", defaultsKA.Time, "server keepalive ping interval; how often the server pings idle connections (env: T4_GRPC_KEEPALIVE_INTERVAL)")
	cmd.Flags().DurationVar(&grpcKeepaliveTimeout, "grpc-keepalive-timeout", defaultsKA.Timeout, "server keepalive ping ack timeout before declaring the connection dead (env: T4_GRPC_KEEPALIVE_TIMEOUT)")
	cmd.Flags().BoolVar(&grpcKeepalivePermitWithoutStream, "grpc-keepalive-permit-without-stream", defaultsKA.PermitWithoutStream, "accept client pings even when no streams are open; required for etcd v3 client compatibility (env: T4_GRPC_KEEPALIVE_PERMIT_WITHOUT_STREAM)")
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{
			"data-dir":                             "T4_DATA_DIR",
			"listen":                               "T4_LISTEN",
			"segment-max-size-mb":                  "T4_SEGMENT_MAX_SIZE_MB",
			"segment-max-age-sec":                  "T4_SEGMENT_MAX_AGE_SEC",
			"wal-sync-upload":                      "T4_WAL_SYNC_UPLOAD",
			"checkpoint-interval-min":              "T4_CHECKPOINT_INTERVAL_MIN",
			"checkpoint-entries":                   "T4_CHECKPOINT_ENTRIES",
			"auto-compact-mode":                    "T4_AUTO_COMPACT_MODE",
			"auto-compact-retention":               "T4_AUTO_COMPACT_RETENTION",
			"auto-compact-retain-revisions":        "T4_AUTO_COMPACT_RETAIN_REVISIONS",
			"auto-compact-interval":                "T4_AUTO_COMPACT_INTERVAL",
			"auto-compact-sample-interval":         "T4_AUTO_COMPACT_SAMPLE_INTERVAL",
			"read-consistency":                     "T4_READ_CONSISTENCY",
			"log-level":                            "T4_LOG_LEVEL",
			"node-id":                              "T4_NODE_ID",
			"peer-listen":                          "T4_PEER_LISTEN",
			"advertise-peer":                       "T4_ADVERTISE_PEER",
			"leader-watch-interval-sec":            "T4_LEADER_WATCH_INTERVAL_SEC",
			"follower-max-retries":                 "T4_FOLLOWER_MAX_RETRIES",
			"watch-send-timeout":                   "T4_WATCH_SEND_TIMEOUT",
			"follower-wait-mode":                   "T4_FOLLOWER_WAIT_MODE",
			"peer-tls-ca":                          "T4_PEER_TLS_CA",
			"peer-tls-cert":                        "T4_PEER_TLS_CERT",
			"peer-tls-key":                         "T4_PEER_TLS_KEY",
			"client-tls-cert":                      "T4_CLIENT_TLS_CERT",
			"client-tls-key":                       "T4_CLIENT_TLS_KEY",
			"client-tls-ca":                        "T4_CLIENT_TLS_CA",
			"auth-enabled":                         "T4_AUTH_ENABLED",
			"token-ttl":                            "T4_TOKEN_TTL",
			"metrics-addr":                         "T4_METRICS_ADDR",
			"branch-prefix":                        "T4_BRANCH_PREFIX",
			"branch-checkpoint":                    "T4_BRANCH_CHECKPOINT",
			"grpc-keepalive-min-time":              "T4_GRPC_KEEPALIVE_MIN_TIME",
			"grpc-keepalive-interval":              "T4_GRPC_KEEPALIVE_INTERVAL",
			"grpc-keepalive-timeout":               "T4_GRPC_KEEPALIVE_TIMEOUT",
			"grpc-keepalive-permit-without-stream": "T4_GRPC_KEEPALIVE_PERMIT_WITHOUT_STREAM",
		})
	})

	return cmd
}

func startupLogFields(
	dataDir string,
	listenAddr string,
	s3 *s3Flags,
	readConsistency string,
	logLevel string,
	nodeID string,
	walSyncUpload string,
	peerListenAddr string,
	advertisePeerAddr string,
	leaderWatchIntervalSec int,
	followerMaxRetries int,
	clientTLSCert string,
	clientTLSCA string,
	peerTLSCert string,
	peerTLSCA string,
	authEnabled bool,
	tokenTTLSec int,
	metricsAddr string,
	autoCompactMode string,
	autoCompactRetention time.Duration,
	autoCompactRevisions int64,
	autoCompactInterval time.Duration,
	autoCompactSampleInt time.Duration,
	branchPrefix string,
	branchCheckpoint string,
	enc *objectStoreEncryptionFlags,
) logrus.Fields {
	fields := logrus.Fields{
		"data_dir":                      dataDir,
		"listen_addr":                   listenAddr,
		"log_level":                     logLevel,
		"mode":                          runMode(s3.Bucket, peerListenAddr, branchCheckpoint),
		"node_id":                       resolvedNodeID(nodeID),
		"read_consistency":              readConsistency,
		"s3_bucket":                     valueOrDisabled(s3.Bucket),
		"s3_prefix":                     valueOrNone(s3.Prefix),
		"s3_endpoint":                   valueOrDefault(s3.Endpoint, "aws-default"),
		"s3_region":                     valueOrDefault(s3.Region, "aws-default"),
		"s3_credentials":                s3CredentialsMode(s3.AccessKeyID, s3.Profile),
		"wal_sync_upload":               resolvedWALSyncUpload(walSyncUpload, peerListenAddr),
		"peer_listen_addr":              valueOrDisabled(peerListenAddr),
		"advertise_peer_addr":           resolvedAdvertisePeerAddr(peerListenAddr, advertisePeerAddr),
		"leader_watch_interval_sec":     leaderWatchIntervalSec,
		"follower_max_retries":          followerMaxRetries,
		"client_tls":                    tlsMode(clientTLSCert, clientTLSCA),
		"peer_tls":                      tlsMode(peerTLSCert, peerTLSCA),
		"auth":                          authMode(authEnabled, tokenTTLSec),
		"metrics_addr":                  valueOrDisabled(metricsAddr),
		"auto_compact_mode":             valueOrDefault(autoCompactMode, "auto"),
		"auto_compact_retention":        autoCompactRetention.String(),
		"auto_compact_retain_revisions": autoCompactRevisions,
		"auto_compact_interval":         autoCompactInterval.String(),
		"auto_compact_sample_interval":  autoCompactSampleInt.String(),
		"object_store_encryption":       enc.enabled(),
	}

	if branchCheckpoint != "" {
		fields["branch_prefix"] = valueOrNone(branchPrefix)
		fields["branch_checkpoint"] = branchCheckpoint
	}

	return fields
}

func s3CredentialsMode(accessKeyID, profile string) string {
	if accessKeyID != "" {
		return "static"
	}
	if profile != "" {
		return "profile:" + profile
	}
	return "default-chain"
}

func runMode(s3Bucket, peerListenAddr, branchCheckpoint string) string {
	switch {
	case branchCheckpoint != "":
		return "branch"
	case peerListenAddr != "":
		return "multi-node"
	case s3Bucket != "":
		return "single-node+s3"
	default:
		return "single-node"
	}
}

func resolvedNodeID(nodeID string) string {
	if nodeID != "" {
		return nodeID
	}
	if h, err := os.Hostname(); err == nil {
		return h + " (auto)"
	}
	return "node-0 (auto fallback)"
}

func resolvedWALSyncUpload(walSyncUpload, peerListenAddr string) string {
	if peerListenAddr != "" {
		return "async (multi-node quorum)"
	}
	if walSyncUpload == "" || walSyncUpload == "true" {
		return "true"
	}
	return "false"
}

func resolvedAdvertisePeerAddr(peerListenAddr, advertisePeerAddr string) string {
	if peerListenAddr == "" {
		return "disabled"
	}
	if advertisePeerAddr != "" {
		return advertisePeerAddr
	}
	return peerListenAddr + " (default)"
}

func tlsMode(certPath, caPath string) string {
	if certPath == "" {
		return "disabled"
	}
	if caPath != "" {
		return "mutual"
	}
	return "server-only"
}

func authMode(enabled bool, tokenTTLSec int) string {
	if !enabled {
		return "disabled"
	}
	return fmt.Sprintf("enabled (token_ttl=%ds)", tokenTTLSec)
}

// buildClientTLS constructs TLS credentials for the client-facing gRPC server.
// cert and key are required. ca is optional: when provided, mutual TLS is
// enforced (client cert required); when absent, only the server presents a
// certificate (encryption-only).
func buildClientTLS(cert, key, ca string) (credentials.TransportCredentials, error) {
	tlsCert, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, fmt.Errorf("load cert/key: %w", err)
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS12,
	}
	if ca != "" {
		caPEM, err := os.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("read CA: %w", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("parse CA cert")
		}
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		tlsCfg.ClientCAs = caPool
	}
	return credentials.NewTLS(tlsCfg), nil
}

// buildPeerTLS constructs mTLS credentials for both the leader's gRPC server
// and a follower's gRPC client from PEM files.
// ca is the CA cert used to verify the peer; cert/key are this node's identity.
func buildPeerTLS(ca, cert, key string) (serverCreds, clientCreds credentials.TransportCredentials, err error) {
	tlsCert, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, nil, fmt.Errorf("load cert/key: %w", err)
	}

	caPEM, err := os.ReadFile(ca)
	if err != nil {
		return nil, nil, fmt.Errorf("read CA: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, nil, fmt.Errorf("parse CA cert")
	}

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS13,
	}
	clientTLS := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS13,
	}
	return credentials.NewTLS(serverTLS), credentials.NewTLS(clientTLS), nil
}

func serveMetrics(ctx context.Context, addr string, node *t4.Node) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.Gatherer(), promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/healthz/leader", func(w http.ResponseWriter, _ *http.Request) {
		if node.IsLeader() {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "not leader", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if node.CurrentRevision() >= 0 {
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
		srv.Shutdown(shutCtx) //nolint:errcheck
	}()
	logrus.Infof("t4: metrics listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logrus.Warnf("t4: metrics server: %v", err)
	}
}

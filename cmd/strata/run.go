package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/strata-db/strata"
	strataetcd "github.com/strata-db/strata/etcd"
	"github.com/strata-db/strata/etcd/auth"
)

func runCmd() *cobra.Command {
	var (
		dataDir               string
		listenAddr            string
		s3Bucket              string
		s3Prefix              string
		s3Endpoint            string
		segmentMaxSizeMB      int64
		segmentMaxAgeSec      int
		checkpointIntervalMin int
		checkpointEntries     int64
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
		branchSourceBucket   string
		branchSourcePrefix   string
		branchSourceEndpoint string
		branchCheckpoint     string
	)

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run a strata node",
		RunE: func(cmd *cobra.Command, _ []string) error {
			lvl, err := logrus.ParseLevel(logLevel)
			if err != nil {
				return fmt.Errorf("invalid log level %q: %w", logLevel, err)
			}
			logrus.SetLevel(lvl)

			if walSyncUpload != "" && walSyncUpload != "true" && walSyncUpload != "false" {
				return fmt.Errorf("--wal-sync-upload must be \"true\" or \"false\", got %q", walSyncUpload)
			}
			switch strata.FollowerWaitMode(followerWaitMode) {
			case "", strata.FollowerWaitNone, strata.FollowerWaitQuorum, strata.FollowerWaitAll:
			default:
				return fmt.Errorf("--follower-wait-mode must be one of \"none\", \"quorum\", or \"all\", got %q", followerWaitMode)
			}

			logrus.WithFields(startupLogFields(
				dataDir,
				listenAddr,
				s3Bucket,
				s3Prefix,
				s3Endpoint,
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
				branchSourceBucket,
				branchSourcePrefix,
				branchSourceEndpoint,
				branchCheckpoint,
			)).Info("starting strata server")

			cfg := strata.Config{
				DataDir:             dataDir,
				ReadConsistency:     strata.ReadConsistency(readConsistency),
				SegmentMaxSize:      segmentMaxSizeMB << 20,
				SegmentMaxAge:       time.Duration(segmentMaxAgeSec) * time.Second,
				CheckpointInterval:  time.Duration(checkpointIntervalMin) * time.Minute,
				CheckpointEntries:   checkpointEntries,
				NodeID:              nodeID,
				PeerListenAddr:      peerListenAddr,
				AdvertisePeerAddr:   advertisePeerAddr,
				LeaderWatchInterval: time.Duration(leaderWatchIntervalSec) * time.Second,
				FollowerMaxRetries:  followerMaxRetries,
				FollowerWaitMode:    strata.FollowerWaitMode(followerWaitMode),
				MetricsAddr:         metricsAddr,
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

			if s3Bucket != "" {
				obj, err := newS3Store(cmd.Context(), s3Bucket, s3Prefix, s3Endpoint)
				if err != nil {
					return fmt.Errorf("init S3: %w", err)
				}
				cfg.ObjectStore = obj
				logrus.Infof("using S3 bucket %q prefix %q", s3Bucket, s3Prefix)
			} else {
				logrus.Warn("no S3 bucket configured — durability is local-only")
			}

			if branchSourceBucket != "" {
				if branchCheckpoint == "" {
					return fmt.Errorf("--branch-checkpoint is required when --branch-source-bucket is set")
				}
				sourceStore, err := newS3Store(cmd.Context(), branchSourceBucket, branchSourcePrefix, branchSourceEndpoint)
				if err != nil {
					return fmt.Errorf("init branch source S3: %w", err)
				}
				cfg.BranchPoint = &strata.BranchPoint{
					SourceStore:   sourceStore,
					CheckpointKey: branchCheckpoint,
				}
				cfg.AncestorStore = sourceStore
				logrus.Infof("branch node: source bucket %q prefix %q checkpoint %q",
					branchSourceBucket, branchSourcePrefix, branchCheckpoint)
			}

			node, err := strata.Open(cfg)
			if err != nil {
				return fmt.Errorf("open node: %w", err)
			}
			defer node.Close()
			logrus.WithFields(logrus.Fields{
				"listen_addr": listenAddr,
				"mode":        runMode(s3Bucket, peerListenAddr, branchSourceBucket),
				"node_id":     resolvedNodeID(nodeID),
				"revision":    node.CurrentRevision(),
			}).Info("strata node opened")

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

			grpcOpts = append(grpcOpts, strataetcd.NewServerOptions(authStore, tokens)...)

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
			strataetcd.New(node, authStore, tokens).Register(srv)

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

	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/lib/strata", "directory for Pebble data and local WAL segments")
	cmd.Flags().StringVar(&listenAddr, "listen", "0.0.0.0:3379", "gRPC listen address (kine/etcd protocol)")
	cmd.Flags().StringVar(&s3Bucket, "s3-bucket", "", "S3 bucket for WAL archive and checkpoints (optional)")
	cmd.Flags().StringVar(&s3Prefix, "s3-prefix", "", "key prefix inside the S3 bucket")
	cmd.Flags().StringVar(&s3Endpoint, "s3-endpoint", "", "custom S3 endpoint URL (for MinIO or other S3-compatible stores)")
	cmd.Flags().Int64Var(&segmentMaxSizeMB, "segment-max-size-mb", 50, "WAL segment rotation size threshold in MiB")
	cmd.Flags().IntVar(&segmentMaxAgeSec, "segment-max-age-sec", 10, "WAL segment rotation age threshold in seconds")
	cmd.Flags().StringVar(&walSyncUpload, "wal-sync-upload", "", "upload WAL segments synchronously before ack (true/false; default true for safety, set false when local storage is durable)")
	cmd.Flags().IntVar(&checkpointIntervalMin, "checkpoint-interval-min", 15, "checkpoint interval in minutes (requires --s3-bucket)")
	cmd.Flags().Int64Var(&checkpointEntries, "checkpoint-entries", 0, "triggers a checkpoint after this many WAL entries regardless of time. 0 means disabled (requires --s3-bucket)")
	cmd.Flags().StringVar(&readConsistency, "read-consistency", "linearizable", "read consistency for follower nodes: linearizable (ReadIndex, etcd-compatible) or serializable (local, ~115x faster but may be slightly stale)")
	cmd.Flags().StringVar(&logLevel, "log-level", "info", "log level (trace/debug/info/warn/error)")
	// multi-node
	cmd.Flags().StringVar(&nodeID, "node-id", "", "stable unique node identifier (default: hostname)")
	cmd.Flags().StringVar(&peerListenAddr, "peer-listen", "", "address for leader→follower WAL stream (e.g. 0.0.0.0:3380); enables multi-node mode")
	cmd.Flags().StringVar(&advertisePeerAddr, "advertise-peer", "", "address followers use to reach this node's peer stream (default: --peer-listen)")
	cmd.Flags().IntVar(&leaderWatchIntervalSec, "leader-watch-interval-sec", 300, "how often (seconds) the leader reads the lock to detect supersession")
	cmd.Flags().IntVar(&followerMaxRetries, "follower-max-retries", 5, "consecutive stream failures before a follower attempts a leader takeover")
	cmd.Flags().StringVar(&followerWaitMode, "follower-wait-mode", "quorum", "leader wait policy for follower ACKs before commit: none, quorum, or all")
	// peer mTLS
	cmd.Flags().StringVar(&peerTLSCA, "peer-tls-ca", "", "CA certificate file for peer mTLS (PEM)")
	cmd.Flags().StringVar(&peerTLSCert, "peer-tls-cert", "", "node certificate file for peer mTLS (PEM)")
	cmd.Flags().StringVar(&peerTLSKey, "peer-tls-key", "", "node private key file for peer mTLS (PEM)")
	// client TLS
	cmd.Flags().StringVar(&clientTLSCert, "client-tls-cert", "", "server certificate file for client-facing TLS (PEM)")
	cmd.Flags().StringVar(&clientTLSKey, "client-tls-key", "", "server private key file for client-facing TLS (PEM)")
	cmd.Flags().StringVar(&clientTLSCA, "client-tls-ca", "", "CA certificate for client mTLS (PEM); omit for server-only TLS")
	// auth
	cmd.Flags().BoolVar(&authEnabled, "auth-enabled", false, "enable etcd-compatible authentication and RBAC")
	cmd.Flags().IntVar(&tokenTTLSec, "token-ttl", 300, "bearer token TTL in seconds")
	// observability
	cmd.Flags().StringVar(&metricsAddr, "metrics-addr", "", "HTTP address for /metrics, /healthz, /readyz (e.g. 0.0.0.0:9090)")
	// branch node
	cmd.Flags().StringVar(&branchSourceBucket, "branch-source-bucket", "", "S3 bucket of the source node to branch from")
	cmd.Flags().StringVar(&branchSourcePrefix, "branch-source-prefix", "", "S3 key prefix of the source node")
	cmd.Flags().StringVar(&branchSourceEndpoint, "branch-source-endpoint", "", "custom S3 endpoint for the source store")
	cmd.Flags().StringVar(&branchCheckpoint, "branch-checkpoint", "", "checkpoint index key returned by 'strata branch fork' (required with --branch-source-bucket)")

	return cmd
}

func startupLogFields(
	dataDir string,
	listenAddr string,
	s3Bucket string,
	s3Prefix string,
	s3Endpoint string,
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
	branchSourceBucket string,
	branchSourcePrefix string,
	branchSourceEndpoint string,
	branchCheckpoint string,
) logrus.Fields {
	fields := logrus.Fields{
		"data_dir":                  dataDir,
		"listen_addr":               listenAddr,
		"log_level":                 logLevel,
		"mode":                      runMode(s3Bucket, peerListenAddr, branchSourceBucket),
		"node_id":                   resolvedNodeID(nodeID),
		"read_consistency":          readConsistency,
		"s3_bucket":                 valueOrDisabled(s3Bucket),
		"s3_prefix":                 valueOrNone(s3Prefix),
		"s3_endpoint":               valueOrDefault(s3Endpoint, "aws-default"),
		"wal_sync_upload":           resolvedWALSyncUpload(walSyncUpload, peerListenAddr),
		"peer_listen_addr":          valueOrDisabled(peerListenAddr),
		"advertise_peer_addr":       resolvedAdvertisePeerAddr(peerListenAddr, advertisePeerAddr),
		"leader_watch_interval_sec": leaderWatchIntervalSec,
		"follower_max_retries":      followerMaxRetries,
		"client_tls":                tlsMode(clientTLSCert, clientTLSCA),
		"peer_tls":                  tlsMode(peerTLSCert, peerTLSCA),
		"auth":                      authMode(authEnabled, tokenTTLSec),
		"metrics_addr":              valueOrDisabled(metricsAddr),
	}

	if branchSourceBucket != "" {
		fields["branch_source_bucket"] = branchSourceBucket
		fields["branch_source_prefix"] = valueOrNone(branchSourcePrefix)
		fields["branch_source_endpoint"] = valueOrDefault(branchSourceEndpoint, "aws-default")
		fields["branch_checkpoint"] = branchCheckpoint
	}

	return fields
}

func runMode(s3Bucket, peerListenAddr, branchSourceBucket string) string {
	switch {
	case branchSourceBucket != "":
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

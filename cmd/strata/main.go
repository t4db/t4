// Command strata runs a Strata node and exposes it as an etcd v3 gRPC endpoint.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/makhov/strata"
	strataetcd "github.com/makhov/strata/etcd"
	"github.com/makhov/strata/internal/checkpoint"
	"github.com/makhov/strata/pkg/object"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "strata",
		Short: "S3-durable kine-compatible datastore",
	}
	root.AddCommand(runCmd())
	root.AddCommand(branchCmd())
	return root
}

// runCmd is the default "run a node" command (previously the root command).
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
		logLevel              string
		// multi-node
		nodeID                 string
		peerListenAddr         string
		advertisePeerAddr      string
		leaderWatchIntervalSec int
		followerMaxRetries     int
		// mTLS
		peerTLSCA   string
		peerTLSCert string
		peerTLSKey  string
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

			cfg := strata.Config{
				DataDir:             dataDir,
				SegmentMaxSize:      segmentMaxSizeMB << 20,
				SegmentMaxAge:       time.Duration(segmentMaxAgeSec) * time.Second,
				CheckpointInterval:  time.Duration(checkpointIntervalMin) * time.Minute,
				CheckpointEntries:   checkpointEntries,
				NodeID:              nodeID,
				PeerListenAddr:      peerListenAddr,
				AdvertisePeerAddr:   advertisePeerAddr,
				LeaderWatchInterval: time.Duration(leaderWatchIntervalSec) * time.Second,
				FollowerMaxRetries:  followerMaxRetries,
				MetricsAddr:         metricsAddr,
			}

			if peerTLSCA != "" || peerTLSCert != "" {
				serverCreds, clientCreds, err := buildPeerTLS(peerTLSCA, peerTLSCert, peerTLSKey)
				if err != nil {
					return fmt.Errorf("peer TLS: %w", err)
				}
				cfg.PeerServerTLS = serverCreds
				cfg.PeerClientTLS = clientCreds
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
			logrus.Infof("node started (rev=%d)", node.CurrentRevision())

			lis, err := net.Listen("tcp", listenAddr)
			if err != nil {
				return fmt.Errorf("listen %s: %w", listenAddr, err)
			}
			logrus.Infof("listening on %s", listenAddr)

			srv := grpc.NewServer()
			strataetcd.New(node).Register(srv)

			go func() {
				if err := srv.Serve(lis); err != nil {
					logrus.Errorf("gRPC serve: %v", err)
				}
			}()

			quit := make(chan os.Signal, 1)
			signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
			<-quit
			logrus.Info("shutting down…")
			srv.GracefulStop()
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
	cmd.Flags().IntVar(&checkpointIntervalMin, "checkpoint-interval-min", 15, "checkpoint interval in minutes (requires --s3-bucket)")
	cmd.Flags().Int64Var(&checkpointEntries, "checkpoint-entries", 0, "triggers a checkpoint after this many WAL entries regardless of time. 0 means disabled (requires --s3-bucket)")
	cmd.Flags().StringVar(&logLevel, "log-level", "info", "log level (trace/debug/info/warn/error)")
	// multi-node
	cmd.Flags().StringVar(&nodeID, "node-id", "", "stable unique node identifier (default: hostname)")
	cmd.Flags().StringVar(&peerListenAddr, "peer-listen", "", "address for leader→follower WAL stream (e.g. 0.0.0.0:3380); enables multi-node mode")
	cmd.Flags().StringVar(&advertisePeerAddr, "advertise-peer", "", "address followers use to reach this node's peer stream (default: --peer-listen)")
	cmd.Flags().IntVar(&leaderWatchIntervalSec, "leader-watch-interval-sec", 300, "how often (seconds) the leader reads the lock to detect supersession")
	cmd.Flags().IntVar(&followerMaxRetries, "follower-max-retries", 5, "consecutive stream failures before a follower attempts a leader takeover")
	// mTLS
	cmd.Flags().StringVar(&peerTLSCA, "peer-tls-ca", "", "CA certificate file for peer mTLS (PEM)")
	cmd.Flags().StringVar(&peerTLSCert, "peer-tls-cert", "", "node certificate file for peer mTLS (PEM)")
	cmd.Flags().StringVar(&peerTLSKey, "peer-tls-key", "", "node private key file for peer mTLS (PEM)")
	// observability
	cmd.Flags().StringVar(&metricsAddr, "metrics-addr", "", "HTTP address for /metrics, /healthz, /readyz (e.g. 0.0.0.0:9090)")
	// branch node
	cmd.Flags().StringVar(&branchSourceBucket, "branch-source-bucket", "", "S3 bucket of the source node to branch from")
	cmd.Flags().StringVar(&branchSourcePrefix, "branch-source-prefix", "", "S3 key prefix of the source node")
	cmd.Flags().StringVar(&branchSourceEndpoint, "branch-source-endpoint", "", "custom S3 endpoint for the source store")
	cmd.Flags().StringVar(&branchCheckpoint, "branch-checkpoint", "", "checkpoint index key returned by 'strata branch fork' (required with --branch-source-bucket)")

	return cmd
}

// branchCmd returns the "strata branch" subcommand tree.
func branchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "branch",
		Short: "Manage database branches",
	}
	cmd.AddCommand(branchForkCmd())
	cmd.AddCommand(branchUnforkCmd())
	return cmd
}

func branchForkCmd() *cobra.Command {
	var (
		sourceBucket   string
		sourcePrefix   string
		sourceEndpoint string
		branchID       string
		checkpointKey  string
	)
	cmd := &cobra.Command{
		Use:   "fork",
		Short: "Register a new branch and print the checkpoint key to use with 'strata run --branch-checkpoint'",
		Long: `Register a new branch in the source store and print the checkpoint index key.

By default the branch is forked from the latest committed checkpoint revision.
Use --checkpoint to fork from a specific older revision instead.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			sourceStore, err := newS3Store(cmd.Context(), sourceBucket, sourcePrefix, sourceEndpoint)
			if err != nil {
				return fmt.Errorf("init source S3: %w", err)
			}
			var cpKey string
			if checkpointKey != "" {
				// Fork from a specific checkpoint rather than the latest.
				if err := checkpoint.RegisterBranch(cmd.Context(), sourceStore, branchID, checkpointKey); err != nil {
					return fmt.Errorf("register branch: %w", err)
				}
				cpKey = checkpointKey
			} else {
				cpKey, err = strata.Fork(cmd.Context(), sourceStore, branchID)
				if err != nil {
					return err
				}
			}
			fmt.Println(cpKey)
			return nil
		},
	}
	cmd.Flags().StringVar(&sourceBucket, "source-bucket", "", "S3 bucket of the source node (required)")
	cmd.Flags().StringVar(&sourcePrefix, "source-prefix", "", "S3 key prefix of the source node")
	cmd.Flags().StringVar(&sourceEndpoint, "source-endpoint", "", "custom S3 endpoint for the source store")
	cmd.Flags().StringVar(&branchID, "branch-id", "", "unique identifier for this branch (required)")
	cmd.Flags().StringVar(&checkpointKey, "checkpoint", "", "fork from this specific checkpoint key instead of the latest revision")
	cmd.MarkFlagRequired("source-bucket")
	cmd.MarkFlagRequired("branch-id")
	return cmd
}

func branchUnforkCmd() *cobra.Command {
	var (
		sourceBucket   string
		sourcePrefix   string
		sourceEndpoint string
		branchID       string
	)
	cmd := &cobra.Command{
		Use:   "unfork",
		Short: "Unregister a branch, allowing GC to reclaim its protected SSTs",
		RunE: func(cmd *cobra.Command, _ []string) error {
			sourceStore, err := newS3Store(cmd.Context(), sourceBucket, sourcePrefix, sourceEndpoint)
			if err != nil {
				return fmt.Errorf("init source S3: %w", err)
			}
			return strata.Unfork(cmd.Context(), sourceStore, branchID)
		},
	}
	cmd.Flags().StringVar(&sourceBucket, "source-bucket", "", "S3 bucket of the source node (required)")
	cmd.Flags().StringVar(&sourcePrefix, "source-prefix", "", "S3 key prefix of the source node")
	cmd.Flags().StringVar(&sourceEndpoint, "source-endpoint", "", "custom S3 endpoint for the source store")
	cmd.Flags().StringVar(&branchID, "branch-id", "", "unique identifier for this branch (required)")
	cmd.MarkFlagRequired("source-bucket")
	cmd.MarkFlagRequired("branch-id")
	return cmd
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

func newS3Store(ctx context.Context, bucket, prefix, endpoint string) (*object.S3Store, error) {
	optFns := []func(*config.LoadOptions) error{}
	if endpoint != "" {
		optFns = append(optFns, config.WithBaseEndpoint(endpoint))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, err
	}
	var clientOpts []func(*s3.Options)
	if endpoint != "" {
		// Path-style is required for MinIO and many S3-compatible stores.
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}
	client := s3.NewFromConfig(awsCfg, clientOpts...)
	return object.NewS3Store(client, bucket, prefix), nil
}

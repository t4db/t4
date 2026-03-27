// Command strata runs a single-node Strata datastore and exposes it as a kine
// endpoint that Kubernetes (via kine) can connect to directly.
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
	kserver "github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/makhov/strata"
	"github.com/makhov/strata/internal/object"
	kinebackend "github.com/makhov/strata/kine"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	var (
		dataDir               string
		listenAddr            string
		s3Bucket              string
		s3Prefix              string
		s3Endpoint            string
		segmentMaxSizeMB      int64
		segmentMaxAgeSec      int
		checkpointIntervalMin int
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
	)

	cmd := &cobra.Command{
		Use:   "strata",
		Short: "S3-durable kine-compatible datastore",
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

			backend := kinebackend.New(node)
			bridge := kserver.New(backend, "grpc", 1*time.Second, "3.5.13")

			srv := grpc.NewServer()
			bridge.Register(srv)

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
	cmd.Flags().StringVar(&listenAddr, "listen", "0.0.0.0:2379", "gRPC listen address (kine/etcd protocol)")
	cmd.Flags().StringVar(&s3Bucket, "s3-bucket", "", "S3 bucket for WAL archive and checkpoints (optional)")
	cmd.Flags().StringVar(&s3Prefix, "s3-prefix", "", "key prefix inside the S3 bucket")
	cmd.Flags().StringVar(&s3Endpoint, "s3-endpoint", "", "custom S3 endpoint URL (for MinIO or other S3-compatible stores)")
	cmd.Flags().Int64Var(&segmentMaxSizeMB, "segment-max-size-mb", 50, "WAL segment rotation size threshold in MiB")
	cmd.Flags().IntVar(&segmentMaxAgeSec, "segment-max-age-sec", 10, "WAL segment rotation age threshold in seconds")
	cmd.Flags().IntVar(&checkpointIntervalMin, "checkpoint-interval-min", 15, "checkpoint interval in minutes (requires --s3-bucket)")
	cmd.Flags().StringVar(&logLevel, "log-level", "info", "log level (trace/debug/info/warn/error)")
	// multi-node
	cmd.Flags().StringVar(&nodeID, "node-id", "", "stable unique node identifier (default: hostname)")
	cmd.Flags().StringVar(&peerListenAddr, "peer-listen", "", "address for leader→follower WAL stream (e.g. 0.0.0.0:2380); enables multi-node mode")
	cmd.Flags().StringVar(&advertisePeerAddr, "advertise-peer", "", "address followers use to reach this node's peer stream (default: --peer-listen)")
	cmd.Flags().IntVar(&leaderWatchIntervalSec, "leader-watch-interval-sec", 300, "how often (seconds) the leader reads the lock to detect supersession")
	cmd.Flags().IntVar(&followerMaxRetries, "follower-max-retries", 5, "consecutive stream failures before a follower attempts a leader takeover")
	// mTLS
	cmd.Flags().StringVar(&peerTLSCA, "peer-tls-ca", "", "CA certificate file for peer mTLS (PEM)")
	cmd.Flags().StringVar(&peerTLSCert, "peer-tls-cert", "", "node certificate file for peer mTLS (PEM)")
	cmd.Flags().StringVar(&peerTLSKey, "peer-tls-key", "", "node private key file for peer mTLS (PEM)")
	// observability
	cmd.Flags().StringVar(&metricsAddr, "metrics-addr", "", "HTTP address for /metrics, /healthz, /readyz (e.g. 0.0.0.0:9090)")

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

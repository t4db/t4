package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/soheilhy/cmux"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"github.com/t4db/t4/etcd/auth"
	"github.com/t4db/t4/internal/dbmux"
	"github.com/t4db/t4/internal/gateway"
	"github.com/t4db/t4/pkg/object"
)

func runCmd() *cobra.Command {
	var (
		s3  *objectStoreFlags
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
		historyRingSize       int
		peerBufferMB          int
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
		metricsAddr  string
		pprofEnabled bool
		// etcd JSON/HTTP gateway (shares --listen unless --gateway-addr is set)
		enableGRPCGateway    bool
		gatewayAddr          string
		gatewayCORSOrigins   string
		gatewayHostWhitelist string
		// branch node
		branchPrefix     string
		branchCheckpoint string
		// several databases
		databases []string
		// gRPC keepalive (etcd-compatible defaults)
		grpcKeepaliveMinTime             time.Duration
		grpcStreamWorkers                int
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

			tp, shutdownTracing, err := initTracing(cmd.Context())
			if err != nil {
				return fmt.Errorf("init tracing: %w", err)
			}
			defer func() { _ = shutdownTracing(context.Background()) }()

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
			if len(databases) > 0 {
				if err := validateDatabases(databases); err != nil {
					return err
				}
				if clientTLSCert == "" || clientTLSCA == "" {
					return fmt.Errorf("--db requires --client-tls-cert, --client-tls-key and --client-tls-ca: clients reach the database named by the Common Name of their certificate")
				}
				if peerListenAddr != "" {
					return fmt.Errorf("--db does not support --peer-listen yet: several databases are served by a single node")
				}
				if branchPrefix != "" || branchCheckpoint != "" {
					return fmt.Errorf("--db cannot be combined with --branch-prefix")
				}
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
				pprofEnabled,
			)).Info("starting t4 server")
			warnLargeGOMAXPROCS()

			cfg := t4.Config{
				DataDir:                      dataDir,
				ReadConsistency:              t4.ReadConsistency(readConsistency),
				HistoryRingSize:              historyRingSize,
				PeerBufferBytes:              peerBufferMB << 20,
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
				TracerProvider:               tp,
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

			var s3Store *object.S3Store
			if s3.configured() {
				obj, err := s3.newStore(cmd.Context(), "")
				if err != nil {
					return err
				}
				cfg.ObjectStore = obj
				logrus.Infof("using object store %s", s3.uri())
				// Several databases divide one store by prefix, which needs
				// the store itself rather than the Store interface.
				if len(databases) > 0 {
					var ok bool
					if s3Store, ok = obj.(*object.S3Store); !ok {
						return fmt.Errorf("--db needs an S3 object store: %s cannot be divided by prefix yet", s3.uri())
					}
				}
			} else {
				logrus.Warn("no object store configured — durability is local-only")
			}

			if branchPrefix != "" || branchCheckpoint != "" {
				if branchCheckpoint == "" {
					return fmt.Errorf("--branch-checkpoint is required when --branch-prefix is set")
				}
				if !s3.configured() {
					return fmt.Errorf("--s3-bucket or --gcs-bucket is required for a branch node")
				}
				// The source lives in the same store under a different prefix.
				sourceStore, err := s3.newStore(cmd.Context(), branchPrefix)
				if err != nil {
					return fmt.Errorf("branch source: %w", err)
				}
				cfg.BranchPoint = &t4.BranchPoint{
					SourceStore:   sourceStore,
					CheckpointKey: branchCheckpoint,
				}
				cfg.AncestorStore = sourceStore
				logrus.Infof("branch node: source prefix %q checkpoint %q",
					branchPrefix, branchCheckpoint)
			}

			dbs, metricsHandler, err := openDatabases(cfg, s3Store, databases)
			if err != nil {
				return err
			}
			defer func() {
				for _, db := range dbs {
					_ = db.node.Close()
				}
			}()
			for _, db := range dbs {
				logrus.WithFields(logrus.Fields{
					"db":          db.name,
					"listen_addr": listenAddr,
					"mode":        runMode(storeURIOrEmpty(s3), peerListenAddr, branchCheckpoint),
					"node_id":     resolvedNodeID(nodeID),
					"revision":    db.node.CurrentRevision(),
				}).Info("t4 node opened")
			}

			// ── Observability ─────────────────────────────────────────────────
			nodes := make([]*t4.Node, len(dbs))
			for i, db := range dbs {
				nodes[i] = db.node
			}
			go serveMetrics(cmd.Context(), metricsAddr, metricsHandler, nodes, pprofEnabled)

			// ── Client listener ──────────────────────────────────────────────
			var clientTLSConf *tls.Config // nil when the client port is plaintext
			if clientTLSCert != "" {
				clientTLSConf, err = buildClientTLSConfig(clientTLSCert, clientTLSKey, clientTLSCA)
				if err != nil {
					return fmt.Errorf("client TLS: %w", err)
				}
				if clientTLSCA != "" {
					logrus.Info("client mTLS enabled")
				} else {
					logrus.Info("client TLS enabled (server-only)")
				}
			}

			lis, err := net.Listen("tcp", listenAddr)
			if err != nil {
				return fmt.Errorf("listen %s: %w", listenAddr, err)
			}
			defer lis.Close()

			var creds credentials.TransportCredentials
			if len(databases) == 0 {
				dbs[0].lis = lis
				// With a gateway, TLS is terminated before gRPC sees a
				// connection: on a shared port because the protocols can only
				// be told apart above HTTP, and on a separate port so that the
				// in-process loopback the gateway dials stays plaintext. A gRPC
				// server holding credentials would try to handshake that pipe
				// too, and giving it credentials as well as decrypted
				// connections would start a second handshake inside the first.
				if clientTLSConf != nil && !enableGRPCGateway {
					creds = credentials.NewTLS(clientTLSConf)
				}
			} else {
				// One listener for every database: the mux routes each
				// connection by the Common Name of its client certificate.
				mux, err := dbmux.New(lis, clientTLSConf, databases, logrus.StandardLogger())
				if err != nil {
					return err
				}
				for _, db := range dbs {
					db.lis = mux.Listener(db.name)
				}
				creds = dbmux.Credentials()
				go func() { _ = mux.Serve() }()
			}
			logrus.WithFields(logrus.Fields{
				"listen_addr": listenAddr,
				"client_tls":  tlsMode(clientTLSCert, clientTLSCA),
				"auth":        authEnabled,
				"databases":   len(dbs),
				"gateway":     enableGRPCGateway && len(databases) == 0,
			}).Info("etcd gRPC server listening")

			// ── gRPC server per database ─────────────────────────────────────
			for _, db := range dbs {
				var (
					authStore *auth.Store
					tokens    *auth.TokenStore
				)
				if authEnabled {
					authStore, err = auth.NewStore(db.node)
					if err != nil {
						return fmt.Errorf("init auth store%s: %w", db.label(), err)
					}
					tokens = auth.NewTokenStore(cmd.Context(), time.Duration(tokenTTLSec)*time.Second, db.node)
				}

				var grpcOpts []grpc.ServerOption
				if creds != nil {
					grpcOpts = append(grpcOpts, grpc.Creds(creds))
				}
				grpcOpts = append(grpcOpts, t4etcd.NewServerOptions(authStore, tokens,
					t4etcd.WithKeepalive(t4etcd.KeepaliveOptions{
						MinTime:             grpcKeepaliveMinTime,
						PermitWithoutStream: grpcKeepalivePermitWithoutStream,
						Time:                grpcKeepaliveTime,
						Timeout:             grpcKeepaliveTimeout,
					}),
					t4etcd.WithStreamWorkers(grpcStreamWorkers),
				)...)
				grpcOpts = append(grpcOpts, t4etcd.TracingOptions(tp)...)

				db.srv = grpc.NewServer(grpcOpts...)
				t4etcd.New(db.node, authStore, tokens).Register(db.srv)
				if len(databases) > 0 {
					go func() {
						if err := db.srv.Serve(db.lis); err != nil {
							logrus.Errorf("gRPC serve%s: %v", db.label(), err)
						}
					}()
				}
			}
			if authEnabled {
				logrus.Infof("auth enabled (token TTL %ds)", tokenTTLSec)
			}

			// ── JSON/HTTP gateway ────────────────────────────────────────────
			// A single database only. dbmux routes a connection by the Common
			// Name of the client certificate that opened it, and offers h2
			// alone; a JSON or websocket request carries no such name, so with
			// --db there is nothing to route one by. The gateway is enabled by
			// default, so this says what it is doing rather than refusing to
			// start.
			var httpSrv *http.Server
			if enableGRPCGateway && len(databases) > 0 && cmd.Flags().Changed("enable-grpc-gateway") {
				logrus.Warn("t4: the JSON/HTTP gateway is not served with --db: a database is " +
					"chosen by client certificate, which a JSON request has no way to present")
			}
			if enableGRPCGateway && len(databases) == 0 {
				loopback := gateway.NewLoopback()
				defer func() { _ = loopback.Close() }()

				conn, err := loopback.ClientConn()
				if err != nil {
					return fmt.Errorf("gateway loopback: %w", err)
				}
				defer func() { _ = conn.Close() }()

				handler, err := gateway.New(cmd.Context(), conn, gateway.Config{
					CORSOrigins:   splitCommaList(gatewayCORSOrigins),
					HostWhitelist: splitCommaList(gatewayHostWhitelist),
				})
				if err != nil {
					return fmt.Errorf("gateway: %w", err)
				}
				httpSrv = &http.Server{
					Handler: handler,
					// A gateway request may be a watch that stays open for
					// hours, so only the headers get a deadline.
					ReadHeaderTimeout: 5 * time.Minute,
				}

				// The gateway reaches the gRPC server over this listener, which
				// carries no network traffic of its own.
				go serveGRPC(dbs[0].srv, loopback)

			}

			// Sharing the port is free while the port is plaintext and costs
			// gRPC roughly half its speed once it is not, because the two
			// protocols can then only be told apart above HTTP. So under TLS
			// the gateway is given a port of its own even when none was asked
			// for, derived from --listen.
			gatewayAddrAuto := false
			if httpSrv != nil && gatewayAddr == "" && clientTLSConf != nil {
				if derived, ok := derivedGatewayAddr(listenAddr); ok {
					gatewayAddr, gatewayAddrAuto = derived, true
				} else {
					logrus.Warnf("t4: cannot derive a gateway address from --listen %q, so the "+
						"JSON/HTTP gateway will share it with gRPC under TLS, which roughly doubles "+
						"gRPC request latency; set --gateway-addr to avoid it", listenAddr)
				}
			}

			var gatewayLis net.Listener
			if httpSrv != nil && gatewayAddr != "" {
				gatewayLis, err = net.Listen("tcp", gatewayAddr)
				switch {
				case err != nil && !gatewayAddrAuto:
					return fmt.Errorf("listen %s: %w", gatewayAddr, err)
				case err != nil:
					// An address nobody asked for must not keep the node from
					// starting. Sharing the port is slower but it works, so
					// say what happened and carry on.
					logrus.Warnf("t4: cannot listen on the derived gateway address %s (%v), so the "+
						"JSON/HTTP gateway will share --listen with gRPC under TLS, which roughly "+
						"doubles gRPC request latency; set --gateway-addr to a free port to avoid it",
						gatewayAddr, err)
					gatewayAddr, gatewayLis = "", nil
				default:
					logrus.WithFields(logrus.Fields{
						"gateway_addr": gatewayAddr,
						"client_tls":   tlsMode(clientTLSCert, clientTLSCA),
						"derived":      gatewayAddrAuto,
					}).Info("etcd JSON/HTTP gateway listening")
				}
			}

			if len(databases) == 0 {
				serveClientPort(dbs[0].srv, httpSrv, lis, gatewayLis, clientTLSConf)
			}

			stopped := make(chan *database, len(dbs))
			for _, db := range dbs {
				go func() {
					<-db.node.Done()
					stopped <- db
				}()
			}
			quit := make(chan os.Signal, 1)
			signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
			defer signal.Stop(quit)
			stop := func() {
				var wg sync.WaitGroup
				for _, db := range dbs {
					wg.Go(db.srv.GracefulStop)
				}
				wg.Wait()
				logrus.Info("gRPC server stopped")
			}
			select {
			case sig := <-quit:
				logrus.WithField("signal", sig.String()).Info("shutdown signal received")
				stopGateway(httpSrv)
				stop()
				return nil
			case <-cmd.Context().Done():
				stop()
				return nil
			case db := <-stopped:
				// The node cannot serve again without a restart: exit so
				// that whatever runs t4 starts it again, as a follower.
				if httpSrv != nil {
					_ = httpSrv.Close()
				}
				for _, db := range dbs {
					db.srv.Stop()
				}
				return fmt.Errorf("node%s stopped: %w", db.label(), db.node.Err())
			}
		},
	}

	s3 = addObjectStoreFlags(cmd, false)
	enc = addObjectStoreEncryptionFlags(cmd)
	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/lib/t4", "directory for Pebble data and local WAL segments (env: T4_DATA_DIR)")
	cmd.Flags().StringVar(&listenAddr, "listen", "0.0.0.0:3379", "gRPC listen address (kine/etcd protocol) (env: T4_LISTEN)")
	cmd.Flags().Int64Var(&segmentMaxSizeMB, "segment-max-size-mb", 50, "WAL segment rotation size threshold in MiB (env: T4_SEGMENT_MAX_SIZE_MB)")
	cmd.Flags().IntVar(&segmentMaxAgeSec, "segment-max-age-sec", 10, "WAL segment rotation age threshold in seconds (env: T4_SEGMENT_MAX_AGE_SEC)")
	cmd.Flags().StringVar(&walSyncUpload, "wal-sync-upload", "", "single-node mode: upload WAL segments synchronously before ack (true/false; default true for safety, set false when local storage is durable); a cluster uploads synchronously exactly while no follower is connected (env: T4_WAL_SYNC_UPLOAD)")
	cmd.Flags().IntVar(&checkpointIntervalMin, "checkpoint-interval-min", 15, "checkpoint interval in minutes (requires --s3-bucket) (env: T4_CHECKPOINT_INTERVAL_MIN)")
	cmd.Flags().Int64Var(&checkpointEntries, "checkpoint-entries", 0, "triggers a checkpoint after this many WAL entries regardless of time. 0 means disabled (requires --s3-bucket) (env: T4_CHECKPOINT_ENTRIES)")
	cmd.Flags().StringVar(&autoCompactMode, "auto-compact-mode", "", "automatic history compaction strategy: off, time, or revision. Empty infers from retention flags (env: T4_AUTO_COMPACT_MODE)")
	cmd.Flags().DurationVar(&autoCompactRetention, "auto-compact-retention", 0, "time strategy: retain revision history for this duration and compact older changes (env: T4_AUTO_COMPACT_RETENTION)")
	cmd.Flags().Int64Var(&autoCompactRevisions, "auto-compact-retain-revisions", 0, "revision strategy: retain this many recent revisions and compact older changes (env: T4_AUTO_COMPACT_RETAIN_REVISIONS)")
	cmd.Flags().DurationVar(&autoCompactInterval, "auto-compact-interval", 0, "how often to check autocompaction; 0 defaults by strategy (env: T4_AUTO_COMPACT_INTERVAL)")
	cmd.Flags().DurationVar(&autoCompactSampleInt, "auto-compact-sample-interval", 0, "time strategy: revision/time sampling interval; 0 defaults to clamp(retention/7, 1m, 24h) when enabled (env: T4_AUTO_COMPACT_SAMPLE_INTERVAL)")
	cmd.Flags().StringVar(&readConsistency, "read-consistency", "linearizable", "read consistency for follower nodes: linearizable (ReadIndex, etcd-compatible) or serializable (local, ~115x faster but may be slightly stale) (env: T4_READ_CONSISTENCY)")
	cmd.Flags().IntVar(&historyRingSize, "history-ring-size", 0, "in-memory recent-history ring capacity in revisions; covered revision-pinned reads skip the log-tail scan (0 defaults to 16384, negative disables) (env: T4_HISTORY_RING_SIZE)")
	cmd.Flags().IntVar(&peerBufferMB, "peer-buffer-mb", 0, "memory bound on the leader's follower catch-up buffer, in MiB; entries are held with their values, so the entry count alone does not bound it (0 defaults to 64, negative disables) (env: T4_PEER_BUFFER_MB)")
	cmd.Flags().StringVar(&logLevel, "log-level", "info", "log level (trace/debug/info/warn/error) (env: T4_LOG_LEVEL)")
	// multi-node
	cmd.Flags().StringVar(&nodeID, "node-id", "", "stable unique node identifier (default: hostname) (env: T4_NODE_ID)")
	cmd.Flags().StringVar(&peerListenAddr, "peer-listen", "", "address for leader→follower WAL stream (e.g. 0.0.0.0:3380); enables multi-node mode (env: T4_PEER_LISTEN)")
	cmd.Flags().StringVar(&advertisePeerAddr, "advertise-peer", "", "address followers use to reach this node's peer stream (default: --peer-listen) (env: T4_ADVERTISE_PEER)")
	cmd.Flags().IntVar(&leaderWatchIntervalSec, "leader-watch-interval-sec", 20, "how often (seconds) the leader renews its lock while it hears all followers; the lock stays valid 3x as long (min 2) (env: T4_LEADER_WATCH_INTERVAL_SEC)")
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
	cmd.Flags().BoolVar(&pprofEnabled, "pprof", false, "also serve Go runtime profiles at /debug/pprof/ on --metrics-addr; they expose process internals, so enable only where that address is not publicly reachable (env: T4_PPROF)")
	cmd.Flags().BoolVar(&enableGRPCGateway, "enable-grpc-gateway", true, "also serve the etcd v3 JSON/HTTP API on --listen, for clients that cannot speak gRPC; streaming RPCs (watch, lease keepalive) are served over websockets (env: T4_ENABLE_GRPC_GATEWAY)")
	cmd.Flags().StringVar(&gatewayAddr, "gateway-addr", "", "serve the JSON/HTTP API on this address instead of sharing --listen. Empty shares the port while it is plaintext, which is free, and takes the port below --listen once client TLS is on, where sharing would roughly double gRPC request latency (env: T4_GATEWAY_ADDR)")
	cmd.Flags().StringVar(&gatewayCORSOrigins, "gateway-cors-origins", "", "comma-separated web origins allowed to call the JSON/HTTP API, or * for all; empty sends no CORS headers, which keeps browsers out while leaving other clients unaffected (env: T4_GATEWAY_CORS_ORIGINS)")
	cmd.Flags().StringVar(&gatewayHostWhitelist, "gateway-host-whitelist", "*", "comma-separated Host header values accepted by the JSON/HTTP API over plaintext, or * for all; narrow it to guard against DNS rebinding (env: T4_GATEWAY_HOST_WHITELIST)")
	// branch node
	cmd.Flags().StringVar(&branchPrefix, "branch-prefix", "", "S3 key prefix of the source node to branch from (uses --s3-bucket) (env: T4_BRANCH_PREFIX)")
	cmd.Flags().StringVar(&branchCheckpoint, "branch-checkpoint", "", "checkpoint index key returned by 't4 branch fork' (required with --branch-prefix) (env: T4_BRANCH_CHECKPOINT)")
	// several databases
	cmd.Flags().StringSliceVar(&databases, "db", nil, "serve this database; repeat or comma-separate for several. Each is stored below dbs/<name> in --data-dir and --s3-prefix, and clients reach the one named by the Common Name of their certificate (requires client mTLS) (env: T4_DBS)")
	// gRPC keepalive — match etcd CLI flag names + defaults so etcd v3 clients
	// (kube-apiserver, etcdctl) don't get GOAWAY too_many_pings.
	defaultsKA := t4etcd.DefaultKeepaliveOptions()
	cmd.Flags().DurationVar(&grpcKeepaliveMinTime, "grpc-keepalive-min-time", defaultsKA.MinTime, "minimum interval the server demands between client keepalive pings (env: T4_GRPC_KEEPALIVE_MIN_TIME)")
	cmd.Flags().DurationVar(&grpcKeepaliveTime, "grpc-keepalive-interval", defaultsKA.Time, "server keepalive ping interval; how often the server pings idle connections (env: T4_GRPC_KEEPALIVE_INTERVAL)")
	cmd.Flags().DurationVar(&grpcKeepaliveTimeout, "grpc-keepalive-timeout", defaultsKA.Timeout, "server keepalive ping ack timeout before declaring the connection dead (env: T4_GRPC_KEEPALIVE_TIMEOUT)")
	cmd.Flags().BoolVar(&grpcKeepalivePermitWithoutStream, "grpc-keepalive-permit-without-stream", defaultsKA.PermitWithoutStream, "accept client pings even when no streams are open; required for etcd v3 client compatibility (env: T4_GRPC_KEEPALIVE_PERMIT_WITHOUT_STREAM)")
	cmd.Flags().IntVar(&grpcStreamWorkers, "grpc-stream-workers", 0, "goroutines kept to serve RPCs; 0 picks max(16, min(4*GOMAXPROCS, 64)), negative starts a goroutine per RPC (env: T4_GRPC_STREAM_WORKERS)")
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
			"history-ring-size":                    "T4_HISTORY_RING_SIZE",
			"peer-buffer-mb":                       "T4_PEER_BUFFER_MB",
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
			"pprof":                                "T4_PPROF",
			"enable-grpc-gateway":                  "T4_ENABLE_GRPC_GATEWAY",
			"gateway-addr":                         "T4_GATEWAY_ADDR",
			"gateway-cors-origins":                 "T4_GATEWAY_CORS_ORIGINS",
			"gateway-host-whitelist":               "T4_GATEWAY_HOST_WHITELIST",
			"branch-prefix":                        "T4_BRANCH_PREFIX",
			"branch-checkpoint":                    "T4_BRANCH_CHECKPOINT",
			"db":                                   "T4_DBS",
			"grpc-keepalive-min-time":              "T4_GRPC_KEEPALIVE_MIN_TIME",
			"grpc-keepalive-interval":              "T4_GRPC_KEEPALIVE_INTERVAL",
			"grpc-keepalive-timeout":               "T4_GRPC_KEEPALIVE_TIMEOUT",
			"grpc-keepalive-permit-without-stream": "T4_GRPC_KEEPALIVE_PERMIT_WITHOUT_STREAM",
			"grpc-stream-workers":                  "T4_GRPC_STREAM_WORKERS",
		})
	})

	return cmd
}

func startupLogFields(
	dataDir string,
	listenAddr string,
	s3 *objectStoreFlags,
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
	pprofEnabled bool,
) logrus.Fields {
	fields := logrus.Fields{
		"data_dir":                      dataDir,
		"listen_addr":                   listenAddr,
		"log_level":                     logLevel,
		"mode":                          runMode(storeURIOrEmpty(s3), peerListenAddr, branchCheckpoint),
		"node_id":                       resolvedNodeID(nodeID),
		"read_consistency":              readConsistency,
		"object_store":                  valueOrDisabled(storeURIOrEmpty(s3)),
		"wal_sync_upload":               resolvedWALSyncUpload(walSyncUpload, peerListenAddr),
		"peer_listen_addr":              valueOrDisabled(peerListenAddr),
		"advertise_peer_addr":           resolvedAdvertisePeerAddr(peerListenAddr, advertisePeerAddr),
		"leader_watch_interval_sec":     leaderWatchIntervalSec,
		"follower_max_retries":          followerMaxRetries,
		"client_tls":                    tlsMode(clientTLSCert, clientTLSCA),
		"peer_tls":                      tlsMode(peerTLSCert, peerTLSCA),
		"auth":                          authMode(authEnabled, tokenTTLSec),
		"metrics_addr":                  valueOrDisabled(metricsAddr),
		"pprof":                         pprofEnabled,
		"auto_compact_mode":             valueOrDefault(autoCompactMode, "auto"),
		"auto_compact_retention":        autoCompactRetention.String(),
		"auto_compact_retain_revisions": autoCompactRevisions,
		"auto_compact_interval":         autoCompactInterval.String(),
		"auto_compact_sample_interval":  autoCompactSampleInt.String(),
		"object_store_encryption":       enc.enabled(),
		"gomaxprocs":                    runtime.GOMAXPROCS(0),
	}
	for k, v := range s3.logFields() {
		fields[k] = v
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

// storeURIOrEmpty is the store's URI, or "" when none is configured. uri()
// always formats a scheme, so callers that need to distinguish "no store"
// cannot test its result for emptiness.
func storeURIOrEmpty(f *objectStoreFlags) string {
	if !f.configured() {
		return ""
	}
	return f.uri()
}

// runMode names the shape this process is running in, for the startup log.
// storeURI is empty when no object store is configured, and otherwise carries
// the scheme, so a single node reports which store backs it.
func runMode(storeURI, peerListenAddr, branchCheckpoint string) string {
	switch {
	case branchCheckpoint != "":
		return "branch"
	case peerListenAddr != "":
		return "multi-node"
	case strings.HasPrefix(storeURI, "gs://"):
		return "single-node+gcs"
	case storeURI != "":
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

// buildClientTLS constructs the TLS configuration for the client-facing gRPC server.
// cert and key are required. ca is optional: when provided, mutual TLS is
// enforced (client cert required); when absent, only the server presents a
// certificate (encryption-only).
func buildClientTLS(cert, key, ca string) (credentials.TransportCredentials, error) {
	cfg, err := buildClientTLSConfig(cert, key, ca)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(cfg), nil
}

// buildClientTLSConfig is buildClientTLS without the gRPC wrapper, for the
// cases that need the config itself: when the JSON gateway shares the client
// port, this process terminates TLS rather than handing the job to gRPC, and
// gateway.SplitTLS needs the *tls.Config to do it.
func buildClientTLSConfig(cert, key, ca string) (*tls.Config, error) {
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
	return tlsCfg, nil
}

// buildPeerTLS constructs mTLS credentials for both the leader's gRPC server
// and a follower's gRPC client from PEM files.
// ca is the CA cert used to verify the peer; cert/key are this node's identity.
// initTracing builds an OTLP TracerProvider when an OTLP endpoint is
// configured, and otherwise returns nil so tracing stays off. Returning nil
// rather than a no-op provider keeps the "not configured" case free of any
// span machinery at all.
//
// Endpoint, headers, sampling and service name are read from the standard
// OTEL_* environment variables by the exporter and resource SDK, so there are
// no t4-specific tracing flags to keep in sync with the spec.
func initTracing(ctx context.Context) (trace.TracerProvider, func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" &&
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return nil, noop, nil
	}
	exp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, noop, fmt.Errorf("OTLP trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(envSampler()),
	)
	// Also set the global provider so any library that resolves its tracer
	// through otel.GetTracerProvider() participates in the same traces.
	otel.SetTracerProvider(tp)
	logrus.WithField("endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")).
		Info("OpenTelemetry tracing enabled")
	return tp, tp.Shutdown, nil
}

// envSampler builds the sampler named by OTEL_TRACES_SAMPLER. The Go SDK —
// unlike the SDKs for some other languages — does not read these variables on
// its own, so a database that would otherwise sample every write at full rate
// has to wire them up explicitly.
//
// Defaults to parentbased_always_on, matching the SDK default, so behaviour is
// unchanged when the variable is unset.
func envSampler() sdktrace.Sampler {
	arg := func(def float64) float64 {
		v, err := strconv.ParseFloat(os.Getenv("OTEL_TRACES_SAMPLER_ARG"), 64)
		if err != nil {
			return def
		}
		return v
	}
	switch os.Getenv("OTEL_TRACES_SAMPLER") {
	case "always_on":
		return sdktrace.AlwaysSample()
	case "always_off":
		return sdktrace.NeverSample()
	case "traceidratio":
		return sdktrace.TraceIDRatioBased(arg(1))
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample())
	case "parentbased_traceidratio":
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(arg(1)))
	default: // "", "parentbased_always_on", or unrecognised
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	}
}

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

// largeGOMAXPROCS is the GOMAXPROCS above which T4 warns at startup.
const largeGOMAXPROCS = 64

// warnLargeGOMAXPROCS warns when Go would use a very large number of OS
// threads: Go derives GOMAXPROCS from the container's CPU limit, so without
// one it uses every core of the host. The garbage collector then runs a
// quarter of them as mark workers and keeps idle ones busy, which on a large
// host costs tens of cores for little work.
func warnLargeGOMAXPROCS() {
	n := runtime.GOMAXPROCS(0)
	if n <= largeGOMAXPROCS || os.Getenv("GOMAXPROCS") != "" {
		return
	}
	logrus.Warnf("t4: GOMAXPROCS is %d (no CPU limit, or a very large one); Go's garbage collector scales with it, "+
		"which can cost tens of cores on a large host. Set a container CPU limit or GOMAXPROCS (for example 8)", n)
}

// clientPortMatchTimeout bounds how long a connection may stay silent while
// the port decides which protocol it is. Without it a client that connects and
// says nothing occupies a matcher until it goes away.
const clientPortMatchTimeout = 10 * time.Second

// derivedGatewayAddr picks an address for the JSON/HTTP gateway from the
// client listen address, for the case where sharing the port would be
// expensive and the operator named no address of their own.
//
// It takes the port below the client port, on the same host. Below rather than
// above because the convention everywhere in t4's own documentation — and in
// etcd's, which t4 follows — puts the peer port one above the client port:
// 3379 and 3380, or 2379 and 2380 on etcd. One below is the adjacent port that
// convention leaves free.
//
// It reports false when --listen names nothing a port can be derived from: a
// missing or unparseable port, or one low enough that there is nothing below
// it. The caller then shares the port and says so.
func derivedGatewayAddr(listenAddr string) (string, bool) {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", false
	}
	n, err := strconv.Atoi(port)
	if err != nil || n <= 1 {
		return "", false
	}
	return net.JoinHostPort(host, strconv.Itoa(n-1)), true
}

// serveClientPort starts serving the client listener in the background.
//
// Without a gateway the gRPC server owns the listener outright, exactly as it
// did before the gateway existed. With one, the port has to carry gRPC and
// plain HTTP at once, and TLS decides how they are told apart.
//
// Plaintext connections announce their protocol in their first bytes, so cmux
// sorts them before either server sees them and gRPC keeps its own transport.
// Under TLS nothing distinguishes them per connection — gRPC and a browser
// both negotiate h2 over ALPN — so the split moves up to the request, where
// the content type is decisive, and gRPC is served through Go's HTTP/2 stack.
// That path drops the gRPC server's own stream limit, so it is set again here
// on the HTTP server; see gateway.GRPCHandler.
//
// Serving gRPC above HTTP is not free: measured on an M1 Pro over TLS, a Get
// goes from 69 to 134 microseconds at the median when the port is shared. A
// gatewayLis moves the gateway to its own port, which leaves the gRPC listener
// exactly as it is without a gateway and costs it nothing.
func serveClientPort(grpcSrv *grpc.Server, httpSrv *http.Server, lis, gatewayLis net.Listener, tlsConf *tls.Config) {
	// Separate ports: neither server has to know about the other, and gRPC
	// keeps the transport it has without a gateway.
	if httpSrv != nil && gatewayLis != nil {
		if tlsConf != nil {
			// The gRPC server carries no credentials of its own whenever a
			// gateway exists, so TLS is terminated for it here. ALPN has to
			// offer h2 and nothing else: a gRPC client requires it, and this
			// listener serves no other protocol.
			grpcTLS := tlsConf.Clone()
			grpcTLS.NextProtos = []string{"h2"}
			lis = tls.NewListener(lis, grpcTLS)
			httpSrv.TLSConfig = tlsConf
			go serveGRPC(grpcSrv, lis)
			go serveGatewayTLS(httpSrv, gatewayLis)
		} else {
			go serveGRPC(grpcSrv, lis)
			go serveGatewayHTTP(httpSrv, gatewayLis)
		}
		return
	}

	switch {
	case httpSrv == nil:
		go serveGRPC(grpcSrv, lis)

	case tlsConf == nil:
		m := cmux.New(lis)
		m.SetReadTimeout(clientPortMatchTimeout)
		// HTTP/1.1 first: the HTTP/2 matcher reads the connection preface,
		// which an HTTP/1.1 request would never send.
		httpL := m.Match(cmux.HTTP1())
		grpcL := m.Match(cmux.HTTP2())
		go serveGatewayHTTP(httpSrv, httpL)
		go serveGRPC(grpcSrv, grpcL)
		go func() {
			if err := m.Serve(); err != nil && !errors.Is(err, net.ErrClosed) {
				logrus.Errorf("client port serve: %v", err)
			}
		}()

	default:
		httpSrv.Handler = gateway.GRPCHandler(grpcSrv, httpSrv.Handler)
		httpSrv.TLSConfig = tlsConf
		// Go's HTTP/2 server caps concurrent streams per connection at its own
		// default, where the gRPC server is configured not to cap them at all.
		// Setting it keeps the two paths saying the same thing. It is not
		// known to fix an observable failure — grpc-go queues a stream the
		// server has no room for rather than refusing it, and clientv3 puts
		// every watcher sharing a context on one stream, so a client hitting
		// the default could not be constructed here — but leaving a quieter
		// limit on one of two routes to the same server invites a difference
		// that only shows up under load.
		httpSrv.HTTP2 = &http.HTTP2Config{MaxConcurrentStreams: math.MaxInt32}
		go serveGatewayTLS(httpSrv, lis)
	}
}

// stopGateway shuts the JSON/HTTP gateway down, if one is being served, giving
// it the same brief grace period as the gRPC server's GracefulStop.
func stopGateway(srv *http.Server) {
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logrus.Warnf("t4: gateway shutdown: %v", err)
	}
	logrus.Info("gateway stopped")
}

// serveGatewayTLS serves srv over TLS on lis.
//
// ServeTLS, rather than a tls.NewListener of our own, because it is what
// installs the HTTP/2 handler and the ALPN protocols that advertise it — and
// on a shared port HTTP/2 is not optional, since every browser speaks it. The
// certificate comes from srv.TLSConfig, so the file arguments stay empty.
func serveGatewayTLS(srv *http.Server, lis net.Listener) {
	if err := srv.ServeTLS(lis, "", ""); err != nil &&
		!errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
		logrus.Errorf("gateway serve: %v", err)
	}
}

func serveGRPC(srv *grpc.Server, lis net.Listener) {
	if err := srv.Serve(lis); err != nil &&
		!errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
		logrus.Errorf("gRPC serve: %v", err)
	}
}

func serveGatewayHTTP(srv *http.Server, lis net.Listener) {
	if err := srv.Serve(lis); err != nil &&
		!errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
		logrus.Warnf("t4: gateway serve: %v", err)
	}
}

// splitCommaList parses a comma-separated flag value, the shape etcd uses for
// its own --cors and --host-whitelist. An empty value yields no entries rather
// than one empty one, which matters because the two lists read emptiness in
// opposite directions (see gateway.Config).
func splitCommaList(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// serveMetrics serves metricsHandler at /metrics and health on addr. With
// several databases a health endpoint reports OK only when it holds for every
// database.
func serveMetrics(ctx context.Context, addr string, metricsHandler http.Handler, nodes []*t4.Node, pprofEnabled bool) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metricsHandler)
	if pprofEnabled {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		for _, node := range nodes {
			select {
			case <-node.Done():
				http.Error(w, "node stopped", http.StatusServiceUnavailable)
				return
			default:
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/healthz/leader", func(w http.ResponseWriter, _ *http.Request) {
		for _, node := range nodes {
			if !node.IsLeader() {
				http.Error(w, "not leader", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		for _, node := range nodes {
			if node.CurrentRevision() < 0 {
				http.Error(w, "not ready", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
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

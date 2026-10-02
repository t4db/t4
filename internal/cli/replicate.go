package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/t4db/t4/internal/replicate"
)

func replicateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "replicate",
		Short: "Replicate an etcd v3 keyspace with identical revisions",
		Long: `Replicate a T4 database into etcd (or etcd into T4) so that the target's
revisions match the source's exactly. Kubernetes can then be switched from the
source to the target without clients noticing.`,
	}
	cmd.AddCommand(replicateRunCmd())
	return cmd
}

// endpointFlags are the connection settings for one side of replication.
type endpointFlags struct {
	side      string
	Endpoints string
	CACert    string
	Cert      string
	Key       string
	User      string
	Password  string
}

func addEndpointFlags(cmd *cobra.Command, side string) *endpointFlags {
	f := &endpointFlags{side: side}
	env := "T4_REPLICATE_" + strings.ToUpper(side) + "_"
	cmd.Flags().StringVar(&f.Endpoints, side+"-endpoints", "", fmt.Sprintf("comma-separated etcd v3 endpoints of the %s (env: %sENDPOINTS)", side, env))
	cmd.Flags().StringVar(&f.CACert, side+"-cacert", "", fmt.Sprintf("CA bundle to verify the %s's TLS certificate (env: %sCACERT)", side, env))
	cmd.Flags().StringVar(&f.Cert, side+"-cert", "", fmt.Sprintf("client certificate for the %s (env: %sCERT)", side, env))
	cmd.Flags().StringVar(&f.Key, side+"-key", "", fmt.Sprintf("client key for the %s (env: %sKEY)", side, env))
	cmd.Flags().StringVar(&f.User, side+"-user", "", fmt.Sprintf("username for the %s (env: %sUSER)", side, env))
	cmd.Flags().StringVar(&f.Password, side+"-password", "", fmt.Sprintf("password for the %s (env: %sPASSWORD)", side, env))
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{
			side + "-endpoints": env + "ENDPOINTS",
			side + "-cacert":    env + "CACERT",
			side + "-cert":      env + "CERT",
			side + "-key":       env + "KEY",
			side + "-user":      env + "USER",
			side + "-password":  env + "PASSWORD",
		})
	})
	return f
}

func (f *endpointFlags) client() (*clientv3.Client, error) {
	if f.Endpoints == "" {
		return nil, fmt.Errorf("--%s-endpoints is required", f.side)
	}
	cfg := clientv3.Config{
		Endpoints:   strings.Split(f.Endpoints, ","),
		DialTimeout: 10 * time.Second,
		Username:    f.User,
		Password:    f.Password,
	}
	if f.CACert != "" || f.Cert != "" || f.Key != "" {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if f.CACert != "" {
			pem, err := os.ReadFile(f.CACert)
			if err != nil {
				return nil, fmt.Errorf("--%s-cacert: %w", f.side, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("--%s-cacert: no certificates in %s", f.side, f.CACert)
			}
			tlsCfg.RootCAs = pool
		}
		if f.Cert != "" || f.Key != "" {
			cert, err := tls.LoadX509KeyPair(f.Cert, f.Key)
			if err != nil {
				return nil, fmt.Errorf("--%s-cert/--%s-key: %w", f.side, f.side, err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		cfg.TLS = tlsCfg
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", f.side, err)
	}
	return cli, nil
}

func replicateRunCmd() *cobra.Command {
	var (
		statePrefix       string
		leaseTTLMargin    time.Duration
		reconcileInterval time.Duration
		metricsAddr       string
		logLevel          string
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Continuously replicate the source into the target",
		Long: `Continuously replicate the source into the target, applying each source
revision as one target transaction so that both are at identical revisions.

The target must be empty (a fresh etcd cluster or T4 database) or have been
replicated into before; its replication cursor lives under --state-prefix.
The source must spend revisions like etcd does: a T4 database created with the
meta keyspace, or etcd. Only the replicator may write to the target.

Replication stops with an error instead of diverging: if the target changes
behind its back, the source compacts history the target still needs, or a
source revision exceeds the target's transaction limits (raise etcd's
--max-txn-ops and --max-request-bytes).`,
		SilenceUsage: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			lvl, err := logrus.ParseLevel(logLevel)
			if err != nil {
				return fmt.Errorf("invalid log level %q: %w", logLevel, err)
			}
			logrus.SetLevel(lvl)
			return nil
		},
	}
	source := addEndpointFlags(cmd, "source")
	target := addEndpointFlags(cmd, "target")
	cmd.Flags().StringVar(&statePrefix, "state-prefix", replicate.DefaultStatePrefix, "target key prefix for replication state; source keys under it are not replicated (env: T4_REPLICATE_STATE_PREFIX)")
	cmd.Flags().DurationVar(&leaseTTLMargin, "lease-ttl-margin", 10*time.Minute, "added to lease TTLs on the target so it never expires a lease before the source; must exceed the longest replicator downtime (env: T4_REPLICATE_LEASE_TTL_MARGIN)")
	cmd.Flags().DurationVar(&reconcileInterval, "lease-reconcile-interval", 5*time.Second, "how often target leases are kept alive and leases gone from the source are revoked (env: T4_REPLICATE_LEASE_RECONCILE_INTERVAL)")
	cmd.Flags().StringVar(&metricsAddr, "metrics-addr", "0.0.0.0:9091", "HTTP address for /metrics and /healthz; empty disables (env: T4_REPLICATE_METRICS_ADDR)")
	cmd.Flags().StringVar(&logLevel, "log-level", "info", "log level (trace/debug/info/warn/error) (env: T4_LOG_LEVEL)")
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{
			"state-prefix":             "T4_REPLICATE_STATE_PREFIX",
			"lease-ttl-margin":         "T4_REPLICATE_LEASE_TTL_MARGIN",
			"lease-reconcile-interval": "T4_REPLICATE_LEASE_RECONCILE_INTERVAL",
			"metrics-addr":             "T4_REPLICATE_METRICS_ADDR",
			"log-level":                "T4_LOG_LEVEL",
		})
	})

	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		src, err := source.client()
		if err != nil {
			return err
		}
		defer func() { _ = src.Close() }()
		dst, err := target.client()
		if err != nil {
			return err
		}
		defer func() { _ = dst.Close() }()

		reg := prometheus.NewRegistry()
		metrics := replicate.NewMetrics(reg)
		if metricsAddr != "" {
			go serveReplicateMetrics(ctx, metricsAddr, reg)
		}
		r, err := replicate.New(replicate.Config{
			Source:            src,
			Target:            dst,
			StatePrefix:       statePrefix,
			LeaseTTLMargin:    leaseTTLMargin,
			ReconcileInterval: reconcileInterval,
			Log:               logrus.StandardLogger(),
			Metrics:           metrics,
		})
		if err != nil {
			return err
		}
		logrus.WithFields(logrus.Fields{
			"source": source.Endpoints,
			"target": target.Endpoints,
		}).Info("replicate: starting")
		err = r.Run(ctx)
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			logrus.WithField("revision", r.Applied()).Info("replicate: stopped")
			return nil
		}
		return err
	}
	return cmd
}

func serveReplicateMetrics(ctx context.Context, addr string, reg *prometheus.Registry) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logrus.WithError(err).Error("replicate: metrics server")
	}
}

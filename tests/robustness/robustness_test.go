// Package robustness runs etcd's robustness tests against T4: the traffic
// generators and validators from go.etcd.io/etcd/tests/v3/robustness, driven
// against real `t4 run` processes while nodes are killed and restarted.
//
// Validation checks that every client-observed result is linearizable, that
// watches deliver the same ordered, gap-free history on every node, that
// stale reads return the state at their revision, and that all nodes end on
// the same HashKV.
//
// It lives in its own module so that etcd's test framework does not become a
// dependency of T4 itself.
package robustness

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/robustness/client"
	"go.etcd.io/etcd/tests/v3/robustness/identity"
	"go.etcd.io/etcd/tests/v3/robustness/report"
	"go.etcd.io/etcd/tests/v3/robustness/traffic"
	"go.etcd.io/etcd/tests/v3/robustness/validate"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

var (
	waitBeforeFailpoint = time.Second
	waitAfterFailpoint  = time.Second
	waitJitter          = 200 * time.Millisecond
)

var t4Bin string

func TestMain(m *testing.M) {
	t4Bin = os.Getenv("T4_ROBUSTNESS_BIN")
	if t4Bin == "" {
		dir, err := os.MkdirTemp("", "t4-robustness-bin")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		t4Bin = filepath.Join(dir, "t4")
		build := exec.Command("go", "build", "-o", t4Bin, "./cmd/t4")
		build.Dir = filepath.Join("..", "..")
		build.Stdout, build.Stderr = os.Stdout, os.Stderr
		if err := build.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "build t4:", err)
			os.Exit(1)
		}
		code := m.Run()
		_ = os.RemoveAll(dir)
		os.Exit(code)
	}
	os.Exit(m.Run())
}

type scenario struct {
	name    string
	traffic traffic.Traffic
	profile traffic.Profile
}

// Compaction traffic is left out: the rebuilt history cannot place a
// compaction among the revisions, so stale reads could not be checked.
var scenarios = []scenario{
	{
		name:    "EtcdPutDeleteLease",
		traffic: traffic.EtcdPutDeleteLease,
		profile: traffic.Profile{KeyValue: &traffic.KeyValueMedium, Watch: &traffic.WatchDefault},
	},
	{
		name:    "Kubernetes",
		traffic: traffic.Kubernetes,
		profile: traffic.Profile{KeyValue: &traffic.KeyValueMedium, Watch: &traffic.WatchDefault},
	},
}

// TestRobustness runs every scenario under every failpoint: on a single node
// without S3 always, and on a three-node cluster when T4_ROBUSTNESS_S3 points
// at an S3-compatible store. T4_ROBUSTNESS_FAILPOINT and
// T4_ROBUSTNESS_SCENARIO narrow the run to one of each, and
// T4_ROBUSTNESS_SKIP leaves out a comma-separated list of Scenario/Failpoint
// pairs.
func TestRobustness(t *testing.T) {
	sizes := []int{1}
	s3, haveS3 := s3FromEnv()
	if haveS3 {
		if err := ensureBucket(t.Context(), s3); err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, 3)
	} else {
		t.Log("T4_ROBUSTNESS_S3 is unset: running single-node scenarios only")
	}
	onlyFailpoint := os.Getenv("T4_ROBUSTNESS_FAILPOINT")
	onlyScenario := os.Getenv("T4_ROBUSTNESS_SCENARIO")
	skip := map[string]bool{}
	for _, name := range strings.Split(os.Getenv("T4_ROBUSTNESS_SKIP"), ",") {
		if name = strings.TrimSpace(name); name != "" {
			skip[name] = true
		}
	}
	if onlyFailpoint != "" {
		if _, err := failpointByName(onlyFailpoint); err != nil {
			t.Fatal(err)
		}
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Nodes%d", size), func(t *testing.T) {
			for _, s := range scenarios {
				if onlyScenario != "" && s.name != onlyScenario {
					continue
				}
				t.Run(s.name, func(t *testing.T) {
					for _, fp := range failpoints {
						if !fp.Available(size) || (onlyFailpoint != "" && fp.Name() != onlyFailpoint) {
							continue
						}
						if skip[s.name+"/"+fp.Name()] {
							t.Logf("skipping %s/%s: T4_ROBUSTNESS_SKIP", s.name, fp.Name())
							continue
						}
						t.Run(fp.Name(), func(t *testing.T) {
							var cfg *s3Config
							if size > 1 {
								cfg = &s3
							}
							testRobustness(t, s, fp, size, cfg)
						})
					}
				})
			}
		})
	}
}

func testRobustness(t *testing.T, s scenario, fp failpoint, size int, s3 *s3Config) {
	lg := zaptest.NewLogger(t, zaptest.Level(zap.InfoLevel))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	c := newCluster(t, t4Bin, size, s3)
	if err := c.start(ctx, t4Bin); err != nil {
		t.Fatal(err)
	}

	r, err := report.NewTestReport(lg, resultsDir(t), c.dataDirs(), &report.TrafficDetail{ExpectUniqueRevision: s.traffic.ExpectUniqueRevision()})
	if err != nil {
		t.Fatal(err)
	}
	panicked := true
	defer func() {
		if err := r.Finalize(t.Failed(), panicked); err != nil {
			t.Error(err)
		}
	}()

	reports, history := runScenario(ctx, t, lg, s, fp, c)
	for _, n := range c.nodes {
		n.mu.Lock()
		if n.restarts > 0 {
			t.Logf("%s exited on its own and was restarted %d time(s)", n.name, n.restarts)
		}
		n.mu.Unlock()
	}
	r.SetClientReports(reports)

	persisted := persistedRequests(history, reports)
	result := validate.ValidateAndReturnVisualize(lg, validate.Config{ExpectRevisionUnique: s.traffic.ExpectUniqueRevision()}, reports, persisted, 5*time.Minute)
	if err := result.Error(); err != nil {
		t.Error(err)
	} else {
		r.SetVisualizer(result.Linearization.Visualize)
	}
	if err := checkHashKV(ctx, c); err != nil {
		t.Error(err)
	}
	panicked = false
}

// runScenario runs traffic, injects the failpoint once traffic is up to speed,
// and collects the history every client observed.
func runScenario(ctx context.Context, t *testing.T, lg *zap.Logger, s scenario, fp failpoint, c *cluster) ([]report.ClientReport, []*clientv3.Event) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	baseTime := time.Now()
	ids := identity.NewIDProvider()

	var g errgroup.Group
	injected := make(chan report.FailpointInjection, 1)
	g.Go(func() error {
		defer close(injected)
		time.Sleep(jitter(waitBeforeFailpoint))
		start := time.Since(baseTime)
		lg.Info("Injecting failpoint", zap.String("failpoint", fp.Name()), zap.Duration("since-start", start))
		if err := fp.Inject(ctx, c, t4Bin); err != nil {
			cancel()
			return fmt.Errorf("inject %s: %w", fp.Name(), err)
		}
		end := time.Since(baseTime)
		lg.Info("Failpoint recovered", zap.String("failpoint", fp.Name()), zap.Duration("took", end-start))
		time.Sleep(jitter(waitAfterFailpoint))
		injected <- report.FailpointInjection{Start: start, End: end, Name: fp.Name()}
		return nil
	})

	trafficSet := client.NewSet(ids, baseTime)
	defer trafficSet.Close()
	maxRevs := []chan int64{make(chan int64, 1), make(chan int64, 1)}
	g.Go(func() error {
		defer func() {
			for _, ch := range maxRevs {
				close(ch)
			}
		}()
		reports, err := simulateTraffic(ctx, lg, c.endpoints(), s, injected, trafficSet)
		if err != nil {
			return err
		}
		maxRev := report.OperationsMaxRevision(reports)
		lg.Info("Finished simulating traffic", zap.Int64("max-revision", maxRev))
		for _, ch := range maxRevs {
			ch <- maxRev
		}
		return nil
	})

	watchSet := client.NewSet(ids, baseTime)
	defer watchSet.Close()
	g.Go(func() error {
		return client.CollectClusterWatchEvents(ctx, client.CollectClusterWatchEventsParam{
			Lg:              lg,
			Endpoints:       c.endpoints(),
			MaxRevisionChan: maxRevs[0],
			ClientSet:       watchSet,
		})
	})
	var history []*clientv3.Event
	g.Go(func() error {
		var err error
		history, err = collectHistory(ctx, c.endpoints(), maxRevs[1])
		return err
	})
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	return slices.Concat(trafficSet.Reports(), watchSet.Reports()), history
}

// simulateTraffic is traffic.SimulateTraffic, which takes an etcd e2e cluster,
// over a list of endpoints.
func simulateTraffic(ctx context.Context, lg *zap.Logger, endpoints []string, s scenario, injected <-chan report.FailpointInjection, clientSet *client.ClientSet) ([]report.ClientReport, error) {
	profile := s.profile
	limiter := rate.NewLimiter(rate.Limit(profile.KeyValue.MaximalQPS), profile.KeyValue.BurstableQPS)
	if err := traffic.CheckEmptyDatabaseAtStart(ctx, lg, endpoints, clientSet); err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	finish := make(chan struct{})
	keyStore := traffic.NewKeyStore(10, "key")
	storage := traffic.NewKubernetesStorage()
	startTime := time.Since(clientSet.BaseTime())
	err := traffic.SimulateKeyValueTraffic(ctx, &wg, profile.KeyValue, endpoints, clientSet, s.traffic, traffic.RunTrafficLoopParam{
		QPSLimiter:                         limiter,
		IDs:                                clientSet.IdentityProvider(),
		LeaseIDStorage:                     identity.NewLeaseIDStorage(),
		NonUniqueRequestConcurrencyLimiter: traffic.NewConcurrencyLimiter(profile.KeyValue.MaxNonUniqueRequestConcurrency),
		KeyStore:                           keyStore,
		Storage:                            storage,
		Finish:                             finish,
	})
	if err != nil {
		close(finish)
		wg.Wait()
		return nil, err
	}
	if profile.Watch != nil {
		err = traffic.SimulateWatchTraffic(ctx, &wg, profile.Watch, endpoints, clientSet, s.traffic, traffic.RunWatchLoopParam{
			Config:     *profile.Watch,
			QPSLimiter: limiter,
			KeyStore:   keyStore,
			Storage:    storage,
			Finish:     finish,
			Logger:     lg,
		})
		if err != nil {
			close(finish)
			wg.Wait()
			return nil, err
		}
	}

	var fr report.FailpointInjection
	var ok bool
	select {
	case fr, ok = <-injected:
	case <-ctx.Done():
	}
	close(finish)
	wg.Wait()
	if !ok {
		return nil, errors.New("traffic finished before the failpoint was injected")
	}
	endTime := time.Since(clientSet.BaseTime())

	// Validation requires the last operation to succeed.
	time.Sleep(time.Second)
	cc, err := clientSet.NewClient(endpoints)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	if _, err := cc.Put(ctx, "tombstone", "true"); err != nil {
		return nil, fmt.Errorf("last operation failed, validation requires it to succeed: %w", err)
	}
	reports := clientSet.Reports()

	before := traffic.CalculateStats(reports, startTime, fr.Start)
	during := traffic.CalculateStats(reports, fr.Start, fr.End)
	after := traffic.CalculateStats(reports, fr.End, endTime)
	lg.Info("Traffic before failpoint", zap.Int("successes", before.Successes), zap.Int("failures", before.Failures), zap.Float64("qps", before.QPS()))
	lg.Info("Traffic during failpoint", zap.Int("successes", during.Successes), zap.Int("failures", during.Failures), zap.Float64("qps", during.QPS()))
	lg.Info("Traffic after failpoint", zap.Int("successes", after.Successes), zap.Int("failures", after.Failures), zap.Float64("qps", after.QPS()))
	if before.QPS() < profile.KeyValue.MinimalQPS {
		return nil, fmt.Errorf("traffic reached %.1f qps before the failpoint, results need at least %.1f", before.QPS(), profile.KeyValue.MinimalQPS)
	}
	return reports, nil
}

// checkHashKV checks that every node holds the same history once they have
// all caught up with the first node's revision.
func checkHashKV(ctx context.Context, c *cluster) error {
	cli, err := clientv3.New(clientv3.Config{Endpoints: c.endpoints(), DialTimeout: time.Second, Logger: zap.NewNop()})
	if err != nil {
		return err
	}
	defer cli.Close()

	first, err := cli.HashKV(ctx, c.nodes[0].clientAddr, 0)
	if err != nil {
		return fmt.Errorf("HashKV %s: %w", c.nodes[0].name, err)
	}
	rev := first.Header.Revision
	for _, n := range c.nodes[1:] {
		deadline := time.Now().Add(10 * time.Second)
		for {
			h, err := cli.HashKV(ctx, n.clientAddr, rev)
			if err == nil {
				if h.Hash != first.Hash || h.CompactRevision != first.CompactRevision {
					return fmt.Errorf("HashKV at revision %d: %s has %d (compact %d), %s has %d (compact %d)",
						rev, c.nodes[0].name, first.Hash, first.CompactRevision, n.name, h.Hash, h.CompactRevision)
				}
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("HashKV %s at revision %d: %w", n.name, rev, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil
}

func jitter(d time.Duration) time.Duration {
	return d - waitJitter + time.Duration(rand.Int63n(int64(2*waitJitter)))
}

// resultsDir is where a failed run's report goes: RESULTS_DIR, or the
// system temp dir.
func resultsDir(t *testing.T) string {
	base := os.Getenv("RESULTS_DIR")
	if base == "" {
		base = os.TempDir()
	}
	dir, err := filepath.Abs(filepath.Join(base, strings.ReplaceAll(t.Name(), "/", "_"), fmt.Sprint(time.Now().UnixNano())))
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

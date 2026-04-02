// stratabench is a standalone load-generator that speaks the etcd v3 protocol.
// It works against any etcd-compatible endpoint, including Strata, making it
// suitable for honest side-by-side comparisons.
//
// Usage:
//
//	stratabench [flags]                 # run a workload
//	stratabench compare [--results f]   # print comparison table from a JSONL file
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ── flags ────────────────────────────────────────────────────────────────────

var (
	fEndpoints   = flag.String("endpoints", "localhost:2379", "comma-separated etcd v3 endpoints")
	fClients     = flag.Int("clients", 1, "concurrent clients (ignored for seq-* workloads)")
	fTotal       = flag.Int("total", 50000, "total operations to issue")
	fWorkload    = flag.String("workload", "seq-put", "seq-put|par-put|seq-get|par-get|mixed|watch")
	fKeySize     = flag.Int("key-size", 64, "target key length in bytes (including prefix)")
	fValSize     = flag.Int("val-size", 256, "value size in bytes")
	fOutput      = flag.String("output", "text", "output format: text|jsonl")
	fPrefix      = flag.String("prefix", "/bench/", "key prefix")
	fScenario    = flag.String("scenario", "", "scenario label written into jsonl output")
	fSystem      = flag.String("system", "", "system label written into jsonl output (e.g. strata, etcd)")
	fDialTimeout = flag.Duration("dial-timeout", 10*time.Second, "connection timeout")
)

// ── types ────────────────────────────────────────────────────────────────────

// Result holds throughput and latency statistics for one benchmark run.
type Result struct {
	Scenario  string  `json:"scenario,omitempty"`
	System    string  `json:"system,omitempty"`
	Workload  string  `json:"workload"`
	Endpoints string  `json:"endpoints"`
	Clients   int     `json:"clients"`
	Total     int     `json:"total"`
	Errors    int     `json:"errors"`
	ElapsedMs int64   `json:"elapsed_ms"`
	OpsPerSec float64 `json:"ops_per_sec"`
	P50Us     int64   `json:"p50_us"`
	P90Us     int64   `json:"p90_us"`
	P99Us     int64   `json:"p99_us"`
	P999Us    int64   `json:"p999_us"`
}

// ── entry point ───────────────────────────────────────────────────────────────

// newClient creates a fresh etcd v3 client for the configured endpoints.
// Each concurrent worker should call newClient so it gets its own connection,
// avoiding gRPC stream-level head-of-line blocking when many goroutines share
// a single underlying HTTP/2 connection.
func newClient() (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(*fEndpoints, ","),
		DialTimeout: *fDialTimeout,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "compare" {
		runCompare(os.Args[2:])
		return
	}

	flag.Parse()

	// Single client for sequential workloads and as a convenience handle for
	// operations that don't need per-worker isolation.
	cli, err := newClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to %s: %v\n", *fEndpoints, err)
		os.Exit(1)
	}
	defer cli.Close()

	val := strings.Repeat("x", *fValSize)

	var (
		latencies []int64
		errors    int
		elapsed   time.Duration
	)

	switch *fWorkload {
	case "seq-put":
		latencies, errors, elapsed = runSeqPut(cli, val)
	case "par-put":
		latencies, errors, elapsed = runParPut(val, *fClients)
	case "seq-get":
		populate(val, *fTotal)
		latencies, errors, elapsed = runSeqGet(cli, *fTotal)
	case "par-get":
		populate(val, *fTotal)
		latencies, errors, elapsed = runParGet(*fTotal, *fClients)
	case "mixed":
		populate(val, *fTotal)
		latencies, errors, elapsed = runMixed(val, *fTotal, *fClients)
	case "watch":
		latencies, errors, elapsed = runWatch(cli, val, *fTotal)
	default:
		fmt.Fprintf(os.Stderr, "unknown workload %q; choose: seq-put par-put seq-get par-get mixed watch\n", *fWorkload)
		os.Exit(1)
	}

	r := computeResult(latencies, errors, elapsed)
	r.Workload = *fWorkload
	r.Endpoints = *fEndpoints
	r.Clients = *fClients
	r.Scenario = *fScenario
	r.System = *fSystem

	switch *fOutput {
	case "jsonl":
		if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
			fmt.Fprintf(os.Stderr, "encode: %v\n", err)
			os.Exit(1)
		}
	default:
		printText(r)
	}
}

// ── key / value helpers ───────────────────────────────────────────────────────

// makeKey returns a zero-padded key of approximately fKeySize bytes.
func makeKey(i int64) string {
	p := *fPrefix
	s := fmt.Sprintf("%d", i)
	pad := *fKeySize - len(p) - len(s)
	if pad <= 0 {
		return p + s
	}
	return p + strings.Repeat("0", pad) + s
}

// populate writes n keys in parallel to pre-seed the keyspace for get workloads.
// It is not measured. Each worker gets its own client to avoid head-of-line
// blocking on a shared HTTP/2 connection under high concurrency.
func populate(val string, n int) {
	ctx := context.Background()
	var counter atomic.Int64
	var wg sync.WaitGroup
	const workers = 64
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			cli, err := newClient()
			if err != nil {
				fmt.Fprintf(os.Stderr, "populate: connect: %v\n", err)
				return
			}
			defer cli.Close()
			for {
				i := counter.Add(1) - 1
				if i >= int64(n) {
					return
				}
				cli.Put(ctx, makeKey(i), val) //nolint:errcheck
			}
		}()
	}
	wg.Wait()
}

// ── workloads ─────────────────────────────────────────────────────────────────

func runSeqPut(cli *clientv3.Client, val string) ([]int64, int, time.Duration) {
	ctx := context.Background()
	lat := make([]int64, 0, *fTotal)
	var errs int
	start := time.Now()
	for i := 0; i < *fTotal; i++ {
		t0 := time.Now()
		if _, err := cli.Put(ctx, makeKey(int64(i)), val); err != nil {
			errs++
			continue
		}
		lat = append(lat, time.Since(t0).Microseconds())
	}
	return lat, errs, time.Since(start)
}

func runParPut(val string, clients int) ([]int64, int, time.Duration) {
	ctx := context.Background()
	var counter atomic.Int64
	var errCount atomic.Int64
	shards := make([][]int64, clients)
	for i := range shards {
		shards[i] = make([]int64, 0, *fTotal/clients+1)
	}
	var wg sync.WaitGroup
	wg.Add(clients)
	start := time.Now()
	for w := 0; w < clients; w++ {
		w := w
		go func() {
			defer wg.Done()
			cli, err := newClient()
			if err != nil {
				fmt.Fprintf(os.Stderr, "worker %d: connect: %v\n", w, err)
				errCount.Add(int64(*fTotal / clients))
				return
			}
			defer cli.Close()
			for {
				i := counter.Add(1) - 1
				if i >= int64(*fTotal) {
					return
				}
				t0 := time.Now()
				if _, err := cli.Put(ctx, makeKey(i), val); err != nil {
					errCount.Add(1)
					continue
				}
				shards[w] = append(shards[w], time.Since(t0).Microseconds())
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	var lat []int64
	for _, s := range shards {
		lat = append(lat, s...)
	}
	return lat, int(errCount.Load()), elapsed
}

func runSeqGet(cli *clientv3.Client, n int) ([]int64, int, time.Duration) {
	ctx := context.Background()
	lat := make([]int64, 0, *fTotal)
	var errs int
	start := time.Now()
	for i := 0; i < *fTotal; i++ {
		t0 := time.Now()
		if _, err := cli.Get(ctx, makeKey(int64(i%n))); err != nil {
			errs++
			continue
		}
		lat = append(lat, time.Since(t0).Microseconds())
	}
	return lat, errs, time.Since(start)
}

func runParGet(n, clients int) ([]int64, int, time.Duration) {
	ctx := context.Background()
	var counter atomic.Int64
	var errCount atomic.Int64
	shards := make([][]int64, clients)
	for i := range shards {
		shards[i] = make([]int64, 0, *fTotal/clients+1)
	}
	var wg sync.WaitGroup
	wg.Add(clients)
	start := time.Now()
	for w := 0; w < clients; w++ {
		w := w
		go func() {
			defer wg.Done()
			cli, err := newClient()
			if err != nil {
				fmt.Fprintf(os.Stderr, "worker %d: connect: %v\n", w, err)
				errCount.Add(int64(*fTotal / clients))
				return
			}
			defer cli.Close()
			for {
				i := counter.Add(1) - 1
				if i >= int64(*fTotal) {
					return
				}
				t0 := time.Now()
				if _, err := cli.Get(ctx, makeKey(i%int64(n))); err != nil {
					errCount.Add(1)
					continue
				}
				shards[w] = append(shards[w], time.Since(t0).Microseconds())
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	var lat []int64
	for _, s := range shards {
		lat = append(lat, s...)
	}
	return lat, int(errCount.Load()), elapsed
}

// runMixed issues writes and reads concurrently: half the goroutines write
// unique keys, the other half read random keys from the pre-seeded set.
func runMixed(val string, n, clients int) ([]int64, int, time.Duration) {
	ctx := context.Background()
	writers := clients / 2
	if writers == 0 {
		writers = 1
	}
	readers := clients - writers
	if readers == 0 {
		readers = 1
	}
	opsEach := *fTotal / (writers + readers)

	var writeIdx atomic.Int64
	var errCount atomic.Int64
	shards := make([][]int64, writers+readers)
	for i := range shards {
		shards[i] = make([]int64, 0, opsEach+1)
	}

	var wg sync.WaitGroup
	start := time.Now()

	// writer goroutines — unique keys beyond the pre-seeded range
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		w := w
		go func() {
			defer wg.Done()
			cli, err := newClient()
			if err != nil {
				fmt.Fprintf(os.Stderr, "writer %d: connect: %v\n", w, err)
				errCount.Add(int64(opsEach))
				return
			}
			defer cli.Close()
			for j := 0; j < opsEach; j++ {
				i := writeIdx.Add(1) - 1
				t0 := time.Now()
				if _, err := cli.Put(ctx, makeKey(int64(n)+i), val); err != nil {
					errCount.Add(1)
					continue
				}
				shards[w] = append(shards[w], time.Since(t0).Microseconds())
			}
		}()
	}

	// reader goroutines — random keys from the pre-seeded set
	wg.Add(readers)
	for w := 0; w < readers; w++ {
		w := w
		rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(w)))
		go func() {
			defer wg.Done()
			cli, err := newClient()
			if err != nil {
				fmt.Fprintf(os.Stderr, "reader %d: connect: %v\n", w, err)
				errCount.Add(int64(opsEach))
				return
			}
			defer cli.Close()
			for j := 0; j < opsEach; j++ {
				i := rng.Int63n(int64(n))
				t0 := time.Now()
				if _, err := cli.Get(ctx, makeKey(i)); err != nil {
					errCount.Add(1)
					continue
				}
				shards[writers+w] = append(shards[writers+w], time.Since(t0).Microseconds())
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	var lat []int64
	for _, s := range shards {
		lat = append(lat, s...)
	}
	return lat, int(errCount.Load()), elapsed
}

// runWatch measures the latency from Put returning to the corresponding watch
// event arriving on the client stream. It is sequential by design.
func runWatch(cli *clientv3.Client, val string, n int) ([]int64, int, time.Duration) {
	watchPrefix := *fPrefix + "watch/"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchCh := cli.Watch(ctx, watchPrefix, clientv3.WithPrefix())

	lat := make([]int64, 0, n)
	var errs int
	start := time.Now()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%s%08d", watchPrefix, i)
		t0 := time.Now()
		if _, err := cli.Put(context.Background(), k, val); err != nil {
			errs++
			continue
		}
		select {
		case <-watchCh:
			lat = append(lat, time.Since(t0).Microseconds())
		case <-time.After(5 * time.Second):
			fmt.Fprintf(os.Stderr, "watch timeout at op %d\n", i)
			errs++
		}
	}
	return lat, errs, time.Since(start)
}

// ── statistics ────────────────────────────────────────────────────────────────

func computeResult(latencies []int64, errors int, elapsed time.Duration) Result {
	r := Result{Errors: errors, ElapsedMs: elapsed.Milliseconds()}
	if len(latencies) == 0 {
		return r
	}
	sorted := make([]int64, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	pct := func(p float64) int64 {
		return sorted[int(float64(len(sorted)-1)*p)]
	}
	r.Total = len(latencies)
	r.OpsPerSec = float64(len(latencies)) / elapsed.Seconds()
	r.P50Us = pct(0.50)
	r.P90Us = pct(0.90)
	r.P99Us = pct(0.99)
	r.P999Us = pct(0.999)
	return r
}

func printText(r Result) {
	label := r.Workload
	if r.System != "" {
		label = r.System + "/" + label
	}
	if r.Scenario != "" {
		label = "[" + r.Scenario + "] " + label
	}
	fmt.Printf("%-42s  %9.0f ops/s  p50=%6dµs  p90=%6dµs  p99=%6dµs  p999=%7dµs  err=%d\n",
		label, r.OpsPerSec, r.P50Us, r.P90Us, r.P99Us, r.P999Us, r.Errors)
}

// ── compare subcommand ────────────────────────────────────────────────────────

func runCompare(args []string) {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	resultsFile := fs.String("results", "results/results.jsonl", "JSONL results file produced by run.sh")
	fs.Parse(args) //nolint:errcheck

	f, err := os.Open(*resultsFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open %s: %v\n", *resultsFile, err)
		os.Exit(1)
	}
	defer f.Close()

	var results []Result
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r Result
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			continue
		}
		results = append(results, r)
	}

	workloads := []string{"seq-put", "par-put", "seq-get", "par-get", "mixed", "watch"}
	scenarios := uniqueOrdered(results, func(r Result) string { return r.Scenario })

	for _, sc := range scenarios {
		systems := uniqueOrdered(results, func(r Result) string {
			if r.Scenario == sc {
				return r.System
			}
			return ""
		})
		// remove blank entry introduced by non-matching rows
		var cleanSystems []string
		for _, s := range systems {
			if s != "" {
				cleanSystems = append(cleanSystems, s)
			}
		}
		systems = cleanSystems

		fmt.Printf("\n┌─ Scenario: %s %s\n", sc, strings.Repeat("─", max(0, 62-len(sc))))
		hdr := fmt.Sprintf("│  %-12s  %-8s  %10s  %8s  %8s  %8s  %9s",
			"workload", "system", "ops/sec", "p50 µs", "p90 µs", "p99 µs", "p999 µs")
		fmt.Println(hdr)
		fmt.Println("│ " + strings.Repeat("─", len(hdr)-2))

		for _, wl := range workloads {
			// collect rows for this (scenario, workload)
			var rows []Result
			for _, r := range results {
				if r.Scenario == sc && r.Workload == wl {
					rows = append(rows, r)
				}
			}
			if len(rows) == 0 {
				continue
			}
			// order rows by system list so strata vs etcd is consistent
			ordered := make([]Result, 0, len(systems))
			for _, sys := range systems {
				for _, r := range rows {
					if r.System == sys {
						ordered = append(ordered, r)
					}
				}
			}
			for j, r := range ordered {
				wlLabel := ""
				if j == 0 {
					wlLabel = wl
				}
				fmt.Printf("│  %-12s  %-8s  %10.0f  %8d  %8d  %8d  %9d\n",
					wlLabel, r.System, r.OpsPerSec, r.P50Us, r.P90Us, r.P99Us, r.P999Us)
			}
		}
		fmt.Println("└" + strings.Repeat("─", len(hdr)-1))
	}
}

func uniqueOrdered(results []Result, key func(Result) string) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range results {
		v := key(r)
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

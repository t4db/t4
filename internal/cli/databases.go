package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/checkpoint"
	istore "github.com/t4db/t4/internal/store"
	"github.com/t4db/t4/pkg/object"
)

// databasesDir is the directory, in --data-dir and below --s3-prefix, that
// holds one subdirectory per database served with --db. It keeps database
// names apart from the keys of a database served without --db (wal/,
// manifest/, ...), so that no name can make two databases share keys.
const databasesDir = "dbs"

// databaseName is what a database served with --db may be called: a DNS
// label, which is safe as a path segment, an object key segment, a metric
// label and a certificate Common Name.
var databaseName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// database is one database served by t4 run.
type database struct {
	name string // empty when t4 run serves a single database without --db
	node *t4.Node
	lis  net.Listener
	srv  *grpc.Server
}

// label names the database in log and error messages.
func (db *database) label() string {
	if db.name == "" {
		return ""
	}
	return fmt.Sprintf(" (db %q)", db.name)
}

func validateDatabases(names []string) error {
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if !databaseName.MatchString(name) {
			return fmt.Errorf("--db %q: a database name must be a DNS label (lowercase letters, digits and '-', at most 63 characters)", name)
		}
		if seen[name] {
			return fmt.Errorf("--db %q is listed twice", name)
		}
		seen[name] = true
	}
	return nil
}

// openDatabases opens the node of each database and returns the handler that
// serves their metrics. Without names it opens the single database cfg
// describes, reporting on prometheus.DefaultRegisterer. With names each
// database gets its own data directory and object-store prefix below
// dbs/<name>, sharing the store's client, and reports its metrics with a "db"
// label on a registry of their own: Prometheus refuses a metric that is
// registered both with and without the label, as it would be next to a node
// that reports on the default registerer.
//
// store may be nil, which serves every database from local storage alone.
func openDatabases(ctx context.Context, cfg t4.Config, store object.Store, names []string) ([]*database, http.Handler, error) {
	if len(names) == 0 {
		node, err := t4.Open(cfg)
		if err != nil {
			return nil, nil, fmt.Errorf("open node: %w", err)
		}
		return []*database{{node: node}}, promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}), nil
	}

	// Every database gets the keys below its own prefix, so the store has to
	// be divisible. Both backends are; this guards a future one that is not,
	// and it is checked before any node is opened.
	var sub object.SubStore
	if store != nil {
		s, ok := store.(object.SubStore)
		if !ok {
			return nil, nil, fmt.Errorf("--db needs an object store that can be divided by prefix, which %T cannot", store)
		}
		sub = s
	}
	if err := refuseSingleDatabase(ctx, cfg.DataDir, store); err != nil {
		return nil, nil, err
	}

	// One database's worth of write buffer and compaction workers, shared out
	// rather than taken whole by each. Built once: a copy of cfg shares the
	// slice's backing array, so appending per database could write the same
	// slot twice.
	pebbleOpts := append(slices.Clone(cfg.PebbleOptions), istore.DividedAmong(len(names)))

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	dbs := make([]*database, 0, len(names))
	for _, name := range names {
		dbCfg := cfg
		dbCfg.DataDir = filepath.Join(cfg.DataDir, databasesDir, name)
		dbCfg.PebbleOptions = pebbleOpts
		if sub != nil {
			dbCfg.ObjectStore = sub.Sub(path.Join(databasesDir, name))
		}
		dbCfg.MetricsRegisterer = prometheus.WrapRegistererWith(prometheus.Labels{"db": name}, reg)
		node, err := t4.Open(dbCfg)
		if err != nil {
			for _, db := range dbs {
				_ = db.node.Close()
			}
			return nil, nil, fmt.Errorf("open database %q: %w", name, err)
		}
		dbs = append(dbs, &database{name: name, node: node})
	}
	return dbs, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), nil
}

// refuseSingleDatabase fails when dataDir or store holds the database of a
// t4 run without --db. --db serves databases below dbs/ and would leave that
// one unserved, so its clients would find an empty database where their data
// was.
func refuseSingleDatabase(ctx context.Context, dataDir string, store object.Store) error {
	for _, dir := range []string{"db", "wal"} {
		p := filepath.Join(dataDir, dir)
		if _, err := os.Stat(p); err == nil {
			return fmt.Errorf("--db: %s holds a database served without --db, which --db would leave unserved; move it below %s/<name>/ or run without --db", p, filepath.Join(dataDir, databasesDir))
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("--db: %w", err)
		}
	}
	if store == nil {
		return nil
	}
	for _, prefix := range []string{path.Dir(checkpoint.ManifestKey) + "/", "wal/"} {
		keys, err := store.List(ctx, prefix)
		if err != nil {
			return fmt.Errorf("--db: look for a database served without --db: %w", err)
		}
		if len(keys) > 0 {
			return fmt.Errorf("--db: the object store holds a database served without --db (%s), which --db would leave unserved; move it below %s/<name>/ or run without --db", keys[0], databasesDir)
		}
	}
	return nil
}

package cli

import (
	"fmt"
	"net"
	"net/http"
	"path"
	"path/filepath"
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/t4db/t4"
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
// dbs/<name>, sharing s3Store's client, and reports its metrics with a "db"
// label on a registry of their own: Prometheus refuses a metric that is
// registered both with and without the label, as it would be next to a node
// that reports on the default registerer.
func openDatabases(cfg t4.Config, s3Store *object.S3Store, names []string) ([]*database, http.Handler, error) {
	if len(names) == 0 {
		node, err := t4.Open(cfg)
		if err != nil {
			return nil, nil, fmt.Errorf("open node: %w", err)
		}
		return []*database{{node: node}}, promhttp.Handler(), nil
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	dbs := make([]*database, 0, len(names))
	for _, name := range names {
		dbCfg := cfg
		dbCfg.DataDir = filepath.Join(cfg.DataDir, databasesDir, name)
		if s3Store != nil {
			dbCfg.ObjectStore = s3Store.Sub(path.Join(databasesDir, name))
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
	return dbs, promhttp.InstrumentMetricHandler(reg, promhttp.HandlerFor(reg, promhttp.HandlerOpts{})), nil
}

package cli

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"text/tabwriter"
	"unicode"
	"unicode/utf8"

	"github.com/spf13/cobra"

	istore "github.com/t4db/t4/internal/store"
)

func inspectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect local t4 data and metadata in read-only mode",
		Long: `Inspect a local t4 data directory without starting a server.

These commands open Pebble read-only and are intended for offline exploration,
debugging restored checkpoints, and support workflows.`,
	}
	cmd.AddCommand(inspectMetaCmd())
	cmd.AddCommand(inspectGetCmd())
	cmd.AddCommand(inspectListCmd())
	cmd.AddCommand(inspectCountCmd())
	cmd.AddCommand(inspectHistoryCmd())
	cmd.AddCommand(inspectDiffCmd())
	return cmd
}

func inspectMetaCmd() *cobra.Command {
	var (
		dataDir string
		asJSON  bool
	)
	cmd := &cobra.Command{
		Use:   "meta",
		Short: "Show local database metadata",
		RunE: func(cmd *cobra.Command, _ []string) error {
			db, err := openInspectStore(dataDir)
			if err != nil {
				return err
			}
			defer db.Close()

			totalKeys, err := db.Count("")
			if err != nil {
				return err
			}

			out := struct {
				DataDir         string `json:"data_dir"`
				PebbleDir       string `json:"pebble_dir"`
				CurrentRevision int64  `json:"current_revision"`
				CompactRevision int64  `json:"compact_revision"`
				TotalKeys       int64  `json:"total_keys"`
			}{
				DataDir:         dataDir,
				PebbleDir:       pebbleDir(dataDir),
				CurrentRevision: db.CurrentRevision(),
				CompactRevision: db.CompactRevision(),
				TotalKeys:       totalKeys,
			}

			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(out)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Data dir:          %s\n", out.DataDir)
			fmt.Fprintf(cmd.OutOrStdout(), "Pebble dir:        %s\n", out.PebbleDir)
			fmt.Fprintf(cmd.OutOrStdout(), "Current revision:  %d\n", out.CurrentRevision)
			fmt.Fprintf(cmd.OutOrStdout(), "Compact revision:  %d\n", out.CompactRevision)
			fmt.Fprintf(cmd.OutOrStdout(), "Total keys:        %d\n", out.TotalKeys)
			return nil
		},
	}
	addInspectFlags(cmd, &dataDir, &asJSON)
	return cmd
}

func inspectGetCmd() *cobra.Command {
	var (
		dataDir string
		asJSON  bool
	)
	cmd := &cobra.Command{
		Use:   "get <key>",
		Short: "Show one key and its metadata",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openInspectStore(dataDir)
			if err != nil {
				return err
			}
			defer db.Close()

			kv, err := db.Get(args[0])
			if err != nil {
				return err
			}
			if kv == nil {
				return fmt.Errorf("key %q not found", args[0])
			}

			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(newJSONKV(kv))
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Key:              %s\n", kv.Key)
			fmt.Fprintf(cmd.OutOrStdout(), "Value:            %s\n", formatValue(kv.Value))
			fmt.Fprintf(cmd.OutOrStdout(), "Revision:         %d\n", kv.Revision)
			fmt.Fprintf(cmd.OutOrStdout(), "Create revision:  %d\n", kv.CreateRevision)
			fmt.Fprintf(cmd.OutOrStdout(), "Prev revision:    %d\n", kv.PrevRevision)
			fmt.Fprintf(cmd.OutOrStdout(), "Lease:            %d\n", kv.Lease)
			return nil
		},
	}
	addInspectFlags(cmd, &dataDir, &asJSON)
	return cmd
}

func inspectListCmd() *cobra.Command {
	var (
		dataDir string
		asJSON  bool
		prefix  string
		limit   int
	)
	cmd := &cobra.Command{
		Use:   "list [prefix]",
		Short: "List live keys by prefix",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			effectivePrefix, err := resolveInspectPrefix(prefix, args)
			if err != nil {
				return err
			}

			db, err := openInspectStore(dataDir)
			if err != nil {
				return err
			}
			defer db.Close()

			kvs, err := db.List(effectivePrefix)
			if err != nil {
				return err
			}
			if limit > 0 && len(kvs) > limit {
				kvs = kvs[:limit]
			}

			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(newJSONKVs(kvs))
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "KEY\tREV\tCREATE\tPREV\tLEASE\tVALUE")
			for _, kv := range kvs {
				fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%s\n",
					kv.Key, kv.Revision, kv.CreateRevision, kv.PrevRevision, kv.Lease, formatValue(kv.Value))
			}
			w.Flush()
			fmt.Fprintf(cmd.OutOrStdout(), "\nListed %d key(s)\n", len(kvs))
			return nil
		},
	}
	addInspectFlags(cmd, &dataDir, &asJSON)
	cmd.Flags().StringVar(&prefix, "prefix", "", "only return keys with this prefix")
	cmd.Flags().IntVar(&limit, "limit", 100, "maximum number of keys to print; 0 means no limit")
	return cmd
}

func inspectCountCmd() *cobra.Command {
	var (
		dataDir string
		asJSON  bool
		prefix  string
	)
	cmd := &cobra.Command{
		Use:   "count",
		Short: "Count live keys by prefix",
		RunE: func(cmd *cobra.Command, _ []string) error {
			db, err := openInspectStore(dataDir)
			if err != nil {
				return err
			}
			defer db.Close()

			n, err := db.Count(prefix)
			if err != nil {
				return err
			}
			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
					Prefix string `json:"prefix"`
					Count  int64  `json:"count"`
				}{Prefix: prefix, Count: n})
			}
			if prefix == "" {
				fmt.Fprintf(cmd.OutOrStdout(), "%d\n", n)
				return nil
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s: %d\n", prefix, n)
			return nil
		},
	}
	addInspectFlags(cmd, &dataDir, &asJSON)
	cmd.Flags().StringVar(&prefix, "prefix", "", "only count keys with this prefix")
	return cmd
}

func inspectHistoryCmd() *cobra.Command {
	var (
		dataDir string
		asJSON  bool
		limit   int
	)
	cmd := &cobra.Command{
		Use:   "history <key>",
		Short: "Show the revision history for one key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openInspectStore(dataDir)
			if err != nil {
				return err
			}
			defer db.Close()

			events, err := db.History(args[0])
			if err != nil {
				return err
			}
			if limit > 0 && len(events) > limit {
				events = events[len(events)-limit:]
			}

			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(newJSONEvents(events))
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "REV\tTYPE\tCREATE\tPREV\tLEASE\tVALUE")
			for _, ev := range events {
				changeType := "put"
				if ev.Type == istore.EventDelete {
					changeType = "delete"
				}
				fmt.Fprintf(w, "%d\t%s\t%d\t%d\t%d\t%s\n",
					ev.KV.Revision, changeType, ev.KV.CreateRevision, ev.KV.PrevRevision, ev.KV.Lease, formatValue(ev.KV.Value))
			}
			w.Flush()
			fmt.Fprintf(cmd.OutOrStdout(), "\nListed %d event(s)\n", len(events))
			return nil
		},
	}
	addInspectFlags(cmd, &dataDir, &asJSON)
	cmd.Flags().IntVar(&limit, "limit", 100, "maximum number of history events to print; 0 means no limit")
	return cmd
}

func inspectDiffCmd() *cobra.Command {
	var (
		dataDir string
		asJSON  bool
		prefix  string
		fromRev int64
		toRev   int64
	)
	cmd := &cobra.Command{
		Use:   "diff",
		Short: "Summarize key changes between two revisions",
		RunE: func(cmd *cobra.Command, _ []string) error {
			db, err := openInspectStore(dataDir)
			if err != nil {
				return err
			}
			defer db.Close()

			if toRev == 0 {
				toRev = db.CurrentRevision()
			}
			events, err := db.Changes(prefix, fromRev, toRev)
			if err != nil {
				return err
			}
			changes := summarizeChanges(events)

			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
					Prefix  string          `json:"prefix"`
					FromRev int64           `json:"from_revision"`
					ToRev   int64           `json:"to_revision"`
					Changes []inspectChange `json:"changes"`
				}{Prefix: prefix, FromRev: fromRev, ToRev: toRev, Changes: changes})
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "KEY\tTYPE\tFIRST_REV\tLAST_REV\tOPS\tBEFORE\tAFTER")
			for _, ch := range changes {
				fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%s\t%s\n",
					ch.Key, ch.Type, ch.FirstRevision, ch.LastRevision, ch.Operations, ch.BeforeValue, ch.AfterValue)
			}
			w.Flush()
			fmt.Fprintf(cmd.OutOrStdout(), "\nChanged %d key(s)\n", len(changes))
			return nil
		},
	}
	addInspectFlags(cmd, &dataDir, &asJSON)
	cmd.Flags().StringVar(&prefix, "prefix", "", "only include keys with this prefix")
	cmd.Flags().Int64Var(&fromRev, "from-rev", 1, "start revision, inclusive")
	cmd.Flags().Int64Var(&toRev, "to-rev", 0, "end revision, inclusive; defaults to current revision")
	return cmd
}

func addInspectFlags(cmd *cobra.Command, dataDir *string, asJSON *bool) {
	cmd.Flags().StringVar(dataDir, "data-dir", "/var/lib/t4", "directory containing local Pebble data (env: T4_DATA_DIR)")
	cmd.Flags().BoolVar(asJSON, "json", false, "emit JSON output")
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{"data-dir": "T4_DATA_DIR"})
	})
}

func openInspectStore(dataDir string) (*istore.Store, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("--data-dir is required")
	}
	dir := pebbleDir(dataDir)
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("pebble database not found at %q", dir)
		}
		return nil, fmt.Errorf("stat pebble dir %q: %w", dir, err)
	}
	return istore.OpenReadOnly(dir)
}

func pebbleDir(dataDir string) string {
	return filepath.Join(dataDir, "db")
}

func formatValue(v []byte) string {
	if utf8.Valid(v) {
		s := string(v)
		printable := true
		for _, r := range s {
			if !unicode.IsPrint(r) && !unicode.IsSpace(r) {
				printable = false
				break
			}
		}
		if printable {
			return strconv.Quote(s)
		}
	}
	return "0x" + hex.EncodeToString(v)
}

// istore.KeyValue and istore.Event carry their key as a Go string and have no
// JSON tags, so encoding/json renders them by field name and rewrites any key
// that is not valid UTF-8, substituting U+FFFD for the offending bytes. The
// printed key is then not the key in the store — and a binary-key incident is
// exactly when an operator reaches for inspect.
//
// These projections keep the field names the command already emits, so
// existing readers are unaffected, and add KeyHex with the exact bytes. KeyHex
// appears only when Key cannot hold them, which is also the signal that Key
// is approximate.
type jsonKV struct {
	Key            string `json:"Key"`
	KeyHex         string `json:"KeyHex,omitempty"`
	Value          []byte `json:"Value"`
	Revision       int64  `json:"Revision"`
	CreateRevision int64  `json:"CreateRevision"`
	PrevRevision   int64  `json:"PrevRevision"`
	Version        int64  `json:"Version"`
	Lease          int64  `json:"Lease"`
}

func newJSONKV(kv *istore.KeyValue) *jsonKV {
	if kv == nil {
		return nil
	}
	return &jsonKV{
		Key:            kv.Key,
		KeyHex:         exactKeyHex(kv.Key),
		Value:          kv.Value,
		Revision:       kv.Revision,
		CreateRevision: kv.CreateRevision,
		PrevRevision:   kv.PrevRevision,
		Version:        kv.Version,
		Lease:          kv.Lease,
	}
}

func newJSONKVs(kvs []*istore.KeyValue) []*jsonKV {
	out := make([]*jsonKV, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, newJSONKV(kv))
	}
	return out
}

type jsonEvent struct {
	Type     istore.EventType `json:"Type"`
	KV       *jsonKV          `json:"KV"`
	PrevKV   *jsonKV          `json:"PrevKV"`
	Revision int64            `json:"Revision"`
}

func newJSONEvents(events []istore.Event) []jsonEvent {
	out := make([]jsonEvent, 0, len(events))
	for _, ev := range events {
		out = append(out, jsonEvent{
			Type:     ev.Type,
			KV:       newJSONKV(ev.KV),
			PrevKV:   newJSONKV(ev.PrevKV),
			Revision: ev.Revision,
		})
	}
	return out
}

// exactKeyHex returns the key's bytes as hex when a JSON string cannot carry
// them, and "" when it can.
func exactKeyHex(key string) string {
	if utf8.ValidString(key) {
		return ""
	}
	return hex.EncodeToString([]byte(key))
}

type inspectChange struct {
	Key           string `json:"key"`
	KeyHex        string `json:"key_hex,omitempty"`
	Type          string `json:"type"`
	FirstRevision int64  `json:"first_revision"`
	LastRevision  int64  `json:"last_revision"`
	Operations    int    `json:"operations"`
	BeforeValue   string `json:"before_value"`
	AfterValue    string `json:"after_value"`
}

func summarizeChanges(events []istore.Event) []inspectChange {
	type agg struct {
		change inspectChange
		before *istore.KeyValue
		after  *istore.KeyValue
		seen   bool
	}

	byKey := make(map[string]*agg, len(events))
	for _, ev := range events {
		key := ev.KV.Key
		cur, ok := byKey[key]
		if !ok {
			cur = &agg{
				change: inspectChange{
					Key:           key,
					KeyHex:        exactKeyHex(key),
					FirstRevision: ev.KV.Revision,
					LastRevision:  ev.KV.Revision,
					Operations:    0,
				},
			}
			byKey[key] = cur
		}
		cur.change.Operations++
		if ev.KV.Revision < cur.change.FirstRevision {
			cur.change.FirstRevision = ev.KV.Revision
		}
		if ev.KV.Revision > cur.change.LastRevision {
			cur.change.LastRevision = ev.KV.Revision
		}
		if !cur.seen {
			cur.before = ev.PrevKV
			cur.seen = true
		}
		if ev.Type == istore.EventDelete {
			cur.after = nil
		} else {
			cur.after = ev.KV
		}
	}

	out := make([]inspectChange, 0, len(byKey))
	for _, cur := range byKey {
		switch {
		case cur.before == nil && cur.after != nil:
			cur.change.Type = "created"
		case cur.before != nil && cur.after == nil:
			cur.change.Type = "deleted"
		default:
			cur.change.Type = "updated"
		}
		cur.change.BeforeValue = formatMaybeValue(cur.before)
		cur.change.AfterValue = formatMaybeValue(cur.after)
		out = append(out, cur.change)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func formatMaybeValue(kv *istore.KeyValue) string {
	if kv == nil {
		return "(absent)"
	}
	return formatValue(kv.Value)
}

func resolveInspectPrefix(flagPrefix string, args []string) (string, error) {
	if len(args) == 0 {
		return flagPrefix, nil
	}
	if flagPrefix != "" {
		return "", fmt.Errorf("use either a positional prefix or --prefix, not both")
	}
	return args[0], nil
}

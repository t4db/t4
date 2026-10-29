package cli

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// newFlagTestCmd returns a command carrying the object-store flags, with a
// no-op body, so PreRunE validation can be exercised through cobra exactly as
// a real invocation would.
func newFlagTestCmd(bucketRequired bool) (*cobra.Command, *objectStoreFlags) {
	cmd := &cobra.Command{
		Use:  "test",
		RunE: func(*cobra.Command, []string) error { return nil },
	}
	cmd.SetOut(&strings.Builder{})
	cmd.SetErr(&strings.Builder{})
	f := addObjectStoreFlags(cmd, bucketRequired)
	return cmd, f
}

func execute(t *testing.T, cmd *cobra.Command, args ...string) error {
	t.Helper()
	cmd.SetArgs(args)
	return cmd.Execute()
}

// Naming both stores is a mistake worth refusing: picking one by a precedence
// rule would quietly send data somewhere the operator did not intend.
func TestObjectStoreFlagsRejectBothBuckets(t *testing.T) {
	cmd, _ := newFlagTestCmd(true)
	err := execute(t, cmd, "--s3-bucket", "a", "--gcs-bucket", "b")
	if err == nil {
		t.Fatal("naming both --s3-bucket and --gcs-bucket must be refused")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("err=%v, should say the two are mutually exclusive", err)
	}
}

func TestObjectStoreFlagsAcceptEitherBucketAlone(t *testing.T) {
	for _, args := range [][]string{
		{"--s3-bucket", "a"},
		{"--gcs-bucket", "b"},
	} {
		t.Run(args[0], func(t *testing.T) {
			cmd, f := newFlagTestCmd(true)
			if err := execute(t, cmd, args...); err != nil {
				t.Fatalf("%v: %v", args, err)
			}
			if !f.configured() {
				t.Error("a named bucket should count as configured")
			}
		})
	}
}

// A command that needs a store has to say so itself: cobra's MarkFlagRequired
// can only require one named flag, and would reject --gcs-bucket on its own.
func TestObjectStoreFlagsRequireAStoreWhenAsked(t *testing.T) {
	cmd, _ := newFlagTestCmd(true)
	err := execute(t, cmd)
	if err == nil {
		t.Fatal("a command that requires a store must refuse to run without one")
	}
	if !strings.Contains(err.Error(), "--s3-bucket or --gcs-bucket") {
		t.Errorf("err=%v, should name both ways to supply a store", err)
	}
}

// t4 run is allowed to start with no object store at all: durability is then
// local-only, which it warns about.
func TestObjectStoreFlagsAllowNoStoreWhenNotRequired(t *testing.T) {
	cmd, f := newFlagTestCmd(false)
	if err := execute(t, cmd); err != nil {
		t.Fatalf("a store is optional here: %v", err)
	}
	if f.configured() {
		t.Error("no bucket was named, so nothing should be configured")
	}
}

// The buckets can arrive through the environment, and the check has to see
// them: it runs after the env-var hooks for that reason.
func TestObjectStoreFlagsSeeBucketsFromTheEnvironment(t *testing.T) {
	t.Run("both from the environment is still refused", func(t *testing.T) {
		t.Setenv("T4_S3_BUCKET", "a")
		t.Setenv("T4_GCS_BUCKET", "b")
		cmd, _ := newFlagTestCmd(true)
		err := execute(t, cmd)
		if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("err=%v, want the mutual-exclusion error", err)
		}
	})

	t.Run("one from the environment satisfies the requirement", func(t *testing.T) {
		t.Setenv("T4_GCS_BUCKET", "b")
		cmd, f := newFlagTestCmd(true)
		if err := execute(t, cmd); err != nil {
			t.Fatalf("T4_GCS_BUCKET should satisfy the requirement: %v", err)
		}
		if f.gcs.Bucket != "b" {
			t.Errorf("gcs bucket = %q, want %q", f.gcs.Bucket, "b")
		}
	})

	t.Run("a flag and the other backend's env var still clash", func(t *testing.T) {
		t.Setenv("T4_GCS_BUCKET", "b")
		cmd, _ := newFlagTestCmd(true)
		err := execute(t, cmd, "--s3-bucket", "a")
		if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("err=%v, want the mutual-exclusion error", err)
		}
	})
}

// Which backend is built follows the bucket that was named.
func TestObjectStoreFlagsBuildTheNamedBackend(t *testing.T) {
	t.Run("gcs", func(t *testing.T) {
		cmd, f := newFlagTestCmd(true)
		// An endpoint keeps this offline: without it the GCS store looks for
		// Application Default Credentials.
		if err := execute(t, cmd, "--gcs-bucket", "b", "--gcs-endpoint", "http://127.0.0.1:1"); err != nil {
			t.Fatal(err)
		}
		store, err := f.newStore(context.Background(), "")
		if err != nil {
			t.Fatalf("newStore: %v", err)
		}
		if got := fmtType(store); !strings.Contains(got, "GCSStore") {
			t.Errorf("built %s, want a GCSStore", got)
		}
		if f.uri() != "gs://b/" {
			t.Errorf("uri = %q, want the gs:// scheme", f.uri())
		}
	})

	t.Run("s3", func(t *testing.T) {
		cmd, f := newFlagTestCmd(true)
		// Static credentials, because the S3 store fails closed without them
		// by design, where the GCS store above needs only an endpoint. That
		// asymmetry is deliberate: see GCSConfig.CredentialsFile.
		if err := execute(t, cmd,
			"--s3-bucket", "b", "--s3-prefix", "t4", "--s3-endpoint", "http://127.0.0.1:1",
			"--s3-access-key-id", "k", "--s3-secret-access-key", "s"); err != nil {
			t.Fatal(err)
		}
		store, err := f.newStore(context.Background(), "")
		if err != nil {
			t.Fatalf("newStore: %v", err)
		}
		if got := fmtType(store); !strings.Contains(got, "S3Store") {
			t.Errorf("built %s, want an S3Store", got)
		}
		if f.uri() != "s3://b/t4" {
			t.Errorf("uri = %q, want the s3:// scheme", f.uri())
		}
	})
}

// A branch node addresses its source through the same backend under a
// different prefix, so the override has to reach the store that gets built.
func TestObjectStoreFlagsPrefixOverrideAppliesToEitherBackend(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"gcs", []string{"--gcs-bucket", "b", "--gcs-endpoint", "http://127.0.0.1:1"}},
		{"s3", []string{
			"--s3-bucket", "b", "--s3-endpoint", "http://127.0.0.1:1",
			"--s3-access-key-id", "k", "--s3-secret-access-key", "s",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd, f := newFlagTestCmd(true)
			if err := execute(t, cmd, tc.args...); err != nil {
				t.Fatal(err)
			}
			if _, err := f.newStore(context.Background(), "source/prefix"); err != nil {
				t.Fatalf("newStore with an override: %v", err)
			}
		})
	}
}

// The startup log should describe the backend in use and not the other one.
func TestObjectStoreFlagsLogFieldsFollowTheBackend(t *testing.T) {
	t.Run("gcs", func(t *testing.T) {
		cmd, f := newFlagTestCmd(true)
		if err := execute(t, cmd, "--gcs-bucket", "b"); err != nil {
			t.Fatal(err)
		}
		got := f.logFields()
		if _, ok := got["gcs_credentials"]; !ok {
			t.Errorf("fields %v should describe the GCS credentials", got)
		}
		for _, unwanted := range []string{"s3_region", "s3_endpoint", "s3_credentials"} {
			if _, ok := got[unwanted]; ok {
				t.Errorf("fields %v should not report %s for a GCS node", got, unwanted)
			}
		}
	})

	t.Run("s3", func(t *testing.T) {
		cmd, f := newFlagTestCmd(true)
		if err := execute(t, cmd, "--s3-bucket", "b"); err != nil {
			t.Fatal(err)
		}
		got := f.logFields()
		if _, ok := got["s3_region"]; !ok {
			t.Errorf("fields %v should describe the S3 region", got)
		}
		if _, ok := got["gcs_credentials"]; ok {
			t.Errorf("fields %v should not report GCS credentials for an S3 node", got)
		}
	})
}

// runMode names the backend, so an operator reading one line of startup log
// can tell which store is behind a single node.
func TestRunModeNamesTheBackend(t *testing.T) {
	for _, tc := range []struct{ uri, want string }{
		{"", "single-node"},
		{"s3://b/t4", "single-node+s3"},
		{"gs://b/t4", "single-node+gcs"},
	} {
		if got := runMode(tc.uri, "", ""); got != tc.want {
			t.Errorf("runMode(%q) = %q, want %q", tc.uri, got, tc.want)
		}
	}
	if got := runMode("gs://b/t4", "127.0.0.1:1", ""); got != "multi-node" {
		t.Errorf("a peer address means multi-node, got %q", got)
	}
	if got := runMode("gs://b/t4", "", "checkpoint/x"); got != "branch" {
		t.Errorf("a branch checkpoint means branch, got %q", got)
	}
}

func TestStoreURIOrEmpty(t *testing.T) {
	cmd, f := newFlagTestCmd(false)
	if err := execute(t, cmd); err != nil {
		t.Fatal(err)
	}
	if got := storeURIOrEmpty(f); got != "" {
		t.Errorf("no store configured should give %q, got %q", "", got)
	}

	cmd, f = newFlagTestCmd(false)
	if err := execute(t, cmd, "--gcs-bucket", "b"); err != nil {
		t.Fatal(err)
	}
	if got := storeURIOrEmpty(f); got != "gs://b/" {
		t.Errorf("got %q, want the configured store's URI", got)
	}
}

func fmtType(v interface{}) string {
	return fmt.Sprintf("%T", v)
}

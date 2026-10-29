package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/t4db/t4"
	"github.com/t4db/t4/pkg/object"
)

// s3Flags holds the parsed values of a set of S3 configuration flags.
type s3Flags struct {
	Bucket          string
	Prefix          string
	Endpoint        string
	Region          string
	Profile         string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	CABundle        string
}

// config converts the flag values into an object.S3Config ready for
// object.NewS3StoreFromConfig.
func (f *s3Flags) config() object.S3Config {
	return object.S3Config{
		Bucket:          f.Bucket,
		Prefix:          f.Prefix,
		Endpoint:        f.Endpoint,
		Region:          f.Region,
		Profile:         f.Profile,
		AccessKeyID:     f.AccessKeyID,
		SecretAccessKey: f.SecretAccessKey,
		SessionToken:    f.SessionToken,
		CABundle:        f.CABundle,
	}
}

// addS3Flags registers the --s3-* flags on cmd, wires their T4_S3_* env vars
// via PreRunE, and returns a pointer to the struct that holds the values.
//
// Whether a store was named at all is not decided here. --gcs-bucket is the
// other way to name one, so the rule is that exactly one of the two is set,
// which addObjectStoreFlags enforces because cobra's MarkFlagRequired can only
// require one named flag and would reject --gcs-bucket on its own.
func addS3Flags(cmd *cobra.Command) *s3Flags {
	f := &s3Flags{}
	cmd.Flags().StringVar(&f.Bucket, "s3-bucket", "", "S3 bucket (env: T4_S3_BUCKET)")
	cmd.Flags().StringVar(&f.Prefix, "s3-prefix", "", "key prefix inside the S3 bucket (env: T4_S3_PREFIX)")
	cmd.Flags().StringVar(&f.Endpoint, "s3-endpoint", "", "custom S3 endpoint URL, e.g. for S3-compatible stores (env: T4_S3_ENDPOINT)")
	cmd.Flags().StringVar(&f.Region, "s3-region", "", "AWS region (env: T4_S3_REGION)")
	cmd.Flags().StringVar(&f.Profile, "s3-profile", "", "enable the ambient AWS credentials chain (env vars → ~/.aws/credentials[profile] → EC2/EKS IMDS); SSO and AssumeRole profiles are not supported; use 'default' to opt in to the default profile (env: T4_S3_PROFILE)")
	cmd.Flags().StringVar(&f.AccessKeyID, "s3-access-key-id", "", "t4 S3 access key ID; when set with --s3-secret-access-key, uses static credentials (env: T4_S3_ACCESS_KEY_ID)")
	cmd.Flags().StringVar(&f.SecretAccessKey, "s3-secret-access-key", "", "AWS secret access key (env: T4_S3_SECRET_ACCESS_KEY)")
	cmd.Flags().StringVar(&f.SessionToken, "s3-session-token", "", "session token for temporary STS credentials; honored with --s3-access-key-id/--s3-secret-access-key (env: T4_S3_SESSION_TOKEN)")
	cmd.Flags().StringVar(&f.CABundle, "s3-ca-bundle", "", "PEM CA bundle file to trust for HTTPS to the S3 endpoint; use this for S3-compatible stores running behind a self-signed CA (env: T4_S3_CA_BUNDLE)")
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{
			"s3-bucket":            "T4_S3_BUCKET",
			"s3-prefix":            "T4_S3_PREFIX",
			"s3-endpoint":          "T4_S3_ENDPOINT",
			"s3-region":            "T4_S3_REGION",
			"s3-profile":           "T4_S3_PROFILE",
			"s3-access-key-id":     "T4_S3_ACCESS_KEY_ID",
			"s3-secret-access-key": "T4_S3_SECRET_ACCESS_KEY",
			"s3-session-token":     "T4_S3_SESSION_TOKEN",
			"s3-ca-bundle":         "T4_S3_CA_BUNDLE",
		})
	})
	return f
}

// gcsFlags holds the parsed values of the Google Cloud Storage flags.
type gcsFlags struct {
	Bucket          string
	Prefix          string
	CredentialsFile string
	Endpoint        string
}

func (f *gcsFlags) config() object.GCSConfig {
	return object.GCSConfig{
		Bucket:          f.Bucket,
		Prefix:          f.Prefix,
		CredentialsFile: f.CredentialsFile,
		Endpoint:        f.Endpoint,
	}
}

// addGCSFlags registers the --gcs-* flags and wires their T4_GCS_* env vars.
func addGCSFlags(cmd *cobra.Command) *gcsFlags {
	f := &gcsFlags{}
	cmd.Flags().StringVar(&f.Bucket, "gcs-bucket", "", "Google Cloud Storage bucket, reached over the GCS API; mutually exclusive with --s3-bucket (env: T4_GCS_BUCKET)")
	cmd.Flags().StringVar(&f.Prefix, "gcs-prefix", "", "object name prefix inside the GCS bucket (env: T4_GCS_PREFIX)")
	cmd.Flags().StringVar(&f.CredentialsFile, "gcs-credentials-file", "", "service account key file; without it credentials come from Application Default Credentials, which is what workload identity on GKE provides (env: T4_GCS_CREDENTIALS_FILE)")
	cmd.Flags().StringVar(&f.Endpoint, "gcs-endpoint", "", "custom GCS endpoint URL, for an emulator; requests are unauthenticated unless --gcs-credentials-file is also set (env: T4_GCS_ENDPOINT)")
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{
			"gcs-bucket":           "T4_GCS_BUCKET",
			"gcs-prefix":           "T4_GCS_PREFIX",
			"gcs-credentials-file": "T4_GCS_CREDENTIALS_FILE",
			"gcs-endpoint":         "T4_GCS_ENDPOINT",
		})
	})
	return f
}

// objectStoreFlags carries every backend's flags. Exactly one backend's bucket
// may be named: they are alternatives, and guessing which one an operator
// meant when both are set would be worse than refusing.
type objectStoreFlags struct {
	s3  *s3Flags
	gcs *gcsFlags
}

// addObjectStoreFlags registers both backends' flags and enforces the choice
// between them. bucketRequired asks for a store to be named at all.
func addObjectStoreFlags(cmd *cobra.Command, bucketRequired bool) *objectStoreFlags {
	f := &objectStoreFlags{s3: addS3Flags(cmd), gcs: addGCSFlags(cmd)}
	// Appended rather than prepended, so it runs after the env-var hooks that
	// addS3Flags and addGCSFlags installed: T4_S3_BUCKET and T4_GCS_BUCKET
	// have to be in place before the choice between them can be judged.
	appendPreRunE(cmd, func(*cobra.Command, []string) error {
		switch {
		case f.s3.Bucket != "" && f.gcs.Bucket != "":
			return errors.New("--s3-bucket and --gcs-bucket are mutually exclusive: name one object store")
		case bucketRequired && !f.configured():
			return errors.New("an object store is required: set --s3-bucket or --gcs-bucket")
		}
		return nil
	})
	return f
}

// configured reports whether a store was named at all.
func (f *objectStoreFlags) configured() bool { return f.s3.Bucket != "" || f.gcs.Bucket != "" }

// uri names the store the way an operator would write it, so a message says
// which store it is talking about rather than assuming S3.
func (f *objectStoreFlags) uri() string {
	if f.gcs.Bucket != "" {
		return fmt.Sprintf("gs://%s/%s", f.gcs.Bucket, f.gcs.Prefix)
	}
	return fmt.Sprintf("s3://%s/%s", f.s3.Bucket, f.s3.Prefix)
}

// logFields describes the configured backend for the startup log. Only the
// chosen backend's settings appear: reporting an AWS region for a node running
// on GCS would be noise at best and misleading at worst.
func (f *objectStoreFlags) logFields() map[string]interface{} {
	if f.gcs.Bucket != "" {
		return map[string]interface{}{
			"gcs_endpoint":    valueOrDefault(f.gcs.Endpoint, "google-default"),
			"gcs_credentials": gcsCredentialsMode(f.gcs.CredentialsFile),
		}
	}
	return map[string]interface{}{
		"s3_endpoint":    valueOrDefault(f.s3.Endpoint, "aws-default"),
		"s3_region":      valueOrDefault(f.s3.Region, "aws-default"),
		"s3_credentials": s3CredentialsMode(f.s3.AccessKeyID, f.s3.Profile),
	}
}

// gcsCredentialsMode names where GCS credentials come from.
func gcsCredentialsMode(credentialsFile string) string {
	if credentialsFile != "" {
		return "service-account-file"
	}
	return "application-default"
}

// newStore builds the store the flags name, without the encryption wrapper.
// A non-empty prefixOverride replaces the configured prefix, which is how a
// branch node addresses its source.
func (f *objectStoreFlags) newStore(ctx context.Context, prefixOverride string) (object.Store, error) {
	if f.gcs.Bucket != "" {
		cfg := f.gcs.config()
		if prefixOverride != "" {
			cfg.Prefix = prefixOverride
		}
		store, err := object.NewGCSStoreFromConfig(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("init GCS: %w", err)
		}
		return store, nil
	}
	cfg := f.s3.config()
	if prefixOverride != "" {
		cfg.Prefix = prefixOverride
	}
	store, err := object.NewS3StoreFromConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("init S3: %w", err)
	}
	return store, nil
}

type objectStoreEncryptionFlags struct {
	KeyFile string
	KeyEnv  string
}

func addObjectStoreEncryptionFlags(cmd *cobra.Command) *objectStoreEncryptionFlags {
	f := &objectStoreEncryptionFlags{}
	cmd.Flags().StringVar(&f.KeyFile, "object-store-encryption-key-file", "", "file containing a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 (env: T4_OBJECT_STORE_ENCRYPTION_KEY_FILE)")
	cmd.Flags().StringVar(&f.KeyEnv, "object-store-encryption-key-env", "", "environment variable holding a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 (env: T4_OBJECT_STORE_ENCRYPTION_KEY_ENV)")
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{
			"object-store-encryption-key-file": "T4_OBJECT_STORE_ENCRYPTION_KEY_FILE",
			"object-store-encryption-key-env":  "T4_OBJECT_STORE_ENCRYPTION_KEY_ENV",
		})
	})
	return f
}

func (f *objectStoreEncryptionFlags) enabled() bool {
	return f != nil && (f.KeyFile != "" || f.KeyEnv != "")
}

func (f *objectStoreEncryptionFlags) config() (*t4.ObjectStoreEncryptionConfig, error) {
	if !f.enabled() {
		return nil, nil
	}
	kp, err := f.keyProvider()
	if err != nil {
		return nil, err
	}
	return &t4.ObjectStoreEncryptionConfig{KeyProvider: kp}, nil
}

func (f *objectStoreEncryptionFlags) wrapStore(store object.Store) (object.Store, error) {
	if !f.enabled() || store == nil {
		return store, nil
	}
	kp, err := f.keyProvider()
	if err != nil {
		return nil, err
	}
	return object.NewEncryptedStore(store, kp), nil
}

func newObjectStoreForCommand(ctx context.Context, f *objectStoreFlags, enc *objectStoreEncryptionFlags) (object.Store, error) {
	rawStore, err := f.newStore(ctx, "")
	if err != nil {
		return nil, err
	}
	store, err := enc.wrapStore(rawStore)
	if err != nil {
		return nil, fmt.Errorf("encryption key: %w", err)
	}
	return store, nil
}

func (f *objectStoreEncryptionFlags) keyProvider() (*object.StaticKeyProvider, error) {
	raw, err := f.keyMaterial()
	if err != nil {
		return nil, err
	}
	decoded, err := decodeKeyMaterial(raw)
	if err != nil {
		return nil, err
	}
	return object.NewStaticKeyProvider(decoded)
}

func (f *objectStoreEncryptionFlags) keyMaterial() ([]byte, error) {
	if f.KeyFile != "" {
		data, err := os.ReadFile(f.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("read object-store encryption key file: %w", err)
		}
		return data, nil
	}
	val := os.Getenv(f.KeyEnv)
	if val == "" {
		return nil, fmt.Errorf("environment variable %q is empty or not set", f.KeyEnv)
	}
	return []byte(val), nil
}

func decodeKeyMaterial(raw []byte) ([]byte, error) {
	if len(raw) == 32 {
		return append([]byte(nil), raw...), nil
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 32 {
		return append([]byte(nil), raw...), nil
	}
	if len(raw) == 64 {
		dst := make([]byte, 32)
		if _, err := hex.Decode(dst, raw); err == nil {
			return dst, nil
		}
	}
	if dst, err := base64.StdEncoding.DecodeString(string(raw)); err == nil && len(dst) == 32 {
		return dst, nil
	}
	if dst, err := base64.RawStdEncoding.DecodeString(string(raw)); err == nil && len(dst) == 32 {
		return dst, nil
	}
	return nil, fmt.Errorf("object-store encryption key must be 32 raw bytes, 64 hex chars, or base64 for 32 bytes; got %d bytes", len(raw))
}

// prependPreRunE prepends fn to cmd's existing PreRunE hook, if any.
// This allows multiple callers to register pre-run logic without overwriting
// each other.
func prependPreRunE(cmd *cobra.Command, fn func(*cobra.Command, []string) error) {
	prev := cmd.PreRunE
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if err := fn(cmd, args); err != nil {
			return err
		}
		if prev != nil {
			return prev(cmd, args)
		}
		return nil
	}
}

// appendPreRunE appends fn after cmd's existing PreRunE hook, for logic that
// has to see the result of the hooks already registered — validating a choice
// between flags, for instance, once their env-var fallbacks have been applied.
func appendPreRunE(cmd *cobra.Command, fn func(*cobra.Command, []string) error) {
	prev := cmd.PreRunE
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if prev != nil {
			if err := prev(cmd, args); err != nil {
				return err
			}
		}
		return fn(cmd, args)
	}
}

// applyEnvVars sets flag values from environment variables for flags that have
// not been explicitly provided on the command line. mapping is a map of flag
// name → environment variable name (e.g. "s3-bucket" → "T4_S3_BUCKET").
// Flags marked as required pass cobra's validation when the env var is set,
// because pflag.Set marks the flag as changed.
func applyEnvVars(cmd *cobra.Command, mapping map[string]string) error {
	for flag, env := range mapping {
		if !cmd.Flags().Changed(flag) {
			if v := os.Getenv(env); v != "" {
				if err := cmd.Flags().Set(flag, v); err != nil {
					return fmt.Errorf("env %s: %w", env, err)
				}
			}
		}
	}
	return nil
}

func valueOrDisabled(v string) string {
	if v == "" {
		return "disabled"
	}
	return v
}

func valueOrNone(v string) string {
	if v == "" {
		return "(none)"
	}
	return v
}

func valueOrDefault(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

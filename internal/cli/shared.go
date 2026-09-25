package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
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

// addS3Flags registers --s3-bucket/prefix/endpoint/region/profile on cmd,
// wires their T4_S3_BUCKET/PREFIX/ENDPOINT/REGION/PROFILE env vars via
// PreRunE, and returns a pointer to the struct that holds the values.
//
// If bucketRequired is true, --s3-bucket is marked required; setting
// T4_S3_BUCKET satisfies that requirement.
//
// Typical usage:
//
//	s3 := addS3Flags(cmd, true)
//	...
//	store, err := object.NewS3StoreFromConfig(ctx, s3.config())
func addS3Flags(cmd *cobra.Command, bucketRequired bool) *s3Flags {
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
	if bucketRequired {
		cmd.MarkFlagRequired("s3-bucket")
	}
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

func newObjectStoreForCommand(ctx context.Context, s3 *s3Flags, enc *objectStoreEncryptionFlags) (object.Store, error) {
	rawStore, err := object.NewS3StoreFromConfig(ctx, s3.config())
	if err != nil {
		return nil, fmt.Errorf("init S3: %w", err)
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

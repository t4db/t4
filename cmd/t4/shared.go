package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

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
	cmd.Flags().StringVar(&f.Endpoint, "s3-endpoint", "", "custom S3 endpoint URL, e.g. for MinIO (env: T4_S3_ENDPOINT)")
	cmd.Flags().StringVar(&f.Region, "s3-region", "", "AWS region (env: T4_S3_REGION)")
	cmd.Flags().StringVar(&f.Profile, "s3-profile", "", "AWS shared config profile (env: T4_S3_PROFILE)")
	cmd.Flags().StringVar(&f.AccessKeyID, "s3-access-key-id", "", "AWS access key ID (env: T4_S3_ACCESS_KEY_ID)")
	cmd.Flags().StringVar(&f.SecretAccessKey, "s3-secret-access-key", "", "AWS secret access key (env: T4_S3_SECRET_ACCESS_KEY)")
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
		})
	})
	return f
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

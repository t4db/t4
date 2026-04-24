package object

import (
	"context"
	"strings"
	"testing"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

func TestS3ConfigDisablesSharedFilesWithoutExplicitProfile(t *testing.T) {
	_, err := NewS3StoreFromConfig(context.Background(), S3Config{Bucket: "bucket"})
	if err == nil {
		t.Fatalf("expected missing credentials error")
	}
	const want = "credentials not configured"
	if got := err.Error(); !strings.Contains(got, want) {
		t.Fatalf("expected %q in error, got %q", want, got)
	}
}

func TestS3ConfigKeepsSharedFilesWhenProfileExplicit(t *testing.T) {
	var opts awsconfig.LoadOptions
	for _, fn := range s3LoadOptions(S3Config{Profile: "prod"}) {
		if err := fn(&opts); err != nil {
			t.Fatalf("apply load option: %v", err)
		}
	}

	if opts.SharedConfigProfile != "prod" {
		t.Fatalf("expected explicit shared config profile, got %q", opts.SharedConfigProfile)
	}
}

func TestS3ConfigDefaultsRegionForCustomEndpoint(t *testing.T) {
	var opts awsconfig.LoadOptions
	for _, fn := range s3LoadOptions(S3Config{
		Endpoint:        "http://minio:9000",
		AccessKeyID:     "access",
		SecretAccessKey: "secret",
	}) {
		if err := fn(&opts); err != nil {
			t.Fatalf("apply load option: %v", err)
		}
	}

	if opts.Region != defaultS3EndpointRegion {
		t.Fatalf("expected endpoint-backed store to default region to %q, got %q", defaultS3EndpointRegion, opts.Region)
	}
}

func TestS3ConfigPreservesExplicitRegionWithCustomEndpoint(t *testing.T) {
	var opts awsconfig.LoadOptions
	for _, fn := range s3LoadOptions(S3Config{
		Endpoint:        "http://minio:9000",
		Region:          "eu-west-1",
		AccessKeyID:     "access",
		SecretAccessKey: "secret",
	}) {
		if err := fn(&opts); err != nil {
			t.Fatalf("apply load option: %v", err)
		}
	}

	if opts.Region != "eu-west-1" {
		t.Fatalf("expected explicit region to be preserved, got %q", opts.Region)
	}
}

func TestS3ConfigDoesNotDefaultRegionForAWSWithoutEndpoint(t *testing.T) {
	var opts awsconfig.LoadOptions
	for _, fn := range s3LoadOptions(S3Config{
		AccessKeyID:     "access",
		SecretAccessKey: "secret",
	}) {
		if err := fn(&opts); err != nil {
			t.Fatalf("apply load option: %v", err)
		}
	}

	if opts.Region != "" {
		t.Fatalf("expected empty region without custom endpoint, got %q", opts.Region)
	}
}

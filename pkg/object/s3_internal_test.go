package object

import (
	"context"
	"strings"
	"testing"
)

func TestS3ConfigDisablesAmbientCredsWithoutExplicitProfile(t *testing.T) {
	_, err := NewS3StoreFromConfig(context.Background(), S3Config{Bucket: "bucket"})
	if err == nil {
		t.Fatalf("expected missing credentials error")
	}
	const want = "credentials not configured"
	if got := err.Error(); !strings.Contains(got, want) {
		t.Fatalf("expected %q in error, got %q", want, got)
	}
}

func TestS3ConfigKeepsExplicitProfile(t *testing.T) {
	opts, err := s3MinioOptions(S3Config{Profile: "prod"})
	if err != nil {
		t.Fatalf("s3MinioOptions: %v", err)
	}
	if opts.Creds == nil {
		t.Fatalf("expected credentials provider for explicit profile")
	}
}

func TestS3ConfigDefaultsRegionForCustomEndpoint(t *testing.T) {
	opts, err := s3MinioOptions(S3Config{
		Endpoint:        "http://minio:9000",
		AccessKeyID:     "access",
		SecretAccessKey: "secret",
	})
	if err != nil {
		t.Fatalf("s3MinioOptions: %v", err)
	}
	if opts.Region != defaultS3EndpointRegion {
		t.Fatalf("expected endpoint-backed store to default region to %q, got %q", defaultS3EndpointRegion, opts.Region)
	}
}

func TestS3ConfigPreservesExplicitRegionWithCustomEndpoint(t *testing.T) {
	opts, err := s3MinioOptions(S3Config{
		Endpoint:        "http://minio:9000",
		Region:          "eu-west-1",
		AccessKeyID:     "access",
		SecretAccessKey: "secret",
	})
	if err != nil {
		t.Fatalf("s3MinioOptions: %v", err)
	}
	if opts.Region != "eu-west-1" {
		t.Fatalf("expected explicit region to be preserved, got %q", opts.Region)
	}
}

func TestS3ConfigDoesNotDefaultRegionForAWSWithoutEndpoint(t *testing.T) {
	opts, err := s3MinioOptions(S3Config{
		AccessKeyID:     "access",
		SecretAccessKey: "secret",
	})
	if err != nil {
		t.Fatalf("s3MinioOptions: %v", err)
	}
	if opts.Region != "" {
		t.Fatalf("expected empty region without custom endpoint, got %q", opts.Region)
	}
}

func TestS3EndpointSecureFromScheme(t *testing.T) {
	cases := []struct {
		endpoint string
		secure   bool
	}{
		{"", true},
		{"http://minio:9000", false},
		{"https://minio:9000", true},
	}
	for _, c := range cases {
		if got := s3EndpointSecure(c.endpoint); got != c.secure {
			t.Errorf("s3EndpointSecure(%q): want %v got %v", c.endpoint, c.secure, got)
		}
	}
}

func TestS3EndpointHostRequiresScheme(t *testing.T) {
	_, err := s3EndpointHost("minio:9000")
	if err == nil {
		t.Fatalf("expected error for endpoint without scheme")
	}
	if !strings.Contains(err.Error(), "must include scheme") {
		t.Fatalf("expected scheme error, got %v", err)
	}
}

func TestS3EndpointHostStripsScheme(t *testing.T) {
	got, err := s3EndpointHost("https://minio.example.com:9000")
	if err != nil {
		t.Fatalf("s3EndpointHost: %v", err)
	}
	if got != "minio.example.com:9000" {
		t.Fatalf("want minio.example.com:9000, got %q", got)
	}
}

func TestS3EndpointHostDefaultsToAWS(t *testing.T) {
	got, err := s3EndpointHost("")
	if err != nil {
		t.Fatalf("s3EndpointHost: %v", err)
	}
	if got != defaultAWSS3Endpoint {
		t.Fatalf("want %q, got %q", defaultAWSS3Endpoint, got)
	}
}

func TestS3StorePrefixIgnoresSurroundingSlashes(t *testing.T) {
	for _, prefix := range []string{"t4", "t4/", "/t4", "/t4/", "t4//"} {
		s := NewS3Store(nil, "bucket", prefix)
		if got := s.key("manifest/latest"); got != "t4/manifest/latest" {
			t.Errorf("prefix %q: key = %q, want %q", prefix, got, "t4/manifest/latest")
		}
	}
	for _, prefix := range []string{"", "/", "//"} {
		s := NewS3Store(nil, "bucket", prefix)
		if got := s.key("manifest/latest"); got != "manifest/latest" {
			t.Errorf("prefix %q: key = %q, want %q", prefix, got, "manifest/latest")
		}
	}
	if got := NewS3Store(nil, "bucket", "a/b/").key("wal/1"); got != "a/b/wal/1" {
		t.Errorf("nested prefix: key = %q, want %q", got, "a/b/wal/1")
	}
}

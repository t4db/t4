package main

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/strata-db/strata/pkg/object"
)

func newS3Store(ctx context.Context, bucket, prefix, endpoint string) (*object.S3Store, error) {
	optFns := []func(*config.LoadOptions) error{}
	if endpoint != "" {
		optFns = append(optFns, config.WithBaseEndpoint(endpoint))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, err
	}
	var clientOpts []func(*s3.Options)
	if endpoint != "" {
		// Path-style is required for MinIO and other S3-compatible stores.
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}
	client := s3.NewFromConfig(awsCfg, clientOpts...)
	return object.NewS3Store(client, bucket, prefix), nil
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

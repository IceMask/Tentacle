package s3

import (
	"bytes"
	"context"
	"strings"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Client struct {
	s3Client *s3.Client
	presign  *s3.PresignClient
	bucket   string
}

// NewClient executes this operation.
func NewClient(ctx context.Context, cfg config.S3Config) (*Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}

	if cfg.Endpoint != "" {
		// Custom endpoint resolver for MinIO etc.
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:           cfg.Endpoint,
				SigningRegion: region,
			}, nil
		})
		opts = append(opts, awsconfig.WithEndpointResolverWithOptions(resolver))
	}

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		// Static credentials
		creds := aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     cfg.AccessKeyID,
				SecretAccessKey: cfg.SecretAccessKey,
			}, nil
		})
		opts = append(opts, awsconfig.WithCredentialsProvider(creds))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(errors.CodeConfigInvalid, "failed to load aws config", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
	})

	return &Client{
		s3Client: client,
		presign:  s3.NewPresignClient(client),
		bucket:   cfg.Bucket,
	}, nil
}

// ValidateBucket verifies that the configured bucket name is present and reachable for metadata requests.
func (c *Client) ValidateBucket(ctx context.Context) error {
	if strings.TrimSpace(c.bucket) == "" { // Reject an empty bucket name before any request is signed or sent to the object store.
		return errors.New(errors.CodeConfigMissing, "s3 bucket is empty") // Surface the missing bucket as a startup configuration error.
	}
	if _, err := c.s3Client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(c.bucket)}); err != nil { // Probe the configured bucket so missing or inaccessible buckets fail during startup instead of the first artifact upload.
		return errors.Wrap(errors.CodeStoreConn, "failed to access s3 bucket", err) // Surface bucket reachability or permission failures as storage connection errors.
	}

	return nil // Return success once the configured bucket has been verified successfully.
}

// PresignPut executes this operation.
func (c *Client) PresignPut(ctx context.Context, key string, contentType string, lifetime time.Duration) (string, error) {
	req, err := c.presign.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	}, s3.WithPresignExpires(lifetime))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}

// PresignGet executes this operation.
func (c *Client) PresignGet(ctx context.Context, key string, lifetime time.Duration) (string, error) {
	req, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(lifetime))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}

// PutObject executes this operation.
func (c *Client) PutObject(ctx context.Context, key string, contentType string, data []byte) error {
	_, err := c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
		Body:        bytes.NewReader(data),
	})
	return err
}

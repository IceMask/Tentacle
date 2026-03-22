// client_test.go verifies local validation behavior for the S3 client wrapper.
package s3

import (
	"context"
	"testing"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
)

// TestValidateBucketRejectsEmptyBucket verifies that bucket validation fails fast before any network request is attempted.
func TestValidateBucketRejectsEmptyBucket(t *testing.T) {
	client, err := NewClient(context.Background(), config.S3Config{Region: "us-east-1"}) // Construct one production S3 client with no bucket configured so the local bucket-validation path is exercised directly.
	if err != nil {                                                                      // Fail the test when the client cannot be constructed without reaching the bucket-validation path.
		t.Fatalf("expected client construction to succeed, got error: %v", err) // Surface the unexpected construction failure so local-validation regressions are obvious.
	}

	if err := client.ValidateBucket(context.Background()); err == nil { // Validate the empty bucket so the local missing-config path runs before any network request is attempted.
		t.Fatal("expected empty bucket validation to fail") // Surface the missing failure because startup preflight depends on this fast local guard.
	} else if !errors.IsCode(err, errors.CodeConfigMissing) { // Fail the test when the returned error does not preserve the stable missing-config code.
		t.Fatalf("expected config missing error, got %v", err) // Surface the unexpected error so validation regressions are obvious.
	}
}

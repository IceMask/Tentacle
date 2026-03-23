// client_test.go verifies local validation, presigning, and upload behavior for the S3 client wrapper.
package s3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

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

// TestPresignGetAndPutUseConfiguredEndpoint verifies that presigned URLs honor the configured custom endpoint and path-style bucket layout.
func TestPresignGetAndPutUseConfiguredEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // Start one local HTTP server because presigned URL generation should point at the configured endpoint host.
		w.WriteHeader(http.StatusOK) // Return a trivial success response because this test asserts only URL generation, not transport behavior.
	}))
	defer server.Close() // Ensure the local HTTP server always stops after the test completes.

	client, err := NewClient(context.Background(), config.S3Config{Region: "us-east-1", Bucket: "artifacts-bucket", Endpoint: server.URL, ForcePathStyle: true, AccessKeyID: "test-access-key", SecretAccessKey: "test-secret-key"}) // Construct one production S3 client with a custom endpoint and static credentials so presigning works without any external AWS dependency.
	if err != nil {                                                                                                                                                                                                                  // Fail the test when the client cannot be constructed for local presign verification.
		t.Fatalf("expected client construction to succeed, got error: %v", err) // Surface the unexpected construction failure so presign regressions are obvious.
	}

	putURL, err := client.PresignPut(context.Background(), "trace-1/screenshot.png", "image/png", 5*time.Minute) // Generate one presigned PUT URL through the production helper so endpoint and path-style behavior can be asserted directly.
	if err != nil {                                                                                              // Fail the test when the presign helper cannot build the PUT URL unexpectedly.
		t.Fatalf("expected presign put to succeed, got error: %v", err) // Surface the unexpected error so presign regressions are obvious.
	}
	getURL, err := client.PresignGet(context.Background(), "trace-1/screenshot.png", 5*time.Minute) // Generate one presigned GET URL through the production helper so endpoint and path-style behavior can be asserted directly.
	if err != nil {                                                                                 // Fail the test when the presign helper cannot build the GET URL unexpectedly.
		t.Fatalf("expected presign get to succeed, got error: %v", err) // Surface the unexpected error so presign regressions are obvious.
	}

	assertPresignedPathStyleURL(t, putURL, server.URL, "/artifacts-bucket/trace-1/screenshot.png") // Assert the PUT URL uses the configured endpoint host and path-style bucket layout.
	assertPresignedPathStyleURL(t, getURL, server.URL, "/artifacts-bucket/trace-1/screenshot.png") // Assert the GET URL uses the configured endpoint host and path-style bucket layout.
}

// TestPutObjectUsesConfiguredEndpoint verifies that object uploads use the configured custom endpoint and path-style bucket layout.
func TestPutObjectUsesConfiguredEndpoint(t *testing.T) {
	requests := make(chan *http.Request, 1)                                                      // Capture one upload request so the production PutObject helper can be asserted without any external S3 dependency.
	bodies := make(chan []byte, 1)                                                               // Capture one upload body so the production PutObject helper can be asserted without any external S3 dependency.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // Start one local HTTP server because the production client should send uploads to the configured endpoint.
		bodyBytes, err := io.ReadAll(r.Body) // Read the full request body so the uploaded bytes can be asserted directly after the helper returns.
		if err != nil {                      // Fail the HTTP handler fast when the request body cannot be read because the upload assertion would be meaningless.
			t.Fatalf("failed to read uploaded body: %v", err) // Surface the handler failure because the test depends on observing the exact uploaded payload.
		}
		requests <- r                // Preserve the received request so method, path, and content-type assertions can run after the helper returns.
		bodies <- bodyBytes          // Preserve the received body bytes so payload assertions can run after the helper returns.
		w.WriteHeader(http.StatusOK) // Return a trivial success response so the production helper treats the upload as successful.
	}))
	defer server.Close() // Ensure the local HTTP server always stops after the test completes.

	client, err := NewClient(context.Background(), config.S3Config{Region: "us-east-1", Bucket: "artifacts-bucket", Endpoint: server.URL, ForcePathStyle: true, AccessKeyID: "test-access-key", SecretAccessKey: "test-secret-key"}) // Construct one production S3 client with a custom endpoint and static credentials so uploads work against the local HTTP server.
	if err != nil {                                                                                                                                                                                                                  // Fail the test when the client cannot be constructed for local upload verification.
		t.Fatalf("expected client construction to succeed, got error: %v", err) // Surface the unexpected construction failure so upload regressions are obvious.
	}

	if err := client.PutObject(context.Background(), "trace-1/screenshot.png", "image/png", []byte("PNG")); err != nil { // Upload one small object through the production helper so request shaping can be asserted directly.
		t.Fatalf("expected put object to succeed, got error: %v", err) // Surface the unexpected upload error so transport regressions are obvious.
	}

	request := <-requests                 // Read back the captured request so the upload method and path can be asserted directly.
	body := <-bodies                      // Read back the captured body so the upload payload can be asserted directly.
	if request.Method != http.MethodPut { // Fail the test when the production helper does not use an HTTP PUT request for the object upload.
		t.Fatalf("expected PUT request, got %s", request.Method) // Surface the unexpected method so upload regressions are obvious.
	}
	if request.URL.Path != "/artifacts-bucket/trace-1/screenshot.png" { // Fail the test when the production helper does not use the configured path-style bucket layout.
		t.Fatalf("expected path-style upload path, got %s", request.URL.Path) // Surface the unexpected path so upload regressions are obvious.
	}
	if request.Header.Get("Content-Type") != "image/png" { // Fail the test when the production helper does not forward the object content type to S3.
		t.Fatalf("expected content type image/png, got %q", request.Header.Get("Content-Type")) // Surface the unexpected content type so upload regressions are obvious.
	}
	if string(body) != "PNG" { // Fail the test when the production helper does not upload the exact caller-supplied payload bytes.
		t.Fatalf("expected uploaded body PNG, got %q", string(body)) // Surface the unexpected payload so upload regressions are obvious.
	}
}

// assertPresignedPathStyleURL verifies that a presigned URL points at the configured endpoint and requested path-style object location.
func assertPresignedPathStyleURL(t *testing.T, rawURL string, expectedBaseURL string, expectedPath string) {
	t.Helper()                          // Mark the helper so failure locations point back to the calling test case.
	parsedURL, err := url.Parse(rawURL) // Parse the generated presigned URL so host, path, and query values can be asserted directly.
	if err != nil {                     // Fail the helper when the generated URL is not parseable at all.
		t.Fatalf("expected valid URL, got error: %v", err) // Surface the malformed URL so presign regressions are obvious.
	}
	expectedBase, err := url.Parse(expectedBaseURL) // Parse the configured endpoint URL so host assertions compare normalized URL components instead of raw strings.
	if err != nil {                                 // Fail the helper when the expected endpoint URL is not parseable, because the test setup itself would be invalid.
		t.Fatalf("expected valid base URL, got error: %v", err) // Surface the malformed base URL so test setup issues are obvious.
	}
	if parsedURL.Host != expectedBase.Host { // Fail the helper when the presigned URL does not target the configured custom endpoint host.
		t.Fatalf("expected host %q, got %q", expectedBase.Host, parsedURL.Host) // Surface the unexpected host so endpoint-routing regressions are obvious.
	}
	if parsedURL.Path != expectedPath { // Fail the helper when the presigned URL does not use the expected path-style bucket and key layout.
		t.Fatalf("expected path %q, got %q", expectedPath, parsedURL.Path) // Surface the unexpected path so endpoint-layout regressions are obvious.
	}
	if !strings.Contains(parsedURL.RawQuery, "X-Amz-Signature=") { // Fail the helper when the presigned URL does not include an AWS signature query parameter.
		t.Fatalf("expected presigned URL to contain X-Amz-Signature, got %q", parsedURL.RawQuery) // Surface the unexpected query string so signing regressions are obvious.
	}
}

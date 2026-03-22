// hmac_test.go verifies canonical-query HMAC validation and nonce replay protection for the gateway auth path.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"mcp_for_appium/internal/errors"
)

// memoryNonceStore implements the minimal replay-protection store contract needed by the HMAC validator tests.
type memoryNonceStore struct {
	mu     sync.Mutex
	values map[string]string
}

// CheckAndSet stores one nonce value when it has not been seen before and reports whether this call acquired the nonce slot.
func (s *memoryNonceStore) CheckAndSet(_ context.Context, key string, value string, _ time.Duration) (bool, string, error) {
	s.mu.Lock()          // Serialize access to the in-memory map so concurrent test calls behave deterministically.
	defer s.mu.Unlock()  // Release the mutex after the nonce lookup and optional insert have completed.
	if s.values == nil { // Allocate the backing map lazily so zero-value stores still behave correctly in tests.
		s.values = make(map[string]string) // Create the replay-protection map before the first insert.
	}
	if existingValue, ok := s.values[key]; ok { // Detect duplicate nonce usage before overwriting the original stored value.
		return false, existingValue, nil // Report the duplicate nonce together with the stored marker value expected by the interface contract.
	}
	s.values[key] = value // Persist the nonce marker so a later duplicate request is rejected by the validator.
	return true, "", nil  // Report successful nonce acquisition because this request used a new nonce.
}

// TestHMACValidatorAcceptsCanonicalQuery verifies that HMAC validation signs the canonicalized query string rather than the raw input order.
func TestHMACValidatorAcceptsCanonicalQuery(t *testing.T) {
	validator := NewHMACValidator(func(_ context.Context, keyID string) (*HMACKeyMaterial, error) { // Build a validator over one static in-memory HMAC key so the test can focus on canonical query handling.
		return &HMACKeyMaterial{KeyID: keyID, SubjectID: "subject-1", TenantID: "tenant-1", Secret: "super-secret", Status: "active"}, nil // Return the authoritative key material expected by the validator for the supplied key ID.
	}, &memoryNonceStore{}, 5*time.Minute, 15*time.Minute) // Provide replay protection and explicit timing values so the test follows the production contract.

	signatureTimestamp := fmt.Sprintf("%d", time.Now().UTC().Unix())                                                                                                // Capture one current timestamp so the test request falls inside the configured signature window.
	signature := mustComputeTestSignature(t, "super-secret", "POST", "/api/v1/sessions", "b=2&a=1", `{"ok":true}`, signatureTimestamp, "nonce-1", "key-1")          // Compute the expected signature from the same canonical request rules used by the validator.
	subject, err := validator.Verify(context.Background(), signature, "POST", "/api/v1/sessions", "b=2&a=1", `{"ok":true}`, signatureTimestamp, "nonce-1", "key-1") // Verify the signed request using the unsorted raw query string so canonicalization is exercised at runtime.
	if err != nil {                                                                                                                                                 // Fail the test when canonical-query verification unexpectedly rejects a correctly signed request.
		t.Fatalf("expected canonical-query HMAC verification to succeed, got error: %v", err) // Surface the unexpected validation failure together with the returned error.
	}
	if subject == nil || subject.ID != "subject-1" || subject.TenantID != "tenant-1" { // Fail the test when the validator does not return the authoritative subject metadata.
		t.Fatalf("expected HMAC subject subject-1/tenant-1, got %#v", subject) // Surface the unexpected authenticated subject so the mismatch is easy to diagnose.
	}
}

// TestHMACValidatorRejectsDuplicateNonce verifies that replaying the same signed request nonce returns the dedicated nonce-duplicate error code.
func TestHMACValidatorRejectsDuplicateNonce(t *testing.T) {
	validator := NewHMACValidator(func(_ context.Context, keyID string) (*HMACKeyMaterial, error) { // Build a validator over one static in-memory HMAC key so the test can focus on nonce replay behavior.
		return &HMACKeyMaterial{KeyID: keyID, SubjectID: "subject-1", TenantID: "tenant-1", Secret: "super-secret", Status: "active"}, nil // Return the authoritative key material expected by the validator for the supplied key ID.
	}, &memoryNonceStore{}, 5*time.Minute, 15*time.Minute) // Provide replay protection and explicit timing values so the duplicate nonce path is active.

	signatureTimestamp := fmt.Sprintf("%d", time.Now().UTC().Unix())                                                                                                      // Capture one current timestamp so both requests fall inside the configured signature window.
	signature := mustComputeTestSignature(t, "super-secret", "POST", "/api/v1/sessions", "", `{"ok":true}`, signatureTimestamp, "nonce-dup", "key-1")                     // Compute one valid signature that both requests will reuse to exercise nonce replay detection.
	if _, err := validator.Verify(context.Background(), signature, "POST", "/api/v1/sessions", "", `{"ok":true}`, signatureTimestamp, "nonce-dup", "key-1"); err != nil { // Verify the first request so the nonce store records the nonce marker successfully.
		t.Fatalf("expected first HMAC verification to succeed, got error: %v", err) // Surface the unexpected failure of the initial non-replayed request.
	}
	_, err := validator.Verify(context.Background(), signature, "POST", "/api/v1/sessions", "", `{"ok":true}`, signatureTimestamp, "nonce-dup", "key-1") // Re-run the same request so the nonce replay path is exercised against the same store.
	if !errors.IsCode(err, errors.CodeNonceDuplicate) {                                                                                                  // Fail the test when the validator does not classify the replay as a nonce-duplicate error.
		t.Fatalf("expected duplicate nonce error code %q, got %v", errors.CodeNonceDuplicate, err) // Surface the unexpected replay classification together with the returned error.
	}
}

// mustComputeTestSignature computes the canonical HMAC signature used by the tests and fails the test immediately when canonicalization fails.
func mustComputeTestSignature(t *testing.T, secret string, method string, path string, rawQuery string, body string, timestamp string, nonce string, keyID string) string {
	t.Helper()                                         // Mark this helper so any failure points at the calling test rather than the helper body.
	canonicalQuery, err := canonicalizeQuery(rawQuery) // Canonicalize the raw query string with the same helper used by the production validator.
	if err != nil {                                    // Stop immediately when the raw query cannot be canonicalized because the expected test signature would be meaningless.
		t.Fatalf("failed to canonicalize test query %q: %v", rawQuery, err) // Surface the canonicalization failure together with the raw query string.
	}
	bodyHash := sha256.Sum256([]byte(body))                                                                                   // Hash the body exactly as the production validator does before building the canonical request string.
	message := fmt.Sprintf("v1\n%s\n%s\n%s\n%x\n%s\n%s\n%s", method, path, canonicalQuery, bodyHash, timestamp, nonce, keyID) // Build the canonical request string in the same field order used by the production validator.
	mac := hmac.New(sha256.New, []byte(secret))                                                                               // Initialize the SHA-256 HMAC over the supplied shared secret.
	if _, err := mac.Write([]byte(message)); err != nil {                                                                     // Feed the canonical request string into the HMAC calculator to derive the expected signature bytes.
		t.Fatalf("failed to write canonical request into HMAC calculator: %v", err) // Surface the unexpected HMAC write failure together with the returned error.
	}
	return hex.EncodeToString(mac.Sum(nil)) // Return the lowercase-hex signature expected by the production validator.
}

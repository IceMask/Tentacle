// hmac.go implements canonical-request HMAC validation for gateway HTTP requests.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/util"
)

const (
	defaultHMACWindow   = 5 * time.Minute  // defaultHMACWindow bounds acceptable request timestamp skew when the caller does not configure a custom window.
	defaultHMACNonceTTL = 15 * time.Minute // defaultHMACNonceTTL bounds replay-protection marker lifetime when the caller does not configure a custom nonce TTL.
)

// HMACKeyMaterial stores the authoritative HMAC key metadata needed to verify one signed request.
type HMACKeyMaterial struct {
	KeyID     string
	TenantID  string
	SubjectID string
	Secret    string
	Status    string
	NotBefore *time.Time
	NotAfter  *time.Time
}

// HMACKeyLookupFunc resolves one HMAC key ID to its authoritative verification material using the caller's request context.
type HMACKeyLookupFunc func(ctx context.Context, keyID string) (*HMACKeyMaterial, error)

// HMACValidator validates canonical-request HMAC signatures and enforces replay-protection checks.
type HMACValidator struct {
	keyLookup  HMACKeyLookupFunc
	nonceStore util.IdempotencyStore
	window     time.Duration
	nonceTTL   time.Duration
}

// NewHMACValidator constructs one HMAC validator over the supplied key lookup and replay-protection store.
func NewHMACValidator(keyLookup HMACKeyLookupFunc, nonceStore util.IdempotencyStore, window time.Duration, nonceTTL time.Duration) *HMACValidator {
	if window <= 0 { // Fill in the default skew window when the caller leaves it unset so runtime verification always has a bounded time contract.
		window = defaultHMACWindow // Preserve the repository default HMAC signature window for callers that do not configure a custom value.
	}
	if nonceTTL <= 0 { // Fill in the default nonce TTL when the caller leaves it unset so replay protection always expires stale markers eventually.
		nonceTTL = defaultHMACNonceTTL // Preserve the repository default HMAC nonce TTL for callers that do not configure a custom value.
	}

	return &HMACValidator{keyLookup: keyLookup, nonceStore: nonceStore, window: window, nonceTTL: nonceTTL} // Store the lookup, nonce store, and timing contracts once so every request uses the same startup-built verifier.
}

// Verify authenticates one HMAC-signed HTTP request by validating the timestamp, nonce, key state, and canonical request signature.
func (v *HMACValidator) Verify(ctx context.Context, sig string, method string, path string, rawQuery string, body string, ts string, nonce string, keyID string) (*Subject, error) {
	if v == nil || v.keyLookup == nil { // Reject calls on an unconfigured validator because request-signature validation cannot proceed without key lookup material.
		return nil, errors.New(errors.CodeUnauthenticated, "hmac validator is not configured") // Surface the missing HMAC source as an authentication failure instead of panicking on a nil function.
	}

	timestamp, err := strconv.ParseInt(strings.TrimSpace(ts), 10, 64) // Parse the caller-supplied timestamp so request skew can be validated against the configured signature window.
	if err != nil {                                                   // Reject malformed timestamps before any expensive lookup or hashing work begins.
		return nil, errors.New(errors.CodeUnauthenticated, "invalid timestamp") // Surface malformed timestamps as standard authentication failures.
	}
	requestTime := time.Unix(timestamp, 0).UTC()  // Normalize the request time into UTC so skew checks behave consistently across environments.
	if time.Since(requestTime).Abs() > v.window { // Reject requests whose signed timestamp falls outside the configured skew window.
		return nil, errors.New(errors.CodeUnauthenticated, "timestamp out of window") // Surface stale or far-future requests as standard authentication failures.
	}

	keyMaterial, err := v.keyLookup(ctx, strings.TrimSpace(keyID)) // Resolve the authoritative HMAC key material so runtime validation can enforce status and validity windows.
	if err != nil {                                                // Stop immediately when the HMAC key ID is unknown or the lookup source fails.
		if errors.IsCode(err, errors.CodeStoreRead) || errors.IsCode(err, errors.CodeStoreConn) { // Preserve source availability failures instead of misclassifying them as bad credentials.
			return nil, err // Let the HTTP layer fail closed with a server error while keeping backend details private.
		}
		return nil, errors.Wrap(errors.CodeUnauthenticated, "key not found", err) // Preserve the source failure while returning the stable unauthenticated code.
	}
	if keyMaterial == nil { // Reject a lookup that succeeds without authoritative key material because signature verification cannot proceed safely.
		return nil, errors.New(errors.CodeUnauthenticated, "key not found") // Fail closed without dereferencing a nil lookup result.
	}
	if !strings.EqualFold(strings.TrimSpace(keyMaterial.Status), "active") { // Accept only the explicit active lifecycle state so unknown or future states fail closed.
		return nil, errors.New(errors.CodeUnauthenticated, "hmac key is not active") // Surface inactive keys as standard authentication failures.
	}
	if keyMaterial.NotBefore != nil && requestTime.Before(keyMaterial.NotBefore.UTC()) { // Reject requests signed before the key validity window begins.
		return nil, errors.New(errors.CodeUnauthenticated, "hmac key not yet valid") // Surface early key use as a standard authentication failure.
	}
	if keyMaterial.NotAfter != nil && requestTime.After(keyMaterial.NotAfter.UTC()) { // Reject requests signed after the key validity window ends.
		return nil, errors.New(errors.CodeTokenExpired, "hmac key expired") // Surface expired keys distinctly so operators can recognize rotation issues quickly.
	}

	canonicalQuery, err := canonicalizeQuery(rawQuery) // Normalize the request query string so client and server signature inputs stay deterministic.
	if err != nil {                                    // Reject malformed raw queries before signature comparison because canonicalization would be ambiguous.
		return nil, errors.Wrap(errors.CodeUnauthenticated, "invalid canonical query", err) // Surface malformed signed URLs as standard authentication failures.
	}
	bodyHash := sha256.Sum256([]byte(body))                                                                                                                                                                                            // Hash the exact HTTP body bytes once so the canonical request covers the payload deterministically.
	message := fmt.Sprintf("v1\n%s\n%s\n%s\n%x\n%s\n%s\n%s", strings.ToUpper(strings.TrimSpace(method)), strings.TrimSpace(path), canonicalQuery, bodyHash, strings.TrimSpace(ts), strings.TrimSpace(nonce), strings.TrimSpace(keyID)) // Build the canonical request string defined by v4.4 so query parameters participate in HMAC verification.

	mac := hmac.New(sha256.New, []byte(keyMaterial.Secret))                                // Initialize the SHA-256 HMAC with the authoritative shared secret resolved for the presented key ID.
	_, _ = mac.Write([]byte(message))                                                      // Feed the canonical request string into the HMAC calculator to derive the expected signature bytes.
	expectedSig := hex.EncodeToString(mac.Sum(nil))                                        // Normalize the computed signature into lowercase hex so comparison matches the on-wire format.
	if !hmac.Equal([]byte(strings.ToLower(strings.TrimSpace(sig))), []byte(expectedSig)) { // Compare the caller-supplied signature against the authoritative computed digest in constant time.
		return nil, errors.New(errors.CodeUnauthenticated, "invalid signature") // Surface mismatched HMAC digests as standard authentication failures.
	}

	if v.nonceStore != nil { // Consume replay state only after the key and signature prove that the caller holds the shared secret.
		nonceKey := "nonce:" + strings.TrimSpace(keyID) + ":" + strings.TrimSpace(nonce) // Scope the replay marker by key ID so distinct credentials can reuse the same nonce safely.
		acquired, _, err := v.nonceStore.CheckAndSet(ctx, nonceKey, "1", v.nonceTTL)     // Store the authenticated replay marker with the configured expiry.
		if err != nil {                                                                  // Stop when replay storage cannot enforce uniqueness for the otherwise valid request.
			return nil, errors.Wrap(errors.CodeInternal, "failed to check nonce", err) // Fail closed because accepting without shared replay state would weaken the signature contract.
		}
		if !acquired { // Reject duplicate nonce usage because a prior authenticated request already consumed this marker.
			return nil, errors.New(errors.CodeNonceDuplicate, "nonce reused") // Surface replayed requests through the dedicated nonce-duplicate error code.
		}
	}

	subjectID := strings.TrimSpace(keyMaterial.SubjectID) // Normalize the authoritative subject ID so downstream handlers receive a stable principal identifier.
	if subjectID == "" {                                  // Fall back to the key ID only when the source does not supply a distinct subject identifier.
		subjectID = strings.TrimSpace(keyMaterial.KeyID) // Preserve a stable authenticated subject identifier even for legacy static HMAC setups without explicit subject data.
	}

	return &Subject{ID: subjectID, Type: "hmac", CredentialID: strings.TrimSpace(keyMaterial.KeyID), TenantID: strings.TrimSpace(keyMaterial.TenantID)}, nil // Return the authenticated HMAC subject with its safe key identifier for downstream authorization and audit.
}

// canonicalizeQuery normalizes one raw HTTP query string into the deterministic encoding used by the canonical HMAC request format.
func canonicalizeQuery(rawQuery string) (string, error) {
	if strings.TrimSpace(rawQuery) == "" { // Treat empty queries as the empty canonical query string because the request path alone already identifies the resource.
		return "", nil // Return the empty canonical query string so client and server can both omit query material deterministically.
	}

	values, err := url.ParseQuery(rawQuery) // Parse the raw query string so keys and values can be sorted and percent-encoded deterministically.
	if err != nil {                         // Stop immediately when the raw query is malformed because canonicalization would otherwise be ambiguous.
		return "", err // Preserve the low-level parse error so the caller can wrap it into an authentication failure.
	}

	return values.Encode(), nil // Return the standard-library encoded query because Encode sorts keys and values into a stable canonical representation.
}

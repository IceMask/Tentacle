// pat.go implements Personal Access Token validation for bearer tokens that follow the repository PAT wire format.
package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"mcp_for_appium/internal/errors"
)

// PATRecord stores the authoritative PAT metadata needed to validate one presented token ID.
type PATRecord struct {
	TokenID    string
	SubjectID  string
	TenantID   string
	SecretHash string
	Status     string
	ScopesJSON []byte
	ExpiresAt  *time.Time
}

// PATLookupFunc resolves one PAT token ID to its authoritative metadata using the caller's request context.
type PATLookupFunc func(ctx context.Context, tokenID string) (*PATRecord, error)

// PATValidator validates bearer PATs against one configured lookup source.
type PATValidator struct {
	lookup PATLookupFunc
}

// NewPATValidator constructs one PAT validator over the supplied lookup function.
func NewPATValidator(lookup PATLookupFunc) *PATValidator {
	return &PATValidator{lookup: lookup} // Store the lookup closure once so every request can resolve PAT metadata through the same startup-built source.
}

// Validate authenticates one PAT bearer token and returns the resulting authenticated subject metadata.
func (v *PATValidator) Validate(ctx context.Context, token string) (*Subject, error) {
	if v == nil || v.lookup == nil { // Reject calls on an unconfigured validator because PAT authentication cannot proceed without a lookup source.
		return nil, errors.New(errors.CodeUnauthenticated, "pat validator is not configured") // Surface the missing PAT source as an authentication failure instead of panicking on a nil function.
	}

	tokenID, secret, err := splitPATToken(token) // Parse the presented PAT into its token ID and secret components before any lookup occurs.
	if err != nil {                              // Stop immediately when the PAT wire format is malformed.
		return nil, err // Preserve the PAT format error so the caller can return a stable unauthenticated response.
	}

	record, err := v.lookup(ctx, tokenID) // Resolve the authoritative PAT metadata by token ID so validation can enforce status, expiry, and secret hash.
	if err != nil {                       // Stop immediately when the PAT ID is unknown or the lookup source fails.
		if errors.IsCode(err, errors.CodeStoreRead) || errors.IsCode(err, errors.CodeStoreConn) { // Preserve source availability failures instead of misclassifying them as bad credentials.
			return nil, err // Let the HTTP layer fail closed with a server error while keeping backend details private.
		}
		return nil, errors.Wrap(errors.CodeUnauthenticated, "token not found", err) // Preserve the source failure while returning the stable unauthenticated code.
	}
	if record == nil { // Reject a lookup that succeeds without authoritative PAT metadata because validation cannot continue safely.
		return nil, errors.New(errors.CodeUnauthenticated, "token not found") // Fail closed without dereferencing a nil lookup result.
	}
	if !strings.EqualFold(strings.TrimSpace(record.Status), "active") { // Accept only the explicit active lifecycle state so unknown, revoked, or future states fail closed.
		return nil, errors.New(errors.CodeUnauthenticated, "token is not active") // Surface every inactive PAT through one stable authentication failure.
	}
	if record.ExpiresAt != nil && time.Now().UTC().After(record.ExpiresAt.UTC()) { // Reject expired PATs before the secret hash comparison because expired credentials must never authenticate again.
		return nil, errors.New(errors.CodeTokenExpired, "token expired") // Surface PAT expiry distinctly so callers can distinguish expiry from bad secrets.
	}

	secretHash := sha256.Sum256([]byte(secret))                                                             // Hash the presented secret exactly once so it can be compared against the stored authoritative digest.
	encodedHash := hex.EncodeToString(secretHash[:])                                                        // Normalize the presented secret hash into the same lowercase hex format stored by the source of truth.
	if subtle.ConstantTimeCompare([]byte(encodedHash), []byte(strings.TrimSpace(record.SecretHash))) != 1 { // Compare equal-length hexadecimal digests without data-dependent early exit.
		return nil, errors.New(errors.CodeUnauthenticated, "invalid token secret") // Surface secret mismatches as standard authentication failures.
	}

	scopes := decodePATScopes(record.ScopesJSON)     // Decode any stored PAT scopes so downstream handlers can enforce authorization decisions consistently.
	subjectID := strings.TrimSpace(record.SubjectID) // Normalize the stored subject ID so auth context consumers receive a stable principal identifier.
	if subjectID == "" {                             // Fall back to the token ID only when the source does not supply a distinct subject identifier.
		subjectID = record.TokenID // Preserve a stable authenticated subject identifier even for legacy static PAT records that do not store subject IDs explicitly.
	}

	return &Subject{ID: subjectID, Type: "pat", CredentialID: strings.TrimSpace(record.TokenID), TenantID: strings.TrimSpace(record.TenantID), Scopes: scopes}, nil // Return the authenticated PAT subject with its safe token identifier for downstream authorization and audit.
}

// splitPATToken parses one PAT bearer token in the repository wire format and returns the token ID plus secret components.
func splitPATToken(token string) (string, string, error) {
	trimmedToken := strings.TrimSpace(token)         // Normalize surrounding whitespace so bearer parsing behaves consistently across transports.
	if !strings.HasPrefix(trimmedToken, "mcp_v1_") { // Reject tokens without the expected PAT prefix because bearer routing relies on a stable PAT token shape.
		return "", "", errors.New(errors.CodeUnauthenticated, "invalid token format") // Surface unsupported bearer shapes as standard authentication failures.
	}

	prefixLength := len("mcp_v1_")                                    // Capture the fixed prefix length once so the token body can be parsed without repeated string literals.
	separatorIndex := strings.Index(trimmedToken[prefixLength:], "_") // Find the separator between token ID and secret inside the PAT body.
	if separatorIndex < 0 {                                           // Reject missing separators because PAT validation requires both an ID and a secret segment.
		return "", "", errors.New(errors.CodeUnauthenticated, "invalid token format") // Surface malformed PAT bodies as standard authentication failures.
	}

	tokenID := strings.TrimSpace(trimmedToken[prefixLength : prefixLength+separatorIndex]) // Extract the token ID segment so the validator can look up the authoritative record.
	secret := strings.TrimSpace(trimmedToken[prefixLength+separatorIndex+1:])              // Extract the PAT secret segment so the validator can compare its SHA-256 hash.
	if tokenID == "" || secret == "" {                                                     // Reject empty segments because PAT validation requires both a lookup ID and a secret.
		return "", "", errors.New(errors.CodeUnauthenticated, "invalid token format") // Surface missing PAT segments as standard authentication failures.
	}

	return tokenID, secret, nil // Return the parsed PAT components so the validator can resolve the token record and compare the secret hash.
}

// decodePATScopes converts one stored PAT scope payload into a string slice while tolerating empty legacy records.
func decodePATScopes(raw []byte) []string {
	if len(raw) == 0 { // Treat empty scope payloads as an empty scope set because legacy static PAT records do not carry structured scopes.
		return nil // Return a nil scope slice so downstream handlers can distinguish "no scopes recorded" from a non-empty scope set.
	}

	var scopes []string                                  // Allocate the destination slice lazily because many PAT records may not store scopes at all.
	if err := json.Unmarshal(raw, &scopes); err != nil { // Ignore malformed stored scopes defensively so one bad record does not panic the gateway auth path.
		return nil // Return an empty scope set when stored PAT scopes are malformed because authentication should still remain deterministic.
	}

	return scopes // Return the decoded PAT scopes for downstream authorization decisions.
}

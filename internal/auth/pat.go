package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"mcp_for_appium/internal/errors"
)

// HashLookup defines the interface to look up token hashes.
type HashLookup interface {
	// Lookup returns the stored hash and metadata for a given token ID.
	// Returns error if not found.
	Lookup(tokenID string) (hash string, tenantID string, revoked bool, err error)
}

type PATValidator struct {
	lookup HashLookup
}

// NewPATValidator executes this operation.
func NewPATValidator(lookup HashLookup) *PATValidator {
	return &PATValidator{lookup: lookup}
}

// Validate validates a PAT string "mcp_v1_..."
func (v *PATValidator) Validate(token string) (*Subject, error) {
	// Format: mcp_v1_<id>_<secret>
	parts := strings.Split(token, "_")
	if len(parts) < 4 {
		return nil, errors.New(errors.CodeUnauthenticated, "invalid token format")
	}

	// Assuming id is part 2 (index 2) and secret is part 3?
	// Let's assume standard format: prefix_version_id_secret
	// mcp_v1_id123_secret456
	id := parts[2]
	secret := parts[3]

	storedHash, tenantID, revoked, err := v.lookup.Lookup(id)
	if err != nil {
		return nil, errors.Wrap(errors.CodeUnauthenticated, "token not found", err)
	}

	if revoked {
		return nil, errors.New(errors.CodeUnauthenticated, "token revoked")
	}

	// Verify hash
	hash := sha256.Sum256([]byte(secret))
	encodedHash := hex.EncodeToString(hash[:])

	if encodedHash != storedHash {
		return nil, errors.New(errors.CodeUnauthenticated, "invalid token secret")
	}

	return &Subject{
		ID:       id,
		Type:     "pat",
		TenantID: tenantID,
	}, nil
}

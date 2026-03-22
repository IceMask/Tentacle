// auth_store.go exposes PostgreSQL-backed lookup helpers for PAT and HMAC authentication sources used by the gateway.
package postgres

import (
	"context"
	"database/sql"

	"mcp_for_appium/internal/errors"
)

// PATToken stores one PostgreSQL-backed PAT credential record resolved by token ID.
type PATToken struct {
	TokenID     string
	TenantID    string
	SubjectID   string
	DisplayName string
	SecretHash  string
	Status      string
	ScopesJSON  []byte
	ExpiresAt   sql.NullTime
}

// HMACKey stores one PostgreSQL-backed HMAC key record resolved by key ID.
type HMACKey struct {
	KeyID       string
	TenantID    string
	DisplayName string
	SecretRef   string
	Status      string
	NotBefore   sql.NullTime
	NotAfter    sql.NullTime
}

// GetPATToken loads one PAT record from PostgreSQL so gateway auth can validate one presented token ID against the authoritative source.
func (d *DAO) GetPATToken(ctx context.Context, tokenID string) (*PATToken, error) {
	record := &PATToken{} // Allocate the destination record once so the query scan can populate the full PAT row in one round trip.
	err := d.pool.QueryRow(ctx, `
		SELECT token_id, tenant_id, subject_id, display_name, secret_hash, status, scopes_json, expires_at
		FROM pat_tokens
		WHERE token_id = $1
	`, tokenID).Scan(&record.TokenID, &record.TenantID, &record.SubjectID, &record.DisplayName, &record.SecretHash, &record.Status, &record.ScopesJSON, &record.ExpiresAt) // Read the authoritative PAT row by token ID because PAT bearer validation starts from the parsed token identifier.
	if err != nil { // Convert missing-row and database failures into the repository-standard storage read contract.
		return nil, errors.Wrap(errors.CodeUnauthenticated, "pat token not found", err) // Preserve the raw lookup failure while returning a stable authentication-oriented code to the caller.
	}

	return record, nil // Return the authoritative PAT record so the caller can validate status, expiry, and secret hash.
}

// GetHMACKey loads one HMAC key record from PostgreSQL so gateway auth can validate one presented key ID against the authoritative source.
func (d *DAO) GetHMACKey(ctx context.Context, keyID string) (*HMACKey, error) {
	record := &HMACKey{} // Allocate the destination record once so the query scan can populate the full HMAC key row in one round trip.
	err := d.pool.QueryRow(ctx, `
		SELECT key_id, tenant_id, display_name, secret_ref, status, not_before, not_after
		FROM hmac_keys
		WHERE key_id = $1
	`, keyID).Scan(&record.KeyID, &record.TenantID, &record.DisplayName, &record.SecretRef, &record.Status, &record.NotBefore, &record.NotAfter) // Read the authoritative HMAC key row by key ID because request-signature validation starts from the caller-supplied key identifier.
	if err != nil { // Convert missing-row and database failures into the repository-standard storage read contract.
		return nil, errors.Wrap(errors.CodeUnauthenticated, "hmac key not found", err) // Preserve the raw lookup failure while returning a stable authentication-oriented code to the caller.
	}

	return record, nil // Return the authoritative HMAC key record so the caller can validate status, validity window, and secret material.
}

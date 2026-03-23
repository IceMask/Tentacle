// static_pat_lookup.go parses and serves statically configured PAT hash records for gateway authentication wiring.
package auth

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// StaticPATRecord stores one parsed PAT hash entry together with tenant and revocation metadata.
type StaticPATRecord struct {
	Hash       string
	SubjectID  string
	TenantID   string
	Status     string
	ScopesJSON []byte
}

// StaticPATLookup implements HashLookup using statically configured PAT metadata loaded at process startup.
type StaticPATLookup struct {
	records map[string]StaticPATRecord
}

// NewStaticPATLookup parses static PAT records encoded as "hash|tenantID|revoked" values keyed by token ID.
func NewStaticPATLookup(raw map[string]string) (*StaticPATLookup, error) {
	records := make(map[string]StaticPATRecord, len(raw)) // Allocate the parsed record map once so every configured PAT entry can be normalized during startup.
	for tokenID, encodedRecord := range raw {             // Walk each configured PAT entry so malformed records fail fast before the gateway starts serving traffic.
		normalizedTokenID := strings.TrimSpace(tokenID) // Trim surrounding whitespace so token IDs loaded from YAML or environment variables compare consistently at runtime.
		if normalizedTokenID == "" {                    // Reject empty token IDs because PAT validation needs a stable lookup key extracted from the bearer token.
			return nil, fmt.Errorf("static PAT token id must not be empty") // Surface the malformed token configuration immediately so operators can fix the exact record.
		}
		record, err := parseStaticPATRecord(encodedRecord) // Parse the encoded record into structured hash, tenant, and revocation fields once during startup.
		if err != nil {                                    // Stop startup when any configured PAT record is malformed because partial auth activation would be misleading and unsafe.
			return nil, fmt.Errorf("invalid static PAT record for %q: %w", normalizedTokenID, err) // Keep the token ID in the error so the broken entry can be identified quickly.
		}
		records[normalizedTokenID] = record // Store the parsed record under the normalized token ID for constant-time runtime validation.
	}

	return &StaticPATLookup{records: records}, nil // Return the fully parsed lookup once every configured PAT record has been validated successfully.
}

// Lookup returns the stored PAT record for the provided token ID using the new request-context-aware lookup signature.
func (l *StaticPATLookup) Lookup(_ context.Context, tokenID string) (*PATRecord, error) {
	record, ok := l.records[tokenID] // Resolve the PAT record directly from the parsed in-memory map so runtime validation stays deterministic and fast.
	if !ok {                         // Reject unknown token IDs so callers receive a consistent authentication failure for missing PAT records.
		return nil, fmt.Errorf("token id %q not found", tokenID) // Surface the missing token ID to the caller so it can be wrapped into an auth error upstream.
	}

	return &PATRecord{TokenID: tokenID, SubjectID: record.SubjectID, TenantID: record.TenantID, SecretHash: record.Hash, Status: record.Status, ScopesJSON: record.ScopesJSON}, nil // Return the normalized PAT metadata in the v4.4 runtime shape expected by PAT validation.
}

// parseStaticPATRecord decodes one static PAT record encoded as "hash|tenantID|status".
func parseStaticPATRecord(rawRecord string) (StaticPATRecord, error) {
	parts := strings.Split(rawRecord, "|") // Split the encoded record on pipe separators so the loader can support a compact env-friendly representation.
	if len(parts) == 0 {                   // Defend against an impossible-but-unsafe empty split result before any index access below.
		return StaticPATRecord{}, fmt.Errorf("record is empty") // Surface the malformed record instead of allowing a later index panic.
	}

	hash := strings.TrimSpace(parts[0]) // Normalize the stored SHA-256 hex digest so surrounding whitespace never breaks token validation.
	if hash == "" {                     // Reject empty hashes because PAT validation cannot compare an empty digest safely.
		return StaticPATRecord{}, fmt.Errorf("hash is required") // Surface the missing hash to the caller so startup fails with a precise message.
	}
	if len(hash) != 64 { // Enforce the expected SHA-256 hex length so misconfigured plaintext or truncated values are rejected up front.
		return StaticPATRecord{}, fmt.Errorf("hash must be a 64-character lowercase hex SHA-256 digest") // Describe the required hash format so the operator can correct the record immediately.
	}
	if _, err := hex.DecodeString(hash); err != nil { // Decode the supplied digest once to prove it is valid hexadecimal before authentication starts.
		return StaticPATRecord{}, fmt.Errorf("hash must be valid lowercase hex: %w", err) // Preserve the decode failure because it pinpoints the malformed digest contents.
	}

	record := StaticPATRecord{Hash: hash, Status: "active"} // Seed the parsed record with the required digest and default active status because later fields are optional.
	if len(parts) >= 2 {                                    // Parse the optional tenant ID only when it is present in the encoded record.
		record.TenantID = strings.TrimSpace(parts[1]) // Normalize the tenant ID so auth context consumers see a stable value.
	}
	if len(parts) >= 3 { // Parse the optional revoked marker only when the record explicitly supplies it.
		statusValue := strings.TrimSpace(parts[2])                        // Normalize the status marker so common YAML and env formatting still parse consistently.
		if strings.EqualFold(statusValue, "true") || statusValue == "1" { // Preserve compatibility with the old revoked boolean marker when legacy values still use true/1.
			record.Status = "revoked" // Translate the legacy revoked marker into the new PAT status field so runtime validation stays consistent.
		} else if statusValue != "" { // Accept explicit v4.4-style status values when the static PAT record already uses them.
			record.Status = statusValue // Preserve the configured status value so operators can use active or revoked without another translation step.
		}
	}
	if len(parts) > 3 { // Reject extra segments because additional unparsed data would make the configuration ambiguous and hard to reason about.
		return StaticPATRecord{}, fmt.Errorf("record must use format hash|tenantID|status") // Surface the accepted record shape explicitly so the configuration can be corrected quickly.
	}
	record.ScopesJSON, _ = json.Marshal([]string{}) // Store an empty JSON scope list so PAT validation sees a consistent payload shape even for legacy static records.

	return record, nil // Return the normalized PAT record once every field has been parsed successfully.
}

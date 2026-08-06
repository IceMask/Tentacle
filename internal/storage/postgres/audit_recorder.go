// audit_recorder.go adapts repository-wide structured audit events into append-only PostgreSQL audit_log rows.
package postgres

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"mcp_for_appium/internal/audit"
	"mcp_for_appium/internal/errors"

	"github.com/google/uuid"
)

// AuditRecorder persists structured security audit events through the PostgreSQL DAO.
type AuditRecorder struct {
	dao *DAO
}

// NewAuditRecorder constructs one PostgreSQL audit recorder, or nil when no DAO is available.
func NewAuditRecorder(dao *DAO) audit.Recorder {
	if dao == nil { // Skip recorder construction when persistence is not wired, such as protocol-only unit tests.
		return nil // Signal to callers that audit recording is unavailable rather than creating a panic-prone adapter.
	}
	return &AuditRecorder{dao: dao} // Reuse the process-wide DAO pool for append-only audit writes.
}

// Record serializes one structured audit event and appends it to the existing audit_logs table.
func (r *AuditRecorder) Record(ctx context.Context, event audit.Event) error {
	if r == nil || r.dao == nil { // Reject direct use of an unconfigured recorder defensively.
		return errors.New(errors.CodeStoreWrite, "audit recorder is not configured") // Preserve a stable storage classification for wiring defects.
	}
	createdAt := event.CreatedAt.UTC() // Normalize caller-supplied audit timestamps for consistent ordering.
	if createdAt.IsZero() {            // Fill timestamps for callers that omit an explicit operation time.
		createdAt = time.Now().UTC() // Stamp the record at persistence time while retaining UTC semantics.
	}
	projectID := strings.TrimSpace(event.ProjectID) // Prefer the concrete project scope when the audited resource belongs to one.
	if projectID == "" {                            // Fall back to tenant scope for authentication and gateway-only events.
		projectID = strings.TrimSpace(event.TenantID) // Keep tenant security events queryable through the existing project_id index.
	}
	if projectID == "" { // Assign a stable system scope for pre-authentication and worker lifecycle events.
		projectID = "system" // Satisfy the existing non-null audit schema without inventing a user or tenant identity.
	}
	actorID := strings.TrimSpace(event.ActorID) // Preserve the authenticated principal when one is known.
	if actorID == "" {                          // Classify pre-authentication failures without a fake credential identity.
		actorID = "anonymous" // Keep the non-null actor column explicit and searchable for rejected traffic.
	}
	resourceType := strings.TrimSpace(event.ResourceType) // Preserve the audited resource category when supplied.
	if resourceType == "" {                               // Fall back to a stable generic category for process events.
		resourceType = "system" // Satisfy the non-null schema while keeping the record semantically explicit.
	}
	resourceID := strings.TrimSpace(event.ResourceID) // Preserve the concrete resource identifier when supplied.
	if resourceID == "" {                             // Fall back to the action identifier when no separate resource exists.
		resourceID = strings.TrimSpace(event.Action) // Keep resource-oriented audit queries deterministic for action-only events.
	}
	payload, err := json.Marshal(map[string]interface{}{ // Serialize the requirement-level audit fields not represented by dedicated legacy columns.
		"actorId":      actorID,                               // Duplicate the actor identifier inside the self-contained JSON payload.
		"actorType":    strings.TrimSpace(event.ActorType),    // Record whether the actor is a user, service, worker, or anonymous requester.
		"authScheme":   strings.TrimSpace(event.AuthScheme),   // Record PAT, OIDC, HMAC, subscription token, or internal RPC authentication.
		"credentialId": strings.TrimSpace(event.CredentialID), // Record a safe key or token identifier without storing any credential secret.
		"tenantId":     strings.TrimSpace(event.TenantID),     // Record the authenticated tenant scope when available.
		"result":       strings.TrimSpace(event.Result),       // Record success, failure, denied, expired, or another stable outcome.
		"reason":       strings.TrimSpace(event.Reason),       // Record only a stable error code or sanitized lifecycle reason.
		"sourceIp":     strings.TrimSpace(event.SourceIP),     // Record the directly observed network peer for security investigation.
		"requestId":    strings.TrimSpace(event.RequestID),    // Correlate the audit event with gateway or RPC request telemetry.
		"metadata":     event.Metadata,                        // Preserve operation-specific non-secret context such as capacity, attempt, or expiry.
	})
	if err != nil { // Stop before persistence when operation metadata cannot be represented as JSON.
		return errors.Wrap(errors.CodeInternal, "failed to serialize audit event", err) // Preserve the serialization cause for server logs.
	}
	return r.dao.InsertAuditLog(ctx, &AuditLog{ // Append the normalized event through the existing persistence helper.
		ID:           uuid.NewString(),                   // Assign one globally unique audit record identifier.
		ProjectID:    projectID,                          // Store project, tenant, or system scope in the indexed legacy column.
		SessionID:    strings.TrimSpace(event.SessionID), // Preserve an optional valid session foreign-key reference.
		TraceID:      strings.TrimSpace(event.TraceID),   // Preserve an optional valid trace foreign-key reference.
		Actor:        actorID,                            // Store the normalized actor in the indexed legacy field.
		Action:       strings.TrimSpace(event.Action),    // Store the stable audited action name.
		ResourceType: resourceType,                       // Store the normalized resource category.
		ResourceID:   resourceID,                         // Store the normalized resource identifier.
		Payload:      payload,                            // Store the complete structured security context as JSON bytes.
		CreatedAt:    createdAt,                          // Store the normalized operation timestamp.
	})
}

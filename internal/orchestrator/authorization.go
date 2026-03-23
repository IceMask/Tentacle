// authorization.go enforces authenticated session and trace ownership checks inside the orchestrator service.
package orchestrator

import (
	"context"
	"strings"

	"mcp_for_appium/internal/auth"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/storage/postgres"
)

// authorizeSessionAccess loads one session row and rejects authenticated callers that do not own it.
func (s *Service) authorizeSessionAccess(ctx context.Context, sessionID string) (*postgres.Session, error) {
	session, err := s.dao.GetSession(ctx, sessionID) // Load the authoritative session row because ownership decisions must use persisted metadata rather than request input.
	if err != nil {                                  // Stop immediately when the requested session does not exist or cannot be loaded from storage.
		return nil, err // Preserve the session-not-found or storage failure so callers keep the existing transport mapping.
	}
	if err := authorizeOwnedResource(ctx, session.TenantID, session.SubjectID, "session", session.ID); err != nil { // Validate the authenticated caller against the persisted session ownership metadata before exposing the resource.
		return nil, err // Preserve the authorization failure so transports can return a stable permission-denied response.
	}

	return session, nil // Return the authorized session row so callers can reuse the loaded record without issuing a second DAO query.
}

// authorizeTraceAccess loads one trace row and rejects authenticated callers that do not own it.
func (s *Service) authorizeTraceAccess(ctx context.Context, traceID string) (*postgres.Trace, error) {
	trace, err := s.dao.GetTrace(ctx, traceID) // Load the authoritative trace row because ownership decisions must use persisted metadata rather than request input.
	if err != nil {                            // Stop immediately when the requested trace does not exist or cannot be loaded from storage.
		return nil, err // Preserve the trace-not-found or storage failure so callers keep the existing transport mapping.
	}
	if err := authorizeOwnedResource(ctx, trace.TenantID, trace.SubjectID, "trace", trace.ID); err != nil { // Validate the authenticated caller against the persisted trace ownership metadata before exposing the resource.
		return nil, err // Preserve the authorization failure so transports can return a stable permission-denied response.
	}

	return trace, nil // Return the authorized trace row so callers can reuse the loaded record without issuing a second DAO query.
}

// authorizeOwnedResource compares one authenticated subject against persisted tenant and subject ownership metadata.
func authorizeOwnedResource(ctx context.Context, tenantID string, subjectID string, resourceType string, resourceID string) error {
	subject, ok := auth.SubjectFrom(ctx) // Read the authenticated subject from the current request context so authorization only applies when the gateway has already authenticated the caller.
	if !ok || subject == nil {           // Allow access when no authenticated subject is attached because stdio and explicitly unauthenticated deployments do not have a resource principal to compare.
		return nil // Treat missing subject context as "authorization disabled" so local workflows stay backward compatible.
	}

	normalizedResourceTenantID := strings.TrimSpace(tenantID)                  // Normalize the persisted tenant identifier so whitespace-only values behave like missing ownership metadata.
	normalizedResourceSubjectID := strings.TrimSpace(subjectID)                // Normalize the persisted subject identifier so whitespace-only values behave like missing ownership metadata.
	normalizedSubjectTenantID := strings.TrimSpace(subject.TenantID)           // Normalize the authenticated tenant identifier before any equality comparison.
	normalizedSubjectID := strings.TrimSpace(subject.ID)                       // Normalize the authenticated subject identifier before any equality comparison.
	if normalizedResourceTenantID == "" && normalizedResourceSubjectID == "" { // Reject authenticated access when the resource has no ownership metadata because the server cannot prove the caller owns it.
		return errors.New(errors.CodePermissionDenied, resourceType+" ownership metadata is unavailable for "+resourceID) // Fail closed for authenticated access so trace and session subscriptions cannot bypass resource checks on legacy rows.
	}
	if normalizedResourceSubjectID != "" && normalizedSubjectID != normalizedResourceSubjectID { // Reject callers whose authenticated subject identifier does not match the persisted exact owner.
		return errors.New(errors.CodePermissionDenied, resourceType+" is not owned by the authenticated subject") // Surface the subject mismatch as a stable permission-denied error.
	}
	if normalizedResourceTenantID != "" && normalizedSubjectTenantID != "" && normalizedSubjectTenantID != normalizedResourceTenantID { // Reject callers whose tenant differs from the persisted resource tenant when both sides provide tenant metadata.
		return errors.New(errors.CodePermissionDenied, resourceType+" belongs to a different tenant") // Surface the tenant mismatch as a stable permission-denied error.
	}
	if normalizedResourceTenantID != "" && normalizedResourceSubjectID == "" && normalizedSubjectTenantID == "" { // Reject callers without tenant metadata when the resource is only tenant-scoped because the server cannot prove same-tenant access.
		return errors.New(errors.CodePermissionDenied, resourceType+" tenant cannot be verified for the authenticated subject") // Fail closed when tenant-scoped ownership exists but the caller does not present a tenant identifier.
	}

	return nil // Allow access once every persisted ownership field that is present matches the authenticated subject context.
}

// normalizedSubjectTenantID returns the authenticated tenant identifier stored in one subject, or the empty string when no subject is present.
func normalizedSubjectTenantID(subject *auth.Subject) string {
	if subject == nil { // Return the empty tenant identifier when no authenticated subject is present on the current request context.
		return "" // Preserve unauthenticated and stdio flows by not inventing ownership metadata when no principal exists.
	}

	return strings.TrimSpace(subject.TenantID) // Normalize the authenticated tenant identifier so persisted ownership metadata never stores stray whitespace.
}

// normalizedSubjectID returns the authenticated subject identifier stored in one subject, or the empty string when no subject is present.
func normalizedSubjectID(subject *auth.Subject) string {
	if subject == nil { // Return the empty subject identifier when no authenticated subject is present on the current request context.
		return "" // Preserve unauthenticated and stdio flows by not inventing ownership metadata when no principal exists.
	}

	return strings.TrimSpace(subject.ID) // Normalize the authenticated subject identifier so persisted ownership metadata never stores stray whitespace.
}

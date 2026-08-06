// recorder.go defines the repository-wide structured security audit event and pluggable recording contract.
package audit

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"
)

// Event captures actor, credential, tenant, action, resource, outcome, request, and metadata fields required for one append-only security audit record.
type Event struct {
	ProjectID    string
	SessionID    string
	TraceID      string
	ActorID      string
	ActorType    string
	AuthScheme   string
	CredentialID string
	TenantID     string
	Action       string
	ResourceType string
	ResourceID   string
	Result       string
	Reason       string
	SourceIP     string
	RequestID    string
	CreatedAt    time.Time
	Metadata     map[string]interface{}
}

// Recorder appends structured audit events to one authoritative sink.
type Recorder interface {
	Record(ctx context.Context, event Event) error
}

// RecorderFunc adapts one function into the Recorder interface.
type RecorderFunc func(ctx context.Context, event Event) error

type requestContextKey struct{}

// RequestContext stores sanitized gateway request correlation fields for downstream audit producers.
type RequestContext struct {
	RequestID string
	SourceIP  string
}

// Record invokes the adapted audit function with the supplied event.
func (f RecorderFunc) Record(ctx context.Context, event Event) error {
	return f(ctx, event) // Delegate recording to the wrapped function so tests and lightweight integrations can inject a sink directly.
}

// WithRequestContext attaches sanitized request correlation metadata for downstream audit events such as WebSocket token issuance.
func WithRequestContext(ctx context.Context, requestContext RequestContext) context.Context {
	return context.WithValue(ctx, requestContextKey{}, requestContext) // Preserve request correlation without attaching headers, credentials, or body data.
}

// RequestContextFrom returns sanitized request correlation metadata when an upstream gateway middleware attached it.
func RequestContextFrom(ctx context.Context) (RequestContext, bool) {
	if ctx == nil { // Reject nil contexts defensively because context.Value cannot be called safely on nil.
		return RequestContext{}, false // Report that no request metadata is available.
	}
	requestContext, ok := ctx.Value(requestContextKey{}).(RequestContext) // Read the exact private context key type used by WithRequestContext.
	return requestContext, ok                                             // Return the stored metadata together with an explicit presence marker.
}

// RequestSourceIP extracts one normalized peer IP from an HTTP request without trusting forwarding headers implicitly.
func RequestSourceIP(r *http.Request) string {
	if r == nil { // Return an empty source when no request metadata exists.
		return "" // Avoid dereferencing a nil request in failure-path audit code.
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr)) // Parse the ordinary host:port peer address supplied by net/http.
	if err == nil {                                                    // Return only the host when the peer address includes a valid port.
		return strings.TrimSpace(host) // Normalize the extracted peer IP before persistence.
	}
	return strings.TrimSpace(r.RemoteAddr) // Preserve a port-less test or proxy peer address without trusting caller-controlled headers.
}

// ErrorReason returns one stable internal error code for audit classification or a generic failure marker for untyped errors.
func ErrorReason(err error, codeLookup func(error) (string, bool)) string {
	if err == nil { // Successful operations have no failure reason.
		return "" // Keep success audit records free of misleading reason text.
	}
	if codeLookup != nil { // Prefer a caller-supplied stable error-code extractor when available.
		if code, ok := codeLookup(err); ok && strings.TrimSpace(code) != "" { // Record only a non-empty stable code.
			return strings.TrimSpace(code) // Avoid persisting wrapped backend error text in the audit record.
		}
	}
	return "unclassified_failure" // Use a stable generic marker for failures without a repository error code.
}

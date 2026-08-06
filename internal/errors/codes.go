package errors

import (
	stdErrors "errors"
	"fmt"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorCode represents a standardized internal error code.
type ErrorCode string

const (
	// Config & System
	CodeConfigMissing  ErrorCode = "E.CONFIG.MISSING"
	CodeConfigInvalid  ErrorCode = "E.CONFIG.INVALID"
	CodeConfigConflict ErrorCode = "E.CONFIG.CONFLICT"
	CodeConfigEnv      ErrorCode = "E.CONFIG.ENV.INVALID"
	CodeConfigDuration ErrorCode = "E.CONFIG.DURATION"

	// Network & TLS
	CodeNetAddrInUse ErrorCode = "E.NET.ADDRINUSE"
	CodeTLSInvalid   ErrorCode = "E.TLS.INVALID"

	// Storage & Data
	CodeStoreConn         ErrorCode = "E.STORE.CONN"
	CodeStoreRead         ErrorCode = "E.STORE.READ"
	CodeStoreWrite        ErrorCode = "E.STORE.WRITE"
	CodeStoreIndex        ErrorCode = "E.STORE.INDEX"
	CodeStorageArtifact   ErrorCode = "E.STORAGE.ARTIFACT_WRITE"
	CodeTraceNotFound     ErrorCode = "E.TRACE.NOT_FOUND"
	CodeSessionNotFound   ErrorCode = "E.SESSION.NOT_FOUND"
	CodeSessionDead       ErrorCode = "E.SESSION.DEAD"
	CodeSessionBroken     ErrorCode = "E.SESSION.BROKEN"
	CodePlanInvalid       ErrorCode = "E.PLAN.INVALID"
	CodeSchemaConflict    ErrorCode = "E.SCHEMA.CONFLICT"
	CodeSchemaInvalid     ErrorCode = "E.SCHEMA.INVALID"
	CodeCursorInvalid     ErrorCode = "E.CURSOR.INVALID"
	CodeStateConflict     ErrorCode = "E.STATE.CONFLICT"
	CodeArtifactDuplicate ErrorCode = "E.ARTIFACT.DUPLICATE"

	// Scheduling & Worker
	CodeSchedNoWorker   ErrorCode = "E.SCHED.NO_WORKER"
	CodeWorkerNotFound  ErrorCode = "E.WORKER.NOT_FOUND"
	CodeTimeoutPlan     ErrorCode = "E.TIMEOUT.PLAN"
	CodeTimeoutStep     ErrorCode = "E.TIMEOUT.STEP"
	CodeStepUnsupported ErrorCode = "E.STEP.UNSUPPORTED"

	// Appium & Automation
	CodeAppElemNotFound ErrorCode = "E.APP.ELEM_NOT_FOUND"
	CodeAppTimeout      ErrorCode = "E.APP.TIMEOUT"

	// Rate Limit & Health
	CodeRateLimited     ErrorCode = "E.RATE.LIMITED"
	CodeHealthDegraded  ErrorCode = "E.HEALTH.DEGRADED"
	CodeHealthDown      ErrorCode = "E.HEALTH.DOWN"
	CodeLogsUnavailable ErrorCode = "E.LOGS.UNAVAILABLE"
	CodeWSTooManySubs   ErrorCode = "E.WS.TOO_MANY_SUBS"
	CodeWSBackpressure  ErrorCode = "E.WS.BACKPRESSURE_DROP"

	// Auth
	CodeUnauthenticated  ErrorCode = "E.AUTH.UNAUTHENTICATED"
	CodePermissionDenied ErrorCode = "E.AUTH.PERMISSION_DENIED"
	CodeNonceDuplicate   ErrorCode = "E.AUTH.NONCE_DUPLICATE"
	CodeTokenExpired     ErrorCode = "E.AUTH.TOKEN_EXPIRED"

	// Internal
	CodeInternal ErrorCode = "E.INTERNAL"
)

// Error wraps a standard error with an internal code.
type Error struct {
	Code    ErrorCode
	Message string
	Err     error
}

// Error executes this operation.
func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// Unwrap executes this operation.
func (e *Error) Unwrap() error {
	return e.Err
}

// New creates a new Error.
func New(code ErrorCode, msg string) *Error {
	return &Error{Code: code, Message: msg}
}

// Wrap creates a new Error wrapping an existing error.
func Wrap(code ErrorCode, msg string, err error) *Error {
	return &Error{Code: code, Message: msg, Err: err}
}

// WrapPreservingCode creates a new Error that keeps the wrapped internal error code when one already exists.
func WrapPreservingCode(msg string, err error) *Error {
	if err == nil { // Fall back to a plain internal error when the caller does not provide a wrapped cause.
		return &Error{Code: CodeInternal, Message: msg} // Return a stable internal-code wrapper so callers never receive a nil error value.
	}
	if code, ok := CodeOf(err); ok { // Reuse the wrapped standardized code so higher layers preserve transport mapping semantics.
		return &Error{Code: code, Message: msg, Err: err} // Keep the original code while still adding the higher-level context message.
	}
	return &Error{Code: CodeInternal, Message: msg, Err: err} // Default to the internal code when the wrapped error is not one of the repository-standardized errors.
}

// MapToGRPC maps an internal error to a gRPC status error.
func MapToGRPC(err error) error {
	var e *Error                // Allocate a typed target so wrapped and joined repository errors preserve their transport classification.
	if !stdErrors.As(err, &e) { // Search the complete error chain instead of requiring the repository error at the outermost layer.
		return status.Error(codes.Unknown, err.Error())
	}

	var c codes.Code
	switch e.Code {
	case CodeConfigMissing, CodeConfigInvalid, CodeConfigEnv, CodeConfigDuration, CodePlanInvalid, CodeSchemaInvalid, CodeCursorInvalid:
		c = codes.InvalidArgument
	case CodeConfigConflict, CodeSchemaConflict, CodeStateConflict, CodeArtifactDuplicate, CodeNonceDuplicate:
		c = codes.AlreadyExists // Or FailedPrecondition depending on context, usually AlreadyExists for duplicates
	case CodeTraceNotFound, CodeSessionNotFound, CodeAppElemNotFound:
		c = codes.NotFound
	case CodeSessionDead, CodeSessionBroken:
		c = codes.Aborted
	case CodeSchedNoWorker, CodeLogsUnavailable, CodeHealthDown:
		c = codes.Unavailable
	case CodeTimeoutPlan, CodeAppTimeout:
		c = codes.DeadlineExceeded
	case CodeRateLimited, CodeWSTooManySubs, CodeWSBackpressure:
		c = codes.ResourceExhausted
	case CodeUnauthenticated, CodeTokenExpired:
		c = codes.Unauthenticated
	case CodePermissionDenied:
		c = codes.PermissionDenied
	case CodeStepUnsupported:
		c = codes.Unimplemented
	default:
		c = codes.Internal
	}

	return status.Error(c, e.Error())
}

// MapToHTTP maps an internal error to an HTTP status code.
func MapToHTTP(err error) int {
	var e *Error                // Allocate a typed target so wrapped and joined repository errors preserve their HTTP classification.
	if !stdErrors.As(err, &e) { // Search the complete error chain instead of requiring the repository error at the outermost layer.
		return http.StatusInternalServerError
	}

	switch e.Code {
	case CodeConfigMissing, CodeConfigInvalid, CodeConfigEnv, CodeConfigDuration, CodePlanInvalid, CodeSchemaInvalid, CodeCursorInvalid:
		return http.StatusBadRequest
	case CodeUnauthenticated, CodeTokenExpired:
		return http.StatusUnauthorized
	case CodePermissionDenied:
		return http.StatusForbidden
	case CodeTraceNotFound, CodeSessionNotFound, CodeAppElemNotFound:
		return http.StatusNotFound
	case CodeConfigConflict, CodeSchemaConflict, CodeStateConflict, CodeArtifactDuplicate, CodeNonceDuplicate:
		return http.StatusConflict
	case CodeRateLimited, CodeWSTooManySubs:
		return http.StatusTooManyRequests
	case CodeSchedNoWorker, CodeLogsUnavailable, CodeHealthDown:
		return http.StatusServiceUnavailable
	case CodeTimeoutPlan, CodeAppTimeout:
		return http.StatusGatewayTimeout
	case CodeStepUnsupported:
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}

// JSONRPCError represents a JSON-RPC 2.0 error object.
type JSONRPCError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// MapToJSONRPC maps an internal error to a JSON-RPC error.
func MapToJSONRPC(err error) *JSONRPCError {
	var e *Error                // Allocate a typed target so wrapped and joined repository errors preserve their JSON-RPC classification.
	if !stdErrors.As(err, &e) { // Search the complete error chain instead of requiring the repository error at the outermost layer.
		return &JSONRPCError{ // Return unknown error types with a stable classification and no wrapped backend text.
			Code:    -32603, // Use JSON-RPC internal error code for non-standard error wrappers.
			Message: "Internal error",
			Data: map[string]interface{}{ // Return only the stable internal classification for programmatic handling.
				"internalCode": string(CodeInternal), // Classify untyped server failures without exposing their raw text.
			},
		}
	}

	var code int
	switch e.Code {
	case CodePlanInvalid, CodeSchemaInvalid, CodeConfigInvalid, CodeConfigMissing:
		code = -32602 // Invalid params
	case CodeTraceNotFound, CodeSessionNotFound, CodeAppElemNotFound:
		code = -32004 // Not found (custom)
	case CodeUnauthenticated, CodePermissionDenied:
		code = -32001 // Auth error (custom)
	case CodeRateLimited, CodeWSTooManySubs:
		code = -32002 // Rate limited (custom)
	case CodeSchedNoWorker, CodeHealthDown:
		code = -32003 // Unavailable (custom)
	default:
		code = -32603 // Internal error
	}

	return &JSONRPCError{ // Return the mapped JSON-RPC error with only its stable internal classification.
		Code:    code,      // Keep mapped transport-level JSON-RPC code.
		Message: e.Message, // Keep concise business-facing message.
		Data: map[string]interface{}{ // Include the stable internal code without exposing the wrapped cause chain.
			"internalCode": string(e.Code), // Expose standardized internal code for programmatic handling.
		},
	}
}

// CodeOf extracts the internal error code, if present.
func CodeOf(err error) (ErrorCode, bool) {
	var e *Error                // Allocate a typed target so wrapped and joined errors expose their first repository classification.
	if !stdErrors.As(err, &e) { // Search the complete error chain instead of requiring the repository error at the outermost layer.
		return "", false
	}
	return e.Code, true
}

// IsCode returns true if err is an internal Error with the given code.
func IsCode(err error, code ErrorCode) bool {
	if err == nil {
		return false
	}
	if c, ok := CodeOf(err); ok {
		return c == code
	}
	return false
}

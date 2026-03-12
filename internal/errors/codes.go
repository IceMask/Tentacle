package errors

import (
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

// MapToGRPC maps an internal error to a gRPC status error.
func MapToGRPC(err error) error {
	e, ok := err.(*Error)
	if !ok {
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
	e, ok := err.(*Error)
	if !ok {
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
	e, ok := err.(*Error)
	if !ok {
		return &JSONRPCError{Code: -32603, Message: "Internal error", Data: err.Error()}
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

	return &JSONRPCError{
		Code:    code,
		Message: e.Message,
		Data:    string(e.Code),
	}
}

// CodeOf extracts the internal error code, if present.
func CodeOf(err error) (ErrorCode, bool) {
	e, ok := err.(*Error)
	if !ok {
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

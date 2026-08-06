// body.go centralizes bounded HTTP request-body reading and strict single-document JSON decoding for gateway transports.
package httpinput

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

const MaxRequestBodyBytes int64 = 1 << 20 // MaxRequestBodyBytes limits protected gateway request bodies to one mebibyte.

// LimitBody rejects declared oversized requests and wraps accepted bodies with net/http's streaming size guard before authentication or dispatch.
func LimitBody(next http.Handler, maxBytes int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // Wrap the downstream route so every body reader observes the same byte ceiling.
		if r.ContentLength > maxBytes { // Reject a known oversized body before authentication spends work reading or hashing it.
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge) // Return the standard HTTP status without reflecting request content.
			return                                                                    // Stop before invoking authentication or application handlers for the oversized request.
		}
		if r.Body != nil { // Install the streaming guard only when the request actually carries a body reader.
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes) // Convert reads beyond the configured ceiling into a typed MaxBytesError.
		}
		next.ServeHTTP(w, r) // Forward the bounded request to authentication and protocol dispatch.
	})
}

// DecodeSingleJSON decodes exactly one JSON value, preserves integral identifiers as json.Number, and rejects trailing documents.
func DecodeSingleJSON(body io.Reader, destination interface{}) error {
	if body == nil { // Reject a missing reader because every JSON transport request requires one document.
		return errors.New("request body is required") // Return a stable parse failure without dereferencing a nil reader.
	}
	decoder := json.NewDecoder(body)                    // Create one streaming decoder over the already bounded request body.
	decoder.UseNumber()                                 // Preserve JSON integer text so request identifiers do not lose precision through float64 conversion.
	if err := decoder.Decode(destination); err != nil { // Decode the sole expected JSON document into the transport envelope.
		return err // Preserve syntax, type, and request-size failures for transport-specific mapping.
	}
	var trailing interface{}                             // Allocate a generic sink used only to detect a second JSON document after the first value.
	if err := decoder.Decode(&trailing); err != io.EOF { // Require only optional whitespace and EOF after the first decoded document.
		if err == nil { // Distinguish a successfully decoded second document from malformed trailing bytes.
			return errors.New("request body must contain exactly one JSON value") // Reject concatenated JSON documents deterministically.
		}
		return err // Preserve malformed trailing-data diagnostics for the protocol parse-error response.
	}
	return nil // Confirm that one complete JSON value and no additional document were supplied.
}

// ReadAndRestoreBody reads at most maxBytes from a request body and restores the exact bytes for downstream decoding after HMAC verification.
func ReadAndRestoreBody(r *http.Request, maxBytes int64) ([]byte, error) {
	if r == nil { // Reject a missing request because there is no body stream to clone safely.
		return nil, errors.New("request is required") // Return a deterministic error instead of panicking on a nil request.
	}
	if r.Body == nil { // Treat body-less requests as an empty signed payload.
		return nil, nil // Preserve empty-body semantics without installing an unnecessary reader.
	}
	limitedBody := io.LimitReader(r.Body, maxBytes+1) // Read one sentinel byte beyond the limit so direct middleware use also detects oversized bodies.
	body, err := io.ReadAll(limitedBody)              // Materialize the bounded payload once for HMAC hashing and downstream replay.
	if err != nil {                                   // Preserve typed MaxBytesError values emitted by an outer net/http guard.
		return nil, err // Return the body-read failure without restoring a partial and unverifiable payload.
	}
	if int64(len(body)) > maxBytes { // Detect oversized bodies when the middleware is invoked without the outer LimitBody wrapper.
		return nil, &http.MaxBytesError{Limit: maxBytes} // Return the standard typed size error so callers can map it to HTTP 413.
	}
	r.Body = io.NopCloser(bytes.NewReader(body)) // Restore the exact accepted bytes so the protocol handler sees the signed payload unchanged.
	return body, nil                             // Return the accepted payload bytes for canonical HMAC calculation.
}

// IsBodyTooLarge reports whether an error chain contains net/http's request-body size sentinel.
func IsBodyTooLarge(err error) bool {
	var sizeError *http.MaxBytesError // Allocate a typed target used by errors.As across wrapped authentication and parsing failures.
	return errors.As(err, &sizeError) // Recognize direct and repository-wrapped body-limit failures consistently.
}

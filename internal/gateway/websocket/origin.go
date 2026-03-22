// origin.go implements a strict WebSocket origin policy with same-origin fallback and optional explicit allowlists.
package websocket

import (
	"net/http"
	"net/url"
	"strings"
)

// ParseAllowedOrigins normalizes a comma-separated origin allowlist into canonical "scheme://host" values.
func ParseAllowedOrigins(rawOrigins string) []string {
	normalizedOrigins := make([]string, 0)                     // Start with an empty allowlist so callers can distinguish no explicit origins from configured ones.
	for _, rawOrigin := range strings.Split(rawOrigins, ",") { // Walk each comma-delimited origin entry so whitespace and empty items can be discarded deterministically.
		trimmedOrigin := strings.TrimSpace(rawOrigin) // Normalize surrounding whitespace because env and YAML lists often contain spaces after commas.
		if trimmedOrigin == "" {                      // Skip empty entries so accidental double commas do not create invalid allowlist slots.
			continue // Ignore blank origin entries because they do not contribute a usable browser origin.
		}
		parsedOrigin, err := url.Parse(trimmedOrigin) // Parse the explicit origin so the runtime stores the normalized scheme and host only.
		if err != nil {                               // Skip malformed origins defensively because startup validation should already have rejected them.
			continue // Ignore malformed runtime entries so a stale config cannot crash the handshake path unexpectedly.
		}
		if parsedOrigin.Scheme == "" || parsedOrigin.Host == "" { // Reject partial URLs because browser Origin headers always provide both scheme and host.
			continue // Ignore incomplete origin entries because they cannot match any real browser handshake.
		}
		normalizedOrigins = append(normalizedOrigins, strings.ToLower(parsedOrigin.Scheme+"://"+parsedOrigin.Host)) // Store a canonical lowercase origin so runtime comparisons stay exact and cheap.
	}

	return normalizedOrigins // Return the canonical allowlist used by the WebSocket handshake checker.
}

// newOriginChecker builds the Gorilla WebSocket origin callback used by the gateway handshake.
func newOriginChecker(allowedOrigins []string) func(*http.Request) bool {
	allowedOriginSet := make(map[string]struct{}, len(allowedOrigins)) // Normalize the explicit allowlist into a set so origin checks stay O(1) per handshake.
	for _, allowedOrigin := range allowedOrigins {                     // Walk the caller-supplied allowlist once so the handshake callback can remain allocation-light.
		allowedOriginSet[strings.ToLower(strings.TrimSpace(allowedOrigin))] = struct{}{} // Store each normalized origin in the set so later comparisons are exact and case-insensitive.
	}

	return func(r *http.Request) bool { // Return the handshake callback consumed directly by Gorilla WebSocket.
		originHeader := strings.TrimSpace(r.Header.Get("Origin")) // Read the browser Origin header once because empty-origin requests need separate handling.
		if originHeader == "" {                                   // Allow empty-origin requests because CLI and server-side clients often do not send an Origin header.
			return true // Accept non-browser clients that do not participate in browser same-origin enforcement.
		}

		parsedOrigin, err := url.Parse(originHeader) // Parse the supplied browser Origin header before any host comparison logic runs.
		if err != nil {                              // Reject malformed origin headers because they cannot be matched safely against host policy.
			return false // Deny malformed browser origins to avoid accidentally treating them as local or trusted.
		}
		if parsedOrigin.Scheme == "" || parsedOrigin.Host == "" { // Reject partial origins because the browser Origin contract requires both scheme and host.
			return false // Deny incomplete origin headers because they cannot be validated safely.
		}
		normalizedOrigin := strings.ToLower(parsedOrigin.Scheme + "://" + parsedOrigin.Host) // Normalize the incoming origin into the same canonical form as the allowlist entries.
		if len(allowedOriginSet) > 0 {                                                       // Prefer the explicit allowlist whenever one is configured by the operator.
			_, ok := allowedOriginSet[normalizedOrigin] // Resolve the normalized incoming origin against the operator-supplied allowlist.
			return ok                                   // Accept only explicit allowlist matches when an allowlist exists.
		}

		return sameOriginHost(r, parsedOrigin.Host) // Fall back to strict same-host validation when no explicit allowlist has been configured.
	}
}

// sameOriginHost compares the request host and Origin host using the same normalization as Go's HTTP server.
func sameOriginHost(r *http.Request, originHost string) bool {
	requestHost := strings.TrimSpace(r.Host) // Normalize the request Host header so proxy-added whitespace never affects comparisons.
	if requestHost == "" {                   // Reject requests without a Host because same-origin comparison requires a concrete request authority.
		return false // Deny host-less requests because they cannot be matched safely against the Origin header.
	}

	return strings.EqualFold(requestHost, originHost) // Accept only exact host matches so the default policy remains strict same-origin for browser callers.
}

package middleware

import (
	"net/http"
	"strings"

	"mcp_for_appium/internal/auth"
)

type AuthMiddleware struct {
	patValidator *auth.PATValidator
	// oidcValidator *auth.OIDCValidator
	// hmacValidator *auth.HMACValidator
}

// NewAuthMiddleware executes this operation.
func NewAuthMiddleware(pat *auth.PATValidator) *AuthMiddleware {
	return &AuthMiddleware{patValidator: pat}
}

// Handle executes this operation.
func (m *AuthMiddleware) Handle(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Simple Bearer check
		token := strings.TrimPrefix(authHeader, "Bearer ")

		// Validate PAT
		sub, err := m.patValidator.Validate(token)
		if err != nil {
			// Try other validators if implemented
			http.Error(w, "Unauthorized: "+err.Error(), http.StatusUnauthorized)
			return
		}

		// Inject subject into context
		ctx := auth.WithSubject(r.Context(), sub)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

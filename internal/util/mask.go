package util

import (
	"regexp"
	"strings"
)

var (
	// Simple regex for common secrets. In production, this would be more robust.
	// Matches "token": "...", "password": "...", etc.
	jsonSecretRegex = regexp.MustCompile(`"(token|secret|password|pin|access_key|secret_key)":\s*"([^"]+)"`)
)

// MaskSecrets masks sensitive information in a string.
func MaskSecrets(s string) string {
	// 1. Mask JSON fields
	s = jsonSecretRegex.ReplaceAllString(s, `"$1": "***"`)

	// 2. Mask generic patterns if needed (e.g. Bearer tokens)
	// This is a simplified implementation.
	return s
}

// MaskSecureFields masks specific fields in a map (e.g. for logging structured data).
func MaskSecureFields(fields map[string]interface{}) map[string]interface{} {
	masked := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		if isSensitiveKey(k) {
			masked[k] = "***"
		} else {
			masked[k] = v
		}
	}
	return masked
}

// isSensitiveKey executes this operation.
func isSensitiveKey(k string) bool {
	k = strings.ToLower(k)
	return strings.Contains(k, "token") ||
		strings.Contains(k, "secret") ||
		strings.Contains(k, "password") ||
		strings.Contains(k, "pin") ||
		strings.Contains(k, "auth")
}

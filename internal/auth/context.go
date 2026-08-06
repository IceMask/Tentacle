package auth

import (
	"context"
)

type contextKey struct{}

var subjectKey = contextKey{}

// Subject represents the authenticated entity.
type Subject struct {
	ID           string
	Type         string // "pat", "oidc", "hmac"
	CredentialID string
	TenantID     string
	Scopes       []string
}

// WithSubject returns a new context with the subject.
func WithSubject(ctx context.Context, sub *Subject) context.Context {
	return context.WithValue(ctx, subjectKey, sub)
}

// SubjectFrom returns the subject from the context, if present.
func SubjectFrom(ctx context.Context) (*Subject, bool) {
	sub, ok := ctx.Value(subjectKey).(*Subject)
	return sub, ok
}

package postgres

import (
	"database/sql"
	"time"
)

type Session struct {
	ID           string
	ProjectID    string
	Status       string
	Capabilities []byte // JSON
	CreatedAt    time.Time
	UpdatedAt    time.Time
	EndedAt      sql.NullTime
}

type Trace struct {
	ID             string
	SessionID      string
	ProjectID      string
	Status         string
	CurrentAttempt int64
	TerminalReason *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type PlanEvent struct {
	TraceID   string
	Seq       int64
	StepIndex int
	Status    string
	Payload   []byte // JSON
	CreatedAt time.Time
}

type Artifact struct {
	ID        string
	TraceID   string
	Key       string
	Type      string
	Size      int64
	CreatedAt time.Time
	Metadata  []byte // JSON
}

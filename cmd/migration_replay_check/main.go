// main.go replays the PostgreSQL migrations against an isolated embedded database instance and verifies the schema plus the current DAO write paths.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/storage/postgres"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	replayDatabaseName = "mcp_mobile_worker" // replayDatabaseName keeps the embedded database name aligned with the repository examples and config defaults.
	replayDatabaseUser = "postgres"          // replayDatabaseUser defines the superuser name used by the embedded PostgreSQL instance.
	replayDatabasePass = "postgres"          // replayDatabasePass defines the password paired with the embedded PostgreSQL superuser.
	replayDatabaseHost = "127.0.0.1"         // replayDatabaseHost keeps the replay check on the local loopback interface only.
	replayDatabasePort = 54329               // replayDatabasePort avoids clashing with any developer-managed PostgreSQL instance that may already use 5432.
)

// verificationSummary captures the machine-readable output emitted after a successful migration replay and DAO verification run.
type verificationSummary struct {
	DSN                  string         `json:"dsn"`                  // DSN reports the exact temporary PostgreSQL connection string used by the replay check.
	Migrations           []string       `json:"migrations"`           // Migrations lists the migration files that were applied during the replay.
	Tables               []string       `json:"tables"`               // Tables records the expected public tables that exist after replay.
	Indexes              []string       `json:"indexes"`              // Indexes records the expected supporting indexes that exist after replay.
	InsertedRowCount     map[string]int `json:"insertedRowCount"`     // InsertedRowCount reports the number of verification rows written through the DAO and direct schema checks.
	SessionEnded         bool           `json:"sessionEnded"`         // SessionEnded reports whether the transactional end-session verification changed the row exactly once.
	TraceRunningChanged  bool           `json:"traceRunningChanged"`  // TraceRunningChanged reports whether the optimistic pending-to-running transition succeeded.
	TraceCompleteChanged bool           `json:"traceCompleteChanged"` // TraceCompleteChanged reports whether the optimistic running-to-completed transition succeeded.
	Status               string         `json:"status"`               // Status reports the overall replay result for CI and human readers.
}

// mustFindRepoRoot walks upward from the current working directory until it finds the repository root containing go.mod.
func mustFindRepoRoot() string {
	workingDir, err := os.Getwd() // Read the current working directory so the command can resolve repository-relative paths without hard-coded machine paths.
	if err != nil {               // Stop immediately when the process cannot determine its starting directory.
		panic(fmt.Sprintf("failed to read current working directory: %v", err)) // Surface the directory lookup failure because no repository files can be resolved without it.
	}

	currentDir := workingDir // Seed the upward walk with the process working directory.
	for {                    // Continue walking parents until go.mod is found or the filesystem root is reached.
		goModPath := filepath.Join(currentDir, "go.mod") // Build the candidate go.mod path for the current directory in the upward walk.
		if _, err := os.Stat(goModPath); err == nil {    // Return the first directory that clearly looks like the repository root.
			return currentDir // Use the directory containing go.mod as the repository root for migration discovery.
		}

		parentDir := filepath.Dir(currentDir) // Compute the parent directory for the next upward-walk iteration.
		if parentDir == currentDir {          // Stop when the walk reaches the filesystem root without finding go.mod.
			panic("failed to locate repository root containing go.mod") // Surface the missing repository root because no migration replay can proceed safely.
		}

		currentDir = parentDir // Advance the search to the next parent directory.
	}
}

// mustListMigrationFiles returns all SQL migration files in lexical order from the repository migration directory.
func mustListMigrationFiles(repoRoot string) []string {
	migrationGlob := filepath.Join(repoRoot, "internal", "storage", "postgres", "migrations", "*.sql") // Build the glob pattern for the repository migration directory.
	migrationFiles, err := filepath.Glob(migrationGlob)                                                // Resolve every SQL migration file shipped in the repository.
	if err != nil {                                                                                    // Stop immediately when the filesystem glob itself fails.
		panic(fmt.Sprintf("failed to glob migration files: %v", err)) // Surface the glob failure because replay cannot continue without the migration file list.
	}
	if len(migrationFiles) == 0 { // Stop when the repository does not contain any SQL migrations to replay.
		panic("no migration files found under internal/storage/postgres/migrations") // Surface the empty migration directory because the replay check would be meaningless.
	}

	sort.Strings(migrationFiles) // Normalize the migration order so replay matches the intended lexical sequence.
	return migrationFiles        // Return the ordered migration file list for application and reporting.
}

// mustReadMigrationFile loads one migration file from disk and returns its raw SQL contents.
func mustReadMigrationFile(path string) string {
	fileBytes, err := os.ReadFile(path) // Read the committed migration file exactly as it exists in the working tree.
	if err != nil {                     // Stop immediately when the migration file cannot be read.
		panic(fmt.Sprintf("failed to read migration file %s: %v", path, err)) // Surface the exact unreadable file to speed up diagnosis.
	}

	return string(fileBytes) // Return the raw SQL text so PostgreSQL executes the same migration content committed in the repository.
}

// mustConnectAdmin opens a direct pgx connection to the embedded replay database using the supplied DSN.
func mustConnectAdmin(ctx context.Context, dsn string) *pgx.Conn {
	conn, err := pgx.Connect(ctx, dsn) // Open a direct PostgreSQL connection used for migration execution and schema inspection.
	if err != nil {                    // Stop immediately when the embedded database cannot be reached.
		panic(fmt.Sprintf("failed to connect to embedded postgres: %v", err)) // Surface the connection failure because all later replay steps depend on a live database session.
	}

	return conn // Return the connected pgx session so callers can execute migrations and inspection queries.
}

// mustExecSQL executes one SQL statement batch and stops the replay when PostgreSQL reports an error.
func mustExecSQL(ctx context.Context, conn *pgx.Conn, sqlText string, label string, args ...any) {
	if _, err := conn.Exec(ctx, sqlText, args...); err != nil { // Execute the SQL batch with any positional arguments against the live replay database.
		panic(fmt.Sprintf("failed to execute %s: %v", label, err)) // Surface the named failing step so the broken migration or verification query is obvious.
	}
}

// mustQueryNames returns a sorted list of names from a one-column text query and stops the replay on any query or scan error.
func mustQueryNames(ctx context.Context, conn *pgx.Conn, query string, args ...any) []string {
	rows, err := conn.Query(ctx, query, args...) // Execute the metadata query that inspects the live schema state after replay.
	if err != nil {                              // Stop immediately when PostgreSQL rejects the inspection query.
		panic(fmt.Sprintf("failed to execute metadata query: %v", err)) // Surface the inspection failure because schema verification cannot continue without it.
	}
	defer rows.Close() // Release the query cursor before the next replay step begins.

	names := make([]string, 0) // Accumulate the returned table or index names in memory for deterministic comparison.
	for rows.Next() {          // Iterate through the full metadata result set returned by PostgreSQL.
		var name string                          // Hold the current metadata name scanned from one row.
		if err := rows.Scan(&name); err != nil { // Stop immediately when a returned row cannot be decoded.
			panic(fmt.Sprintf("failed to scan metadata row: %v", err)) // Surface the scan failure because it means the verification query did not return the expected shape.
		}
		names = append(names, name) // Preserve the returned name for later comparison and reporting.
	}
	if err := rows.Err(); err != nil { // Stop if PostgreSQL reports a deferred cursor error after iteration.
		panic(fmt.Sprintf("failed during metadata query iteration: %v", err)) // Surface the deferred cursor failure because it invalidates the inspection result.
	}

	sort.Strings(names) // Normalize the result order so comparisons and JSON output stay deterministic.
	return names        // Return the ordered metadata names to the caller.
}

// mustQueryCount returns the integer result from a scalar count query and stops the replay on error.
func mustQueryCount(ctx context.Context, conn *pgx.Conn, query string, args ...any) int {
	var count int                                                           // Hold the scalar count result returned by PostgreSQL.
	if err := conn.QueryRow(ctx, query, args...).Scan(&count); err != nil { // Execute the count query and decode the single integer cell.
		panic(fmt.Sprintf("failed to query verification count: %v", err)) // Surface the count-query failure because the minimal DAO write-path check depends on it.
	}

	return count // Return the decoded row count for inclusion in the final verification summary.
}

// applyMigrations executes every discovered SQL migration in order against the embedded replay database.
func applyMigrations(ctx context.Context, conn *pgx.Conn, migrationFiles []string, labelPrefix string) {
	for _, migrationFile := range migrationFiles { // Replay every committed migration file in lexical order so the schema matches normal startup expectations.
		sqlText := mustReadMigrationFile(migrationFile)                          // Load the exact SQL text from the current working tree before execution.
		label := fmt.Sprintf("%s:%s", labelPrefix, filepath.Base(migrationFile)) // Build a readable execution label that identifies the migration file and replay phase.
		mustExecSQL(ctx, conn, sqlText, label)                                   // Execute the migration SQL against the live replay database.
	}
}

// verifySchema checks that the expected tables and indexes exist after the migration replay.
func verifySchema(ctx context.Context, conn *pgx.Conn) ([]string, []string) {
	expectedTables := []string{"artifacts", "audit_logs", "hmac_keys", "pat_tokens", "plan_events", "sessions", "traces"} // expectedTables defines the current public tables promised by the design and migration files.
	expectedIndexes := []string{                                                                                          // expectedIndexes enumerates the current supporting indexes that the DAO paths depend on.
		"idx_artifacts_key_unique",
		"idx_artifacts_trace_created_at",
		"idx_audit_logs_project_created_at",
		"idx_audit_logs_resource_created_at",
		"idx_audit_logs_trace_created_at",
		"idx_hmac_keys_tenant_status_created_at",
		"idx_hmac_keys_tenant_validity",
		"idx_pat_tokens_rotated_from",
		"idx_pat_tokens_subject_status_created_at",
		"idx_pat_tokens_tenant_status_created_at",
		"idx_plan_events_trace_created_at",
		"idx_sessions_project_created_at",
		"idx_sessions_status_updated_at",
		"idx_traces_project_status_updated_at",
		"idx_traces_session_created_at",
	}

	actualTables := mustQueryNames(ctx, conn, "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = ANY($1)", expectedTables)    // Query the live table set created by the migrations.
	actualIndexes := mustQueryNames(ctx, conn, "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND indexname = ANY($1)", expectedIndexes) // Query the live index set created by the migrations.

	if strings.Join(actualTables, ",") != strings.Join(expectedTables, ",") { // Fail when the live table set differs from the expected schema contract.
		panic(fmt.Sprintf("unexpected table set after migration replay: %v", actualTables)) // Surface the live table names so missing or extra objects are easy to inspect.
	}
	if strings.Join(actualIndexes, ",") != strings.Join(expectedIndexes, ",") { // Fail when the live index set differs from the expected DAO support indexes.
		panic(fmt.Sprintf("unexpected index set after migration replay: %v", actualIndexes)) // Surface the live index names so migration drift is easy to diagnose.
	}

	return actualTables, actualIndexes // Return the verified live schema objects for the final JSON summary.
}

// verifyDAOPaths writes and reads a minimal set of rows through the current DAO API so schema compatibility is checked against real repository code.
func verifyDAOPaths(ctx context.Context, dsn string) (map[string]int, bool, bool, bool) {
	dao, err := postgres.NewDAO(ctx, config.PostgresConfig{DSN: dsn, MaxOpenConns: 4, MaxIdleConns: 2, ConnMaxLifetime: time.Minute}) // Open the repository DAO against the live replay database so the verification runs through production code paths.
	if err != nil {                                                                                                                   // Stop immediately when the DAO cannot connect to the replay database.
		panic(fmt.Sprintf("failed to construct DAO for replay verification: %v", err)) // Surface the DAO construction failure because the schema compatibility check cannot proceed without it.
	}
	defer dao.Close() // Release the DAO connection pool before the embedded database shuts down.

	now := time.Now().UTC()        // Capture one stable timestamp so the verification rows have coherent timestamps across related tables.
	sessionID := uuid.NewString()  // Generate an isolated session identifier so repeated local replay runs never collide on primary keys.
	traceID := uuid.NewString()    // Generate an isolated trace identifier linked to the verification session.
	artifactID := uuid.NewString() // Generate an isolated artifact identifier for the artifact metadata verification row.
	auditID := uuid.NewString()    // Generate an isolated audit-log identifier for the audit verification row.

	if err := dao.CreateSession(ctx, &postgres.Session{ID: sessionID, ProjectID: "migration-replay-project", Status: "created", Capabilities: []byte(`{"platformName":"Android","automationName":"UiAutomator2"}`), CreatedAt: now, UpdatedAt: now}); err != nil { // Insert one session row through the repository DAO path.
		panic(fmt.Sprintf("failed to create verification session: %v", err)) // Surface the DAO write failure because it indicates schema drift against the live database.
	}

	sessionEnded := false                                      // Track whether the transactional end-session path performed its state transition exactly once.
	if err := dao.WithTx(ctx, func(tx *postgres.TxDAO) error { // Exercise the new transactional helper path against the live replay database.
		session, err := tx.GetSessionForUpdate(ctx, sessionID) // Lock the verification session row using the repository DAO helper.
		if err != nil {                                        // Abort when the transactional read helper cannot retrieve or lock the session row.
			return err // Preserve the original DAO error so the replay failure stays specific.
		}
		if session.Status != "created" { // Validate the initial session status before the end-session transition is attempted.
			return fmt.Errorf("unexpected verification session status: %s", session.Status) // Surface unexpected session state because it indicates DAO persistence drift.
		}
		updated, err := tx.EndSessionIfActive(ctx, sessionID, now.Add(time.Second)) // Exercise the row-locked idempotent session-ending helper.
		if err != nil {                                                             // Abort when the transactional update helper cannot persist the terminal session state.
			return err // Preserve the original DAO write error so the replay failure remains precise.
		}
		sessionEnded = updated // Record whether the transactional helper performed the expected single-row transition.
		return nil             // Allow the transaction to commit once the lock-and-update path has been verified.
	}); err != nil { // Stop when the transactional helper path fails on the live replay database.
		panic(fmt.Sprintf("failed to verify transactional session helpers: %v", err)) // Surface the transactional helper failure because it indicates schema or DAO drift.
	}

	if err := dao.CreateTrace(ctx, &postgres.Trace{ID: traceID, SessionID: sessionID, ProjectID: "migration-replay-project", Status: "pending", CreatedAt: now, UpdatedAt: now}); err != nil { // Insert one trace row through the repository DAO path.
		panic(fmt.Sprintf("failed to create verification trace: %v", err)) // Surface the DAO trace-write failure because it indicates schema drift against the live database.
	}

	traceRunningChanged, err := dao.CompareAndSwapTraceStatus(ctx, traceID, []string{"pending"}, "running") // Exercise the optimistic trace transition from pending to running.
	if err != nil {                                                                                         // Stop when the first optimistic transition fails unexpectedly.
		panic(fmt.Sprintf("failed to transition verification trace to running: %v", err)) // Surface the optimistic transition failure because it indicates schema or DAO drift.
	}
	traceCompleteChanged, err := dao.CompareAndSwapTraceStatus(ctx, traceID, []string{"running"}, "completed") // Exercise the optimistic trace transition from running to completed.
	if err != nil {                                                                                            // Stop when the second optimistic transition fails unexpectedly.
		panic(fmt.Sprintf("failed to transition verification trace to completed: %v", err)) // Surface the optimistic terminal transition failure because it indicates schema or DAO drift.
	}

	if err := dao.InsertPlanEvent(ctx, &postgres.PlanEvent{TraceID: traceID, Seq: 1, StepIndex: 0, Status: "passed", Payload: []byte(`{"message":"migration replay verification"}`), CreatedAt: now}); err != nil { // Insert one plan-event row through the repository DAO path.
		panic(fmt.Sprintf("failed to insert verification plan event: %v", err)) // Surface the DAO event-write failure because it indicates schema drift against the live database.
	}
	if err := dao.InsertArtifact(ctx, &postgres.Artifact{ID: artifactID, TraceID: traceID, Key: traceID + "/artifact-replay-check/screenshot.png", Type: "image/png", Size: 128, CreatedAt: now, Metadata: []byte(`{"kind":"verification"}`)}); err != nil { // Insert one artifact row through the repository DAO path.
		panic(fmt.Sprintf("failed to insert verification artifact: %v", err)) // Surface the DAO artifact-write failure because it indicates schema drift against the live database.
	}
	if err := dao.InsertAuditLog(ctx, &postgres.AuditLog{ID: auditID, ProjectID: "migration-replay-project", SessionID: sessionID, TraceID: traceID, Actor: "migration-replay-check", Action: "verify", ResourceType: "trace", ResourceID: traceID, Payload: []byte(`{"source":"migration_replay_check"}`), CreatedAt: now}); err != nil { // Insert one audit-log row through the new DAO audit path.
		panic(fmt.Sprintf("failed to insert verification audit log: %v", err)) // Surface the DAO audit-write failure because it indicates schema drift against the live database.
	}

	events, err := dao.ListEvents(ctx, traceID, 0, 10) // Read back the verification events through the repository DAO read path.
	if err != nil {                                    // Stop when the event read path cannot retrieve the inserted verification row.
		panic(fmt.Sprintf("failed to list verification events: %v", err)) // Surface the DAO read failure because it indicates schema drift or query incompatibility.
	}
	artifacts, err := dao.ListArtifacts(ctx, traceID) // Read back the verification artifacts through the repository DAO read path.
	if err != nil {                                   // Stop when the artifact read path cannot retrieve the inserted verification row.
		panic(fmt.Sprintf("failed to list verification artifacts: %v", err)) // Surface the DAO read failure because it indicates schema drift or query incompatibility.
	}
	session, err := dao.GetSession(ctx, sessionID) // Read back the verification session through the repository DAO read path.
	if err != nil {                                // Stop when the session read path cannot retrieve the inserted verification row.
		panic(fmt.Sprintf("failed to get verification session: %v", err)) // Surface the DAO read failure because it indicates schema drift or query incompatibility.
	}
	trace, err := dao.GetTrace(ctx, traceID) // Read back the verification trace through the repository DAO read path.
	if err != nil {                          // Stop when the trace read path cannot retrieve the inserted verification row.
		panic(fmt.Sprintf("failed to get verification trace: %v", err)) // Surface the DAO read failure because it indicates schema drift or query incompatibility.
	}

	if !session.EndedAt.Valid { // Fail when the transactional end-session helper did not persist a terminal timestamp.
		panic("verification session was not marked ended by transactional helper") // Surface the missing terminal timestamp because it means the row-locked update path did not persist correctly.
	}
	if trace.Status != "completed" { // Fail when the optimistic trace transitions did not leave the verification trace in the expected terminal state.
		panic(fmt.Sprintf("verification trace ended in unexpected status: %s", trace.Status)) // Surface the actual trace status because it identifies the broken state transition.
	}
	if len(events) != 1 { // Fail when the verification event row cannot be read back through the repository DAO list path.
		panic(fmt.Sprintf("unexpected verification event count via DAO: %d", len(events))) // Surface the live event count because it pinpoints write or read drift.
	}
	if len(artifacts) != 1 { // Fail when the verification artifact row cannot be read back through the repository DAO list path.
		panic(fmt.Sprintf("unexpected verification artifact count via DAO: %d", len(artifacts))) // Surface the live artifact count because it pinpoints write or read drift.
	}

	adminConn := mustConnectAdmin(ctx, dsn) // Open a direct admin connection for scalar audit-row counts not yet exposed through the repository DAO.
	defer adminConn.Close(ctx)              // Release the direct inspection connection before the embedded database shuts down.

	counts := map[string]int{ // Collect the live row counts written during the minimal DAO compatibility check.
		"sessions":   mustQueryCount(ctx, adminConn, "SELECT COUNT(*) FROM sessions WHERE id = $1", sessionID),        // Count the verification session row in the live database.
		"traces":     mustQueryCount(ctx, adminConn, "SELECT COUNT(*) FROM traces WHERE id = $1", traceID),            // Count the verification trace row in the live database.
		"planEvents": mustQueryCount(ctx, adminConn, "SELECT COUNT(*) FROM plan_events WHERE trace_id = $1", traceID), // Count the verification event row in the live database.
		"artifacts":  mustQueryCount(ctx, adminConn, "SELECT COUNT(*) FROM artifacts WHERE id = $1", artifactID),      // Count the verification artifact row in the live database.
		"auditLogs":  mustQueryCount(ctx, adminConn, "SELECT COUNT(*) FROM audit_logs WHERE id = $1", auditID),        // Count the verification audit-log row in the live database.
	}

	return counts, sessionEnded, traceRunningChanged, traceCompleteChanged // Return the verification counts and helper-path results for the final replay summary.
}

// main boots an isolated embedded PostgreSQL instance, replays the repository migrations twice, verifies the schema, and exercises the current DAO paths.
func main() {
	repoRoot := mustFindRepoRoot()                              // Discover the repository root dynamically so the command works from CI and local shells without hard-coded paths.
	migrationFiles := mustListMigrationFiles(repoRoot)          // Discover the committed SQL migration files that must be replayed in order.
	tempRoot, err := os.MkdirTemp("", "mcp-migration-replay-*") // Create one isolated temporary root that holds the embedded runtime and data directories for this execution.
	if err != nil {                                             // Stop immediately when the process cannot create its isolated temporary workspace.
		panic(fmt.Sprintf("failed to create temporary replay directory: %v", err)) // Surface the temporary-directory failure because the embedded database cannot boot without it.
	}
	defer os.RemoveAll(tempRoot) // Remove the isolated temporary workspace after the embedded database shuts down.

	runtimePath := filepath.Join(tempRoot, "runtime")                                                                                                                   // Place the embedded PostgreSQL binaries under the isolated temporary root for this run.
	dataPath := filepath.Join(tempRoot, "data")                                                                                                                         // Place the embedded PostgreSQL data directory under the isolated temporary root for this run.
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", replayDatabaseUser, replayDatabasePass, replayDatabaseHost, replayDatabasePort, replayDatabaseName) // Build the loopback DSN used by the migration replay and DAO verification steps.

	embeddedDatabase := embeddedpostgres.NewDatabase( // Configure one isolated embedded PostgreSQL instance dedicated to the replay check.
		embeddedpostgres.DefaultConfig().
			Version(embeddedpostgres.V16). // Pin the embedded PostgreSQL major version used by the replay check.
			Database(replayDatabaseName).  // Create the temporary database with the same logical name used by repository examples.
			Username(replayDatabaseUser).  // Configure the embedded superuser name for the replay database.
			Password(replayDatabasePass).  // Configure the embedded superuser password for the replay database.
			Port(replayDatabasePort).      // Bind the embedded database to the dedicated replay port.
			RuntimePath(runtimePath).      // Store the embedded PostgreSQL runtime files in the isolated temporary workspace.
			DataPath(dataPath),            // Store the embedded PostgreSQL data directory in the isolated temporary workspace.
	)

	if err := embeddedDatabase.Start(); err != nil { // Boot the embedded PostgreSQL instance before replaying any migration files.
		panic(fmt.Sprintf("failed to start embedded postgres for migration replay: %v", err)) // Surface the embedded-database startup failure because no live replay can occur without it.
	}
	defer func() {
		if err := embeddedDatabase.Stop(); err != nil { // Stop the embedded PostgreSQL instance after replay and verification complete.
			panic(fmt.Sprintf("failed to stop embedded postgres after migration replay: %v", err)) // Surface shutdown failures because they can leave temporary processes or files behind.
		}
	}() // Ensure the embedded database is always torn down before the command exits.

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second) // Bound the full replay and verification flow so CI and local runs fail fast when the database hangs.
	defer cancel()                                                           // Release the timeout context resources after the replay command finishes.

	adminConn := mustConnectAdmin(ctx, dsn) // Open the direct pgx connection used for migration replay and schema inspection.
	defer adminConn.Close(ctx)              // Release the admin pgx connection before shutting down the embedded database.

	applyMigrations(ctx, adminConn, migrationFiles, "initial")                                  // Apply the committed migration files to the empty live database.
	applyMigrations(ctx, adminConn, migrationFiles, "replay")                                   // Re-apply the committed migration files to the already-migrated live database to verify idempotency.
	tables, indexes := verifySchema(ctx, adminConn)                                             // Verify that the replay created the expected live schema objects and supporting indexes.
	counts, sessionEnded, traceRunningChanged, traceCompleteChanged := verifyDAOPaths(ctx, dsn) // Exercise the current DAO helpers against the live replay database.

	summary := verificationSummary{ // Build the final machine-readable report consumed by humans and CI logs.
		DSN:                  dsn,                  // Report the loopback DSN used by the isolated replay database.
		Migrations:           migrationFiles,       // Report the ordered migration files that were replayed.
		Tables:               tables,               // Report the verified live table set created by the migrations.
		Indexes:              indexes,              // Report the verified live index set created by the migrations.
		InsertedRowCount:     counts,               // Report the live row counts written through the DAO verification path.
		SessionEnded:         sessionEnded,         // Report whether the row-locked session termination helper updated the live row.
		TraceRunningChanged:  traceRunningChanged,  // Report whether the optimistic pending-to-running trace transition succeeded.
		TraceCompleteChanged: traceCompleteChanged, // Report whether the optimistic running-to-completed trace transition succeeded.
		Status:               "ok",                 // Report overall success because every replay and verification step completed.
	}

	encodedSummary, err := json.MarshalIndent(summary, "", "  ") // Render the final replay report as readable JSON for local and CI logs.
	if err != nil {                                              // Stop immediately when the final verification summary cannot be encoded.
		panic(fmt.Sprintf("failed to encode migration replay summary: %v", err)) // Surface the JSON encoding failure because it would hide the replay result from the operator.
	}

	fmt.Println(string(encodedSummary)) // Print the final migration replay report so operators can inspect the live schema and DAO verification results.
}

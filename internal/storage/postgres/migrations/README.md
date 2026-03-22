# PostgreSQL Migrations

This directory contains the PostgreSQL schema bootstrap files required by the current DAO implementation.

## Scope

- `001_init.sql` creates the primary runtime tables currently used by the repository code:
  - `sessions`
  - `traces`
  - `plan_events`
  - `artifacts`
- `002_audit_logs.sql` adds the append-only `audit_logs` table promised by the design document.
- `004_pat_tokens.sql` adds the authoritative `pat_tokens` table used by the v4.4 PAT authentication path.
- `005_hmac_keys.sql` adds the authoritative `hmac_keys` table used by the v4.4 HMAC authentication path.
- The migrations also create the indexes needed by the current query paths in `internal/storage/postgres/dao.go` and the new audit-log lookup paths.

## When To Apply

Apply the migration before starting:

- `gateway`
- `orchestrator`

The worker process does not open PostgreSQL directly, but it still depends on the schema being present when orchestrator-backed execution is used.

## How To Apply

Use the same DSN you place in `storage.postgres.dsn` inside `config.yaml` or `config.example.yaml`.

Example:

```bash
export DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/mcp_mobile_worker?sslmode=disable'
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/001_init.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/002_audit_logs.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/004_pat_tokens.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/005_hmac_keys.sql
```

## How To Verify

After applying the migration, verify that the required tables exist:

```bash
psql "$DATABASE_URL" -c '\dt sessions traces plan_events artifacts audit_logs pat_tokens hmac_keys'
```

You can also verify the expected indexes:

```bash
psql "$DATABASE_URL" -c '\di idx_sessions_* idx_traces_* idx_plan_events_* idx_artifacts_* idx_audit_logs_* idx_pat_tokens_* idx_hmac_keys_*'
```

You can also run the repository's self-contained replay check, which boots an isolated embedded PostgreSQL instance, applies every migration twice, and exercises the current DAO write paths:

```bash
go run ./cmd/migration_replay_check
```

## Notes

- The current schema intentionally matches the DAO's existing `[]byte` payload handling, so JSON-like blobs are stored in `BYTEA` columns.
- Session shutdown now uses row-level locking, and trace lifecycle changes now use optimistic compare-and-swap updates, so no additional schema columns were required for the concurrency hardening in this pass.
- Migration numbering intentionally leaves room for the future `003_traces_execution_state.sql` file planned by the v4.4 batch roadmap, so the auth-table migrations start at `004` and `005`.
- Future migrations can introduce richer typed columns once the DAO layer is updated to write and read them explicitly.

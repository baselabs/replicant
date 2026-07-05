# Replicant — AI Agent & Contributor Guide

How to work effectively in this repo. The Critical Rules are binding.

## What this is

A framework-agnostic Elixir CDC consumer for Postgres logical replication
(`pgoutput`). It consumes the logical stream, assembles committed transactions,
and delivers them synchronously to a pluggable sink — advancing the replication
slot only AFTER the sink has durably persisted. **Tenant-blind, Ash-agnostic,
classification-blind.** Multitenancy and classification live in the sibling
`ash_replicant` sink adapter, never here.

## Critical rules

**1. No row value in an error, log, or telemetry event.** Assume every value is
PII or a secret. The vendored decoder raises on malformed WAL and embeds raw
bytes; decode therefore runs behind `Replicant.Decoder.decode/1`, which catches
raises and scrubs raw bytes into a value-free `Replicant.Error`. Column names
stay **strings** — never `String.to_atom` (a wide or attacker-influenced schema
would exhaust the atom table). Telemetry metadata is allowlisted
(`Replicant.Telemetry`): LSNs, table names, counts, durations, error classes —
never values.

**2. Validate identifiers.** Slot and publication names reach SQL; they go
through `Replicant.Identifier.validate/1` (a strict Postgres-identifier
allowlist) before interpolation. A failure carries the invalid-SHAPE fact only,
never the offending string. This hardens walex's raw `'#{publication}'`
interpolation.

**3. Exactly-once is at-least-once + a transaction-watermark-idempotent sink.**
The watermark is the **commit LSN at transaction granularity** (every row in a
pgoutput proto-v1 transaction shares one commit LSN). Skip any transaction whose
`commit_lsn <= checkpoint`; upsert rows by table PK. There is no naked
exactly-once without two-phase commit or an idempotent sink — do not claim one.

**4. Unchanged TOAST is a sentinel, not a value.** An UPDATE that does not touch
a TOASTed column sends a sentinel. `%Change{}` surfaces it as a first-class
`unchanged: [col]` list; the sentinel never appears in `record`. Sinks must
leave those columns untouched on upsert. A *change* of replica identity or a
dropped column classifies as `:destructive` and halts fail-closed.

**5. Stay tenant-blind.** No multitenancy, scope, or classification logic enters
this lib. That boundary is the whole reason `replicant` and `ash_replicant` are
separate.

## Development workflow

    mix deps.get
    mix format
    mix credo --strict
    mix compile --warnings-as-errors
    mix test
    mix dialyzer
    # or all quality gates at once:
    mix quality

All gates must pass before a commit/PR. Update `CHANGELOG.md` under
`[Unreleased]`.

## Testing

- **Unit + real-byte conformance tests** (`test/**/*_test.exs`): no live server,
  no `postgrex` dependency. The decoder conformance suite decodes REAL captured
  `pgoutput` bytes (walex's MIT-licensed capture, inlined and credited) for every
  message type — including the unchanged-TOAST sentinel and all replica-identity
  modes. It never self-signs fixtures. An independent docker-PG16 capture
  (`test/integration/pg16_conformance_test.exs`) corroborates it against a live
  server.
- **Integration + crash-injection tests** (`test/integration/**`): gate
  on `REPLICANT_TEST_URL` pointing at a live PG16 with `wal_level=logical`;
  skip when unset. Spin PG16 with
  `docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p 5599:5432 postgres:16 -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10`
  then `export REPLICANT_TEST_URL="postgres://postgres@localhost:5599/postgres"`.
- **TDD:** write the test first.

## Docs & lifecycle-artifact policy

- **Tracked / published:** `AGENTS.md`, `README.md`, `CHANGELOG.md`,
  `CONTRIBUTING.md`, `usage-rules.md`, `LICENSE`, `NOTICE`.
- **Never tracked:** all brainstorm specs, plans, exec notes, reviews, and
  handoffs. They live under `/docs/`, which is **gitignored**. Do not move
  `/docs/` artifacts to the repo root.
- AI-tool state dirs (`.claude/`, `.serena/`, etc.) are gitignored.

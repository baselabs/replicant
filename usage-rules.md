# replicant usage rules

_A framework-agnostic Elixir CDC consumer for Postgres logical replication (`pgoutput`)._

## What replicant is (and is not)

- **Is:** a reliable CDC consumer. Decodes the `pgoutput` logical replication
  stream, assembles committed transactions, validates schema changes, and
  delivers each transaction to a pluggable sink with sink-owned,
  transaction-granularity exactly-once semantics.
- **Is not:** Ash-aware, tenant-aware, or classification-aware. Never put
  multitenancy or sensitive-data logic here — that is `ash_replicant`'s job.
- **Is not (yet):** a live streaming client. This version ships the offline
  core (decode / assemble / validate / redact). The
  `Postgrex.ReplicationConnection` that owns the slot and streams live WAL is a
  later slice.

## Public surface

- **`Replicant`** — the facade module. `t:lsn/0` (a `non_neg_integer` 64-bit
  LSN, `(file <<< 32) ||| offset`) and `lsn_to_string/1` (uppercase
  `"file/offset"` hex display, matching Postgres `pg_lsn`).
- **`Replicant.Transaction`** — an assembled, committed transaction: ordered
  changes plus the transaction's single `commit_lsn`.
- **`Replicant.Change`** — a single row change (`insert`/`update`/`delete`),
  the decoded `record`, and the `unchanged` list of TOASTed columns the source
  UPDATE did not touch (never a value — sinks must leave those columns alone).
- **`Replicant.SchemaChange`** — a detected DDL-shape change (column add/drop,
  type change, replica-identity change). Destructive changes halt fail-closed.
- **`Replicant.Sink`** — the behaviour a consumer implements: receives one
  `Replicant.Transaction` at a time and must durably persist it (or raise)
  before the slot advances past its `commit_lsn`.
- **`Replicant.Decoder`** — `decode/1` wraps the vendored `pgoutput` byte
  parser; catches and redacts any raise into a value-free `Replicant.Error`.
- **`Replicant.Assembler`** — groups decoded messages into
  `Replicant.Transaction`s by `commit_lsn`.
- **`Replicant.QueryBuilder`** — builds the identifier-validated SQL used to
  create/manage slots and publications.
- **`Replicant.Identifier`** — allowlist validation for slot, publication, and
  other identifiers that reach SQL.
- **`Replicant.Telemetry`** — value-free `:telemetry` spans (LSNs, table
  names, counts, durations, error classes — never row values).
- **`Replicant.Error`** — the typed, value-free error struct raised/returned
  at decode and validation boundaries.

## Non-negotiable rules

- **No row value in an error, log, or telemetry event.** Assume every value is
  PII or a secret. Column names are strings, never atoms (`String.to_atom` on
  a wide or attacker-influenced schema exhausts the atom table).
- **Validate identifiers.** Slot and publication names go through
  `Replicant.Identifier.validate/1` before reaching SQL. A failure carries the
  invalid-shape fact only, never the offending string.
- **Exactly-once is at-least-once + a transaction-watermark-idempotent sink.**
  The watermark is the commit LSN at transaction granularity — skip any
  transaction whose `commit_lsn <= checkpoint`; upsert rows by table PK.
  There is no naked exactly-once without two-phase commit or an idempotent
  sink; never claim one.
- **Unchanged TOAST is a sentinel, not a value.** It surfaces only as
  `Replicant.Change`'s `unchanged` list of column names, never in `record`.
  Sinks must leave those columns untouched on upsert.
- **Stay tenant-blind.** No multitenancy, scope, or classification logic
  belongs here — that boundary is the whole reason `replicant` and
  `ash_replicant` are separate libraries.

See `AGENTS.md` for the full working rules.

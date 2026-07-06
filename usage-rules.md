# replicant usage rules

_A framework-agnostic Elixir CDC consumer for Postgres logical replication (`pgoutput`)._

## What replicant is (and is not)

- **Is:** a reliable CDC consumer. Decodes the `pgoutput` logical replication
  stream, assembles committed transactions, validates schema changes, and
  delivers each transaction to a pluggable sink with sink-owned,
  transaction-granularity exactly-once semantics.
- **Is not:** Ash-aware, tenant-aware, or classification-aware. Never put
  multitenancy or sensitive-data logic here — that is `ash_replicant`'s job.
- **Is:** a live streaming client. Owns the replication slot via
  `Postgrex.ReplicationConnection`, acks only after the sink durably commits
  (ack-after-checkpoint), and halts fail-closed on slot invalidation — proven
  by a real-PG16 crash-injection suite (loss = 0, effect-dup = 0).

## Public surface

- **`Replicant`** — the facade module. `start_link/1` starts a supervised
  streaming pipeline (validates opts, enforces the go-forward guard) and
  `stop/1` tears one down; `t:lsn/0` (a `non_neg_integer` 64-bit LSN,
  `(file <<< 32) ||| offset`) and `lsn_to_string/1` (uppercase `"file/offset"`
  hex display, matching Postgres `pg_lsn`).
- **`Replicant.Transaction`** — an assembled, committed transaction: ordered
  changes plus the transaction's single `commit_lsn`.
- **`Replicant.Change`** — a single row change (`insert`/`update`/`delete`),
  the decoded `record`, and the `unchanged` list of TOASTed columns the source
  UPDATE did not touch (never a value — sinks must leave those columns alone).
- **`Replicant.SchemaChange`** — a detected DDL-shape change (column add/drop,
  type change, replica-identity change). Destructive changes halt fail-closed.
- **`Replicant.Sink`** — the behaviour a consumer implements: receives one
  `Replicant.Transaction` at a time and must durably persist it (or raise)
  before the slot advances past its `commit_lsn`. Optional snapshot callbacks
  (`handle_snapshot/2` + `handle_snapshot_complete/1`) let a sink be bootstrapped
  from a populated source: batches of `%Change{op: :snapshot}` rows (upsert by PK;
  clear the table when `first_for_table?` is true — a hard redo-safety obligation),
  then a durable handoff checkpoint at the snapshot's consistent point. In **lib mode**
  (`:checkpoint_store` configured) the library owns the checkpoint, so a sink implements
  only `handle_transaction/1` — `checkpoint/0` is optional there; its returned LSN is ignored.
- **`Replicant.Decoder`** — `decode/1` wraps the vendored `pgoutput` byte
  parser; catches and redacts any raise into a value-free `Replicant.Error`.
- **`Replicant.Assembler`** — groups decoded messages into
  `Replicant.Transaction`s by `commit_lsn`.
- **`Replicant.Connection`** — the `Postgrex.ReplicationConnection` that owns
  the replication slot: ack-after-checkpoint keepalive replies, async ack,
  slot-invalidation fail-closed halt, and the bounded in-flight window.
- **`Replicant.AssemblerServer`** — the serial process that applies the sink
  synchronously off the keepalive path.
- **`Replicant.Pipeline`** — the per-slot `:one_for_all` supervisor pairing a
  Connection with an AssemblerServer; started by `Replicant.start_link/1`.
- **`Replicant.Config`** — validates pipeline options + the start-mode guard
  (`go_forward_only` / `snapshot` / resume; `go_forward_only` + `snapshot` both
  set is refused as `:conflicting_start_mode`).
- **`Replicant.Snapshotter`** — reads a consistent snapshot of the publication's
  tables at the `EXPORT_SNAPSHOT` LSN (a `REPEATABLE READ` cursor on a separate
  connection) and pushes `%Change{op: :snapshot}` batches to the sink, behind a
  value-free error boundary; the `Connection` hands off to streaming at that LSN.
- **`Replicant.CheckpointStore`** — in **lib mode** (a `:checkpoint_store` option
  is present), a supervised GenServer owning one `replicant_checkpoints` row per slot:
  the library writes the checkpoint (`commit_lsn bigint`) to this durable Postgres table
  **after** the sink persists, so a non-transactional sink (files, S3, Kafka, external
  APIs) needs no atomic data+checkpoint unit. Value-free boundary; lazy table create +
  shape-probe. A store fault is bounded by two `:checkpoint_store` knobs — `max_retries`
  (default 5) and `retry_backoff_ms` (default 1000) — shared by both fault sites: a
  transient connect-read fault paces N fresh reconnects, a transient mid-stream write fault
  retries N (blocking the applier, so dup-bounded-to-one holds), then both **halt
  fail-closed** on exhaustion (`Supervisor.halt`, loss = 0). A permanent fault (schema
  mismatch / `:config_invalid`) halts immediately; `max_retries: 0` opts out of retry
  (halt-now). Each retry emits `[:replicant, :checkpoint_store, :retrying]`.
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
- **Lib mode is at-least-once, never effect-once.** With a `:checkpoint_store`, the
  library writes the checkpoint after the sink persists (checkpoint-after-persist), so a
  crash between persist and checkpoint re-delivers exactly one transaction on resume:
  **duplicate bounded to one transaction, never loss.** A non-transactional sink cannot
  dedup — do not claim effect-once for it.
- **Batching is opt-in and lib-mode only.** `checkpoint_store: [batch: [max_transactions: N, max_delay_ms: T]]`. It batches the checkpoint write + ack, NOT sink delivery (`handle_transaction/1` is still per-transaction). A crash or graceful stop mid-batch re-delivers up to one batch — size `max_transactions` for your dup tolerance. Do not set `:batch` at the top level (it belongs under `:checkpoint_store`; a misplaced top-level `:batch` is rejected at start).
- **Unchanged TOAST is a sentinel, not a value.** It surfaces only as
  `Replicant.Change`'s `unchanged` list of column names, never in `record`.
  Sinks must leave those columns untouched on upsert.
- **Stay tenant-blind.** No multitenancy, scope, or classification logic
  belongs here — that boundary is the whole reason `replicant` and
  `ash_replicant` are separate libraries.

See `AGENTS.md` for the full working rules.

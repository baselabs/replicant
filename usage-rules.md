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
  changes plus the transaction's single `commit_lsn`, and (when `messages: true`
  is enabled) any **transactional** logical-decoding messages in `messages`
  (a `[Replicant.Decoder.Messages.Message.t()]` — these ride the txn and inherit
  its `commit_lsn` effect-once dedup). Ordinarily `changes` is a
  `List`; for an oversized **spilled** streamed transaction (opt-in
  `streaming: [spill: [dir: …, max_spill_bytes: …]]`) it is a lazy, single-pass,
  disk-backed `Enumerable` (`Replicant.Spill.Reader`) valid only *during* the
  `handle_transaction/1` / `handle_batch/1` call — iterate it with `Enum`/`Stream`,
  never `length/1` / `Enum.to_list/1` (which force the whole transaction back into
  RAM, defeating spill), and do not retain it past the call. Spill emits value-free
  `[:replicant, :stream, :spilled]` (a tail flushed to disk) and, when disk usage
  would exceed `max_spill_bytes`, `[:replicant, :stream, :spill_exhausted]` before
  the fail-closed halt — attach to alert operators on exhaustion.
- **`Replicant.Change`** — a single row change (`insert`/`update`/`delete`),
  the decoded `record`, and the `unchanged` list of TOASTed columns the source
  UPDATE did not touch (never a value — sinks must leave those columns alone).
- **`Replicant.SchemaChange`** — a detected DDL-shape change (column add/drop,
  type change, replica-identity change). Destructive changes halt fail-closed.
- **`Replicant.SessionIdentity`** — the system identifier, timeline, current LSN,
  and database returned by `IDENTIFY_SYSTEM` on the exact replication connection.
  A source-bound sink implements `handle_session_identity/2` and returns `:ok`
  only after accepting the identity plus context slot/publications. This callback
  always precedes checkpoint lookup and runs again on reconnect; a separate
  preflight connection is not authoritative.
- **`Replicant.Sink`** — the behaviour a consumer implements. In the default **sink-owned
  transaction mode** (`batch_delivery` not set), receives one `Replicant.Transaction` at a time
  and must durably persist it (or raise) before the slot advances past its `commit_lsn`. When
  **sink-owned batch delivery** is enabled (`batch_delivery: [max_transactions: N, max_delay_ms: T]`),
  receives N committed transactions in a single `handle_batch/1` call and must persist all rows
  + checkpoint atomically — the transaction is atomic and the effect-once guarantee (dup=0, loss=0)
  rests on it. Transactions arrive in ascending `commit_lsn` order; the sink must skip any
  `commit_lsn <= checkpoint` and upsert rows by table PK. A non-`{:ok, _}` return (or a
  raise/throw/exit) halts the pipeline fail-closed; the batch is discarded un-acked and
  re-delivered on resume, deduped to zero net effect by the idempotent sink. Optional snapshot
  callbacks (`handle_snapshot/2` + `handle_snapshot_complete/1`) let a sink be bootstrapped
  from a populated source: batches of `%Change{op: :snapshot}` rows (upsert by PK;
  clear the table when `first_for_table?` is true — a hard redo-safety obligation),
  then a durable handoff checkpoint at the snapshot's consistent point. In **lib mode**
  (`:checkpoint_store` configured) the library owns the checkpoint, so a sink implements
  only `handle_transaction/1` — `checkpoint/0` is optional there; its returned LSN is ignored.
  Batch delivery is mutually exclusive with lib mode.
- **`handle_message/2`** (opt-in, `messages: true`) — delivers a **non-transactional**
  logical-decoding message (`pg_logical_emit_message` with `transactional => false`).
  The guarantee is **at-least-once, NOT effect-once** (see the rule below): a reconnect
  between the `{:ok}` return and the checkpoint advancing can re-deliver the same message.
  On `:ok`/`{:ok, _}` the library advances the checkpoint to the message's LSN; a non-`:ok`
  return (or raise/throw/exit) halts fail-closed. `context` carries `%{lsn: lsn}`. A
  `messages: true` config whose sink lacks this callback is rejected at start
  (`:messages_unsupported`). **Transactional** messages do NOT route here — they ride
  `%Transaction.messages` and inherit the txn path's effect-once.
- **Incremental snapshot** (`snapshot: [mode: :incremental]`) — a resumable, chunked backfill
  for large tables, interleaved with the live stream. Chunks arrive through the SAME
  `handle_snapshot/2` (same `first_for_table?` redo-safety obligation; `handle_snapshot_complete/1`
  is NOT used — completion rides a dedicated final `handle_snapshot/2` call). A **sink-owned**
  incremental sink must ALSO implement `snapshot_progress/0` (return the opaque `ctx.progress`
  token it persisted atomically with each chunk); **lib mode** carries progress in the checkpoint
  store, so only `handle_snapshot/2` is required. A concurrent write to a backfilling row wins over
  its stale chunk row (collision-corrected); PK-less tables fall back to a bounded whole-table redo.
- **`Replicant.Decoder`** — `decode/1` wraps the vendored `pgoutput` byte
  parser; catches and redacts any raise into a value-free `Replicant.Error`.
- **`Replicant.Assembler`** — groups decoded messages into
  `Replicant.Transaction`s by `commit_lsn`.
- **`Replicant.Connection`** — the `Postgrex.ReplicationConnection` that owns
  the replication slot: actual-session identity before checkpoint lookup,
  ack-after-checkpoint keepalive replies, async ack,
  slot-invalidation fail-closed halt, the bounded in-flight window, and the
  replication-command-error watchdog (below).
- **`Replicant.AssemblerServer`** — the serial process that applies the sink
  synchronously off the keepalive path.
- **`Replicant.Pipeline`** — the per-slot `:one_for_all` supervisor pairing a
  Connection with an AssemblerServer; started by `Replicant.start_link/1`.
- **`Replicant.Config`** — validates pipeline options + the start-mode guard
  (`go_forward_only` / `snapshot` / resume; `go_forward_only` + `snapshot` both
  set is refused as `:conflicting_start_mode`). `publication:` accepts a single
  validated name **or a list** (`["p1", "p2"]`) for multi-publication; every name
  is identifier-validated, the connect chain fails closed if any requested pub is
  absent (`:publication_check`), and `messages: true` is the opt-in for
  logical-decoding messages (rejected `:messages_unsupported` if the sink lacks
  `handle_message/2`).
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
- **Replication-command-error watchdog** — the top-level `max_command_retries` option
  (default 5) bounds a persistent PRE-FRAME command error — `CREATE_REPLICATION_SLOT`
  failing because the server's replication slots are exhausted, a slot already active for
  another consumer, or a forward-incompatible result shape — which otherwise reconnects
  forever via `auto_reconnect`. After `max_command_retries` failed connect cycles without
  the stream establishing, the pipeline **halts fail-closed and stays idle**, emitting
  value-free `[:replicant, :connection, :command_error_halt]` (`attempt`/`max_retries`/
  `slot_name`); `max_command_retries: 0` halts on the first fault. The bound is a CYCLE
  count, not a wall-clock time. Sibling of the store-retry bound above but a separate
  budget (a store outage never trips it). Only PRE-FRAME errors are bounded: once the
  stream is flowing the counter resets on the first replication frame, so a later transient
  outage self-heals, and a server that is simply down (connection refused, never
  establishes) keeps retrying untouched.
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
- **Sink-owned atomic batch delivery (`handle_batch/1`) preserves effect-once.** The `batch_delivery: [max_transactions: N, max_delay_ms: T]` config (top-level, sink-owned only; mutually exclusive with `:checkpoint_store`) routes delivery through `handle_batch/1` instead of `handle_transaction/1`. The HARD OBLIGATION is that the data + checkpoint write is ATOMIC — the effect-once guarantee (dup=0 across mid-batch teardown) rests on it. Transactions arrive in ascending `commit_lsn` order; the sink must skip any `commit_lsn <= checkpoint` and upsert rows by table PK, exactly as `handle_transaction/1` does. A non-`{:ok, _}` return (or a raise/throw/exit) halts the pipeline fail-closed; the batch is discarded un-acked and re-delivered on resume, deduped to zero net effect by the idempotent sink.
- **Failover slots are opt-in and PG17+.** `failover: true` creates the replication slot with
  Postgres's `FAILOVER` option so it syncs to physical standbys, letting a pipeline resume
  against a promoted standby with zero loss (the slot's `confirmed_flush` position carries
  over). PG16 does not support `FAILOVER` slots — passing `failover: true` there halts
  fail-closed (`{:config, :failover_unsupported}`) instead of silently ignoring the option.
  Never point Replicant at an unpromoted standby's synced slot — it halts fail-closed
  (`{:slot_synced_unpromoted}`) instead of retry-looping against a slot it cannot yet consume.
- **Multi-publication is opt-in via a list.** `publication: "p"` stays the byte-unchanged default;
  `publication: ["p1", "p2"]` streams the union. Every requested publication must exist — a missing
  one halts fail-closed at connect (`:publication_check`) rather than silently streaming the subset
  (a `START_REPLICATION` that names a missing pub streams only the found set). pgoutput de-dupes
  overlapping tables across pubs on the wire.
- **Logical-decoding messages: state the guarantee honestly.** `messages: true` is opt-in. A
  **transactional** message (`transactional => true` to `pg_logical_emit_message`) rides
  `%Transaction.messages` and is **effect-once** (inherits the txn `commit_lsn` dedup). A
  **non-transactional** message routes to `handle_message/2` and is **at-least-once — duplicates
  are possible on reconnect** (no dedup key). Never claim effect-once for the non-transactional path.
  A message's `content` and `prefix` are **user bytes** (Critical Rule 1) — never log them or surface
  them in telemetry.
- **Unchanged TOAST is a sentinel, not a value.** It surfaces only as
  `Replicant.Change`'s `unchanged` list of column names, never in `record`.
  Sinks must leave those columns untouched on upsert.
- **Stay tenant-blind.** No multitenancy, scope, or classification logic
  belongs here — that boundary is the whole reason `replicant` and
  `ash_replicant` are separate libraries.

See [`docs/INVARIANTS.md`](docs/INVARIANTS.md) for the full published rules.

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
  `(file <<< 32) ||| offset`), `lsn_to_string/1` (uppercase `"file/offset"`
  hex display, matching Postgres `pg_lsn`), and `lsn_from_string/1`
  (`{:ok, lsn} | {:error, :invalid_lsn}` since 1.3.0 — a public function
  never raises, so a caller's input can never reach a crash report).
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
- **`handle_slot_origin/2`** (optional) — receives the slot's typed consistent-point **origin**
  LSN a go-forward stream begins at, on every connect/reconnect before `START_REPLICATION`, for
  a go-forward **append** consumer. `context` is `%{slot_name, reused?}` (value-free): `reused?:
  false` → the `CREATE_REPLICATION_SLOT` consistent_point (new slot); `reused?: true` → the
  greater of the durable checkpoint and the slot's live `confirmed_flush_lsn` (the effective
  `START_REPLICATION` origin for a resumed slot).
  Return `:ok` to proceed; any other return/raise/throw/exit halts fail-closed
  (`:slot_origin_rejected`) — a veto for an origin that gapped past the consumer's last appended
  LSN, instead of silently skipping WAL. A missing/NULL/malformed database origin halts before the
  callback with `:slot_origin_unavailable`; origin `0` is never fabricated. A sink without the
  callback is unaffected (no extra query). An `:append_log` sink does not perform the generic
  filtered-WAL idle advance, so a reused origin ahead of its checkpoint is a real gap. Publish a
  normal heartbeat transaction on a quiet append publication to advance the checkpoint and release
  retained WAL.
- **`sink_kind/0`** (optional) — declares how the sink consumes the stream:
  the default (callback absent) is a **state mirror** (replays/upserts state; the generic
  idle keepalive may advance the slot over filtered WAL), `:append_log` is a **go-forward
  append** consumer (never acknowledges past its durable delivered checkpoint, so an
  out-of-band slot advance is detectable as a gap on reconnect). The same split governs
  **empty transactions** (`BEGIN`/`COMMIT` with zero published changes, still streamed by
  pre-PG15 servers; PG15+ skips them server-side): they are never delivered to the sink —
  a state mirror's slot acks over the proven-empty WAL, an append log retains it. This is
  the knob behind the 1.2.3 append-log ack behavior.
- **`handle_schema_change/2`** (optional) — consulted on a **destructive** schema change
  (a dropped column, a replica-identity change, a narrowing type change). Default
  behavior without the callback: additive changes apply automatically, destructive
  changes halt fail-closed. With it, return `:ok` to accept the change (you are
  asserting your sink can handle the new shape) or `{:error, reason}` to halt
  value-free — a migration-window hook for sinks that can adapt their own schema.
  `context` is value-free.
- **Incremental snapshot** (`snapshot: [mode: :incremental]`) — a resumable, chunked backfill
  for large tables, interleaved with the live stream. Chunks arrive through the SAME
  `handle_snapshot/2` (same `first_for_table?` redo-safety obligation; `handle_snapshot_complete/1`
  is NOT used — completion rides a dedicated final `handle_snapshot/2` call). A **sink-owned**
  incremental sink must ALSO implement `snapshot_progress/0` (return the opaque `ctx.progress`
  token it persisted atomically with each chunk, or `:backfill_pending` after durably arming the
  backfill but before the first chunk); **lib mode** carries progress in the checkpoint store, so
  only `handle_snapshot/2` is required. A pending restart reads the live slot origin and resumes
  discovery instead of silently selecting stream-only delivery. A concurrent write to a backfilling row wins over
  its stale chunk row (collision-corrected). Keyed drop-cap contention and PK-less whole-table
  contention both halt `:snapshot_table_contended` after three discarded attempts; reconnects do
  not consume that reader-local budget.
- **`Replicant.Decoder`** — `decode/1` wraps the vendored `pgoutput` byte
  parser; catches and redacts any raise into a value-free `Replicant.Error`.
- **`Replicant.Assembler`** — groups decoded messages into
  `Replicant.Transaction`s by `commit_lsn`.
- **`Replicant.Connection`** — the `Postgrex.ReplicationConnection` that owns
  the replication slot: actual-session identity before checkpoint lookup,
  ack-after-checkpoint keepalive replies (state mirrors may idle-advance over
  filtered WAL; append logs never do), async ack,
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
  `handle_message/2`). `streaming: [max_concurrent_txns: N]` (default 64) opts into
  proto-v2 in-progress transaction streaming; its nested `spill: [dir:, max_spill_bytes:]`
  opts into consumer-side disk spill for oversized streamed transactions.
  `max_inflight_lag` (default 64 MiB) bounds how far the received WAL frontier may
  run ahead of the durable checkpoint before the sink is "too slow"
  (see the halt-reason table below).
- **`Replicant.Snapshotter`** — reads a consistent snapshot of the publication's
  tables at the `EXPORT_SNAPSHOT` LSN (a `REPEATABLE READ` cursor on a separate
  connection) and pushes `%Change{op: :snapshot}` batches to the sink, behind a
  value-free error boundary; the `Connection` hands off to streaming at that LSN.
- **`Replicant.CheckpointStore`** — in **lib mode** (a `:checkpoint_store` option
  is present), a supervised GenServer owning one `replicant_checkpoints` row per slot:
  the library writes the checkpoint (`commit_lsn bigint`) to this durable Postgres table
  **after** the sink persists, so a non-transactional sink (files, S3, Kafka, external
  APIs) needs no atomic data+checkpoint unit. Value-free boundary; lazy table create +
  shape-probe. With `snapshot: [mode: :incremental]` the store ALSO owns a second
  table — `progress_table` (default `"replicant_snapshot_progress"`, one row per slot)
  — carrying the opaque, value-free backfill progress token. Both table names are
  configurable (`checkpoint_store: [table: ..., progress_table: ...]`) and both are
  created lazily. A store fault is bounded by two `:checkpoint_store` knobs — `max_retries`
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

## Value casting — what `record` holds

`%Change{}.record` values are cast from Postgres's text output (ADR-0008 for the
full contract). The guarantees a sink can rely on:

- **Lenient by default.** A value that fails its type's parse is delivered as the
  original string, never dropped or raised — EXCEPT a small documented raise-site
  set (malformed `numeric`, non-hex `bytea`) that scrubs to a value-free
  `:decode_failure` halt at the decode boundary: genuinely-malformed input is a
  halt, an unparseable-but-legitimate value is a string.
- **Multidimensional arrays nest.** Every casted array type (`numeric[][]`,
  `timestamptz[][]`, `jsonb[][]`, `bool[][]`, …) delivers nested lists with the same
  per-element semantics as its scalar clause. `NULL` elements are `nil` at any depth
  (since 1.3.0; before it, only `int[]`/`float[]` recursed and other 2-D arrays
  halted). `interval[]` and `timetz[]` deliver raw-string elements, mirroring their
  scalar raw-string clauses (since 1.3.0; `interval[]` previously fell into the
  integer-array clause and was silently truncated to its leading integer).
- **`money` is locale-honest.** Money output follows the server's `lc_monetary`.
  The strict C/en-US shapes (`$1,234.56`, `-$5.00`, `$1234567.89`) deliver a
  `Decimal`; anything else (e.g. a `de_DE` `"1.234,56"`) delivers the **original
  string** — never a silently-wrong Decimal, never a raise. Accept
  `Decimal | String.t()` for money columns, and normalize the string form in your
  sink if you run a non-C locale (since 1.3.0).
- **`timetz` delivers the raw server string** (fractional seconds and offset
  preserved; there is no Elixir type for time-with-offset — same as `interval`).
  Before 1.3.0 the offset and fraction were silently truncated.
- **`type_modifier` is a signed int32** (`-1` is Postgres's "no modifier" marker,
  not `4294967295`).

## Telemetry reference

Every event is value-free by construction: metadata keys are closed to
`commit_lsn, change_count, byte_size, lag_ms, duration, attempt, max_retries,
transactional, table, slot_name, reason, error_class, kind`, each with a
value-shape contract enforced at emission (`Replicant.Telemetry` raises on an
off-list key or a wrong shape — never ships a row value). All thirty events:

| Event | Fires when | Measurements | Metadata |
| --- | --- | --- | --- |
| `[:replicant, :connection, :connected]` | replication connection established (each connect/reconnect) | — | `kind` (`:primary`/recovery kind) |
| `[:replicant, :connection, :disconnected]` | connection dropped — **also the only signal of the `:sink_too_slow` lag halt** (with `reason: :sink_too_slow` and a signed `lag` measurement in bytes) | `lag` (halt only) | `reason` (halt only) |
| `[:replicant, :connection, :slot_active]` | the slot is created/owned and streaming begins | — | — |
| `[:replicant, :connection, :slot_invalidated]` | slot invalidation / fail-closed config rejection | — | `reason` (`:failover_unsupported`, `:publication_missing`, invalidation class) |
| `[:replicant, :connection, :session_identity_rejected]` | the sink vetoed the replication-session identity | — | `reason: :session_identity_rejected` |
| `[:replicant, :connection, :command_error_halt]` | the pre-frame command-error watchdog exhausted its budget | — | `attempt`, `max_retries`, `slot_name` |
| `[:replicant, :checkpoint, :advanced]` | the checkpoint advanced (per txn, async ack, or idle advance) | — | `commit_lsn` (`kind: :idle` on the idle-advance path) |
| `[:replicant, :checkpoint_store, :read]` | lib mode: checkpoint read at connect | — | `slot_name`, `commit_lsn` (nil on a fresh slot) |
| `[:replicant, :checkpoint_store, :written]` | lib mode: checkpoint durably written | — | `slot_name`, `commit_lsn` |
| `[:replicant, :checkpoint_store, :batch_flushed]` | lib mode: a batched checkpoint window flushed | — | `slot_name`, `change_count`, `byte_size` (LSN span) |
| `[:replicant, :checkpoint_store, :retrying]` | a transient store fault is being retried | — | `slot_name`, `attempt`, `max_retries` |
| `[:replicant, :checkpoint_store, :failed]` | a store fault halted (retry exhaustion or permanent) | `duration` (mid-stream) | `slot_name`, `reason` |
| `[:replicant, :transaction, :assembled]` | a committed transaction was assembled for delivery | — | `commit_lsn`, `change_count`, `byte_size` |
| `[:replicant, :sink, :committed]` | the sink returned `{:ok, lsn}` for a transaction | `duration` | `commit_lsn` |
| `[:replicant, :sink, :batch_committed]` | `handle_batch/1` committed an atomic batch | `duration` | `commit_lsn`, `change_count`, `reason` |
| `[:replicant, :sink, :failed]` | the sink returned an error / raised (halt follows) | `duration` | `reason` (`:sink_failed`, `:spill_io_failed`, …) |
| `[:replicant, :message, :received]` | a logical-decoding message arrived (`messages: true`) | — | `commit_lsn`, `byte_size`, `transactional` |
| `[:replicant, :schema_change, :additive]` | an additive schema change applied automatically | — | `table`, `kind: :additive` |
| `[:replicant, :schema_change, :halted]` | a destructive schema change halted (or `handle_schema_change/2` vetoed) | — | `table`, `kind: :destructive` |
| `[:replicant, :snapshot, :started]` | a snapshot/backfill started (point-in-time or incremental floor) | — | `commit_lsn` |
| `[:replicant, :snapshot, :resumed]` | an incremental backfill resumed from durable progress | — | `slot_name` |
| `[:replicant, :snapshot, :table_completed]` | one table finished snapshotting | — | `table`, `change_count` |
| `[:replicant, :snapshot, :chunk_completed]` | one incremental chunk applied | — | `table`, `change_count` |
| `[:replicant, :snapshot, :chunk_retried]` | a chunk was discarded for contention and will retry | — | `table`, `reason: :snapshot_table_contended` |
| `[:replicant, :snapshot, :completed]` | the snapshot finished and handed off to streaming | `duration` | `commit_lsn`, `change_count` |
| `[:replicant, :snapshot, :failed]` | the snapshot halted (value-free boundary) | — | `reason` (`:snapshot_failed`, `:snapshot_table_contended`, …) |
| `[:replicant, :stream, :committed]` | a proto-v2 streamed transaction committed | — | `commit_lsn`, `byte_size` (`change_count` on the folded path) |
| `[:replicant, :stream, :aborted]` | a streamed transaction aborted | — | `reason: :stream_abort` |
| `[:replicant, :stream, :spilled]` | an oversized streamed transaction's tail spilled to disk | — | `byte_size`, `change_count` |
| `[:replicant, :stream, :spill_exhausted]` | the disk ceiling was reached (halt follows) | — | `byte_size`, `reason: :spill_exhausted` |

The two-column trap to know: the **lag halt** (`:sink_too_slow`) rides
`[:replicant, :connection, :disconnected]`, not a dedicated event — alert on
`reason: :sink_too_slow`, not on the event name alone.

## Halt reasons (operator reference)

Every failure mode halts **fail-closed** — the pipeline stops and stays idle
rather than dropping data or reconnecting forever. Restart resumes from the
durable checkpoint (loss = 0; duplicates bounded by the mode's contract).

| Halt signal | Cause | Operator action |
| --- | --- | --- |
| `[:replicant, :sink, :failed]` | the sink returned an error or raised | fix the sink/store it writes; restart the pipeline |
| `:disconnected` + `reason: :sink_too_slow` | received WAL ran `max_inflight_lag` (default 64 MiB) past the durable checkpoint | speed the sink up, batch (`batch_delivery`), or raise the bound |
| `[:replicant, :connection, :slot_invalidated]` | the slot was invalidated (`wal_status = 'lost'`), a requested publication vanished, or failover config was rejected | recreate the slot (accept the re-stream from its origin — see `handle_slot_origin/2` for append sinks); restore/rename the publication; drop `failover:` on PG<17 |
| `[:replicant, :connection, :command_error_halt]` | persistent pre-frame command errors (slot exhaustion, slot already active, forward-incompatible result) | free a replication slot / disconnect the other consumer; then restart |
| `[:replicant, :connection, :session_identity_rejected]` | the sink vetoed the actual replication-session identity | point at the intended source, or update the sink's accepted identity |
| `:slot_origin_rejected` / `:slot_origin_unavailable` | an append sink vetoed the go-forward origin / the slot's origin state was missing or malformed | reconcile the append log with the origin gap, then restart |
| `[:replicant, :checkpoint_store, :failed]` | the lib-mode checkpoint store faulted past its retry budget (or a permanent schema/config fault) | restore the store, fix the schema/config; restart |
| `[:replicant, :schema_change, :halted]` | a destructive schema change (dropped column, replica-identity change, narrowing type) — or `handle_schema_change/2` vetoed | adapt the sink (or implement the callback to accept), then restart |
| `[:replicant, :snapshot, :failed]` with `:snapshot_table_contended` | a backfilling table stayed hot through three contention attempts | backfill during a quieter window, or shrink `chunk_rows` |
| `[:replicant, :snapshot, :failed]` (other) | the snapshot reader faulted (incl. connection faults during a backfill) | check source connectivity; restart — the backfill resumes from durable progress |
| `[:replicant, :stream, :spill_exhausted]` | a spilled transaction exceeded `max_spill_bytes` | free disk / raise `max_spill_bytes` / shrink the oversized transaction |
| `:decode_failure` | genuinely-malformed WAL/cast input at the value-free boundary | check upstream WAL/plugin integrity — this is never ordinary data |
| `:decoder_protocol_unsupported` | the pglogical startup message reported a protocol range outside what the decoder negotiates (protocol 1) | mismatched pglogical build; pin pglogical 2.x, then restart |
| `:decoder_lsn_missing` | a wal2json commit document arrived without a commit LSN (`include-lsn` carries it on every supported build) | upstream/plugin integrity — never ordinary data |
| `[:replicant, :connection, :slot_invalidated]` + `reason: :decoder_option_unsupported` | the installed `pglogical_output`/`wal2json` build rejects a requested option (pre-flight-probed at connect, ADR-0009) | upgrade the output plugin to a build carrying the option (`wal2json` ≥ 2.6 for `numeric-data-types-as-string`), then restart |
| `:decoder_unsupported_on_server` | `decoder: :pgoutput` against a pre-10 server (no `pgoutput`, no publications) | use `decoder: :pglogical` or `decoder: :wal2json` (ADR-0009), or upgrade the server |
| `:decoder_table_missing` | a table configured for the `:wal2json` decoder (or discovered in a `:pglogical` replication set) does not exist on the server at connect | create the table / fix the configured table list, then restart |
| `{:decoder, :table_keyless}` | a table configured for the `:wal2json` decoder has no replica-identity index and `REPLICA IDENTITY ≠ FULL` at connect — its updates/deletes would be silently dropped plugin-side | add a PK/identity index, set `REPLICA IDENTITY FULL`, or — for a genuinely insert-only table — set `allow_keyless_tables: true` |
| `:schema_change` `:destructive` under wal2json | a configured table's column was dropped server-side: detected at the change (insert, or a fixed-width column on update) or within `schema_check_interval` (a TOASTable column on an update-only table) | restore or migrate the sink schema, then restart |
| `:slot_synced_unpromoted` | `failover: true` against a standby whose synced slot is not yet promoted | promote the standby (or point at the primary), then restart |

Start-time rejections (no pipeline starts, nothing halts): `:invalid_identifier`,
`:invalid_sink`, `:config_invalid`, `:conflicting_start_mode`,
`:go_forward_required`, `:snapshot_unsupported`, `:batch_unsupported`,
`:messages_unsupported`, `:decoder_capability_unsupported` (a configured capability
the chosen decoder cannot express — `streaming:`/`failover:` off pgoutput, `messages:`
on pglogical — ADR-0009), `{:config, :failover_unsupported}`, and
`{:error, :invalid_start_lsn}` from the query builder (1.3.0).

## Decoder selection (ADR-0009)

`decoder:` selects the logical-decoding output plugin: `:pgoutput` (the default,
PostgreSQL 15-18, byte-identical to 1.3.0; the plugin matrix also runs it on 12), `:pglogical` (the `pglogical_output`
binary protocol, pglogical 2.x, PostgreSQL 9.6-14) or `:wal2json` (JSON format
version 2, wal2json ≥ 2.6, PostgreSQL 9.6-14). The sink contract, the `commit_lsn`
watermark, both checkpoint modes and every halt keep their semantics across decoders.
The table set is named per decoder — `publication:` for pgoutput,
`replication_sets:` for pglogical, `tables: [{schema, table}]` for wal2json — and the
wrong key for the chosen decoder is rejected at start. The plugin differences that
remain are all fail-closed, none silent (OBSERVED from the plugins' sources):

- **Keyless tables.** An update/delete on a table with no replica-identity index and
  `REPLICA IDENTITY ≠ FULL` never reaches any stream (pgoutput publications refuse the
  write outright with 55000; pglogical refuses such tables in update-carrying
  replication sets; wal2json filters them plugin-side with only a server WARNING). A
  wal2json pipeline therefore **halts at start** (`{:decoder, :table_keyless}`) for a
  configured keyless table; a genuinely insert-only table declares
  `allow_keyless_tables: true` (wal2json-only) to accept that U/D on such tables are
  invisible to the stream.
- **Dropped columns.** pgoutput and pglogical re-emit relation metadata after DDL, so
  the drop classifies `:destructive` immediately. wal2json format 2 has no relation
  messages, so Replicant splits the ambiguous absence: a cached column missing from an
  INSERT (inserts carry every live column) or missing from an UPDATE when its type is
  fixed-width (int/float/bool/date/time/timestamp/interval/uuid — never stored
  out-of-line) halts `:destructive` at the change; the residual case (a dropped
  TOASTable column on an update-only table, wire-identical to the unchanged-TOAST
  sentinel) is bounded by the periodic catalog re-read `schema_check_interval`
  (default `30_000` ms, wal2json-only) — detection within one interval, through the
  same `:destructive` classification.
- **`type_modifier`** carries pgoutput's raw atttypmod where the plugin's stream
  expresses it.

Non-transactional messages stay **at-least-once** on every decoder — that is the
paradigm, not a plugin gap (see invariant 3).

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

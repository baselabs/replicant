# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Reconcile the README, contributor guide, and durable roadmap with the current
  `replicant` 0.3.1 and `ash_replicant` 0.3.0 releases while retaining older
  lifecycle rows as explicitly historical evidence. Remove public-roadmap links
  into ignored lifecycle artifacts and repair release comparison references
  through 0.3.1.

## [0.3.1] - 2026-07-14

### Changed

- **Docs.** Documented the A6 command-error watchdog on every surface it was missing:
  a "Resilience knobs" reference section in the getting-started Livebook (grouping
  `max_inflight_lag`, checkpoint-store retry, `max_command_retries`, and `failover`), and
  the `max_command_retries` option + `[:connection, :command_error_halt]` event in
  `usage-rules.md`. No library API change (the watchdog itself shipped in 0.3.0).

## [0.3.0] - 2026-07-14

### Added

- **Replication-command-error watchdog (`max_command_retries`, default 5).** A persistent
  pre-frame replication-command error (e.g. `CREATE_REPLICATION_SLOT` failing because the
  server's replication slots are exhausted, or a slot already active for another consumer)
  previously reconnected forever via `auto_reconnect`. The pipeline now halts fail-closed and
  stays idle after `max_command_retries` failed connect cycles without the stream establishing,
  emitting `[:replicant, :connection, :command_error_halt]` (value-free metadata:
  `attempt`/`max_retries`/`slot_name`). `max_command_retries: 0` halts on the first fault.
  Transient outages that occur once the stream is flowing still self-heal (the counter resets
  on the first replication frame), and a down server keeps retrying untouched. The bound is a
  cycle count, not a wall-clock time. (Behavior change: persistent pre-frame command errors
  now halt instead of livelocking.)

## [0.2.2] - 2026-07-14

### Added

- **Getting-started Livebook.** `notebooks/getting_started.livemd` — a runnable, self-verifying
  interactive tour: it starts a live pipeline, streams `INSERT`/`UPDATE`/`DELETE` through a small
  in-notebook sink, then demonstrates the unchanged-TOAST sentinel, transaction-granularity
  exactly-once, snapshot/backfill, and logical-decoding messages. Rendered on HexDocs (with a
  "Run in Livebook" badge) and shipped in the Hex package. Its code is executed against a live
  PG16/PG17 on every CI run (`test/integration/livebook_getting_started_test.exs`), so it can
  never drift from the library. No library API change.

## [0.2.1] - 2026-07-14

### Changed

- Packaging: `AGENTS.md` is no longer included in the published Hex tarball — it is an
  agent-contract/meta file, not part of the library's public documentation. No code change.

## [0.2.0] - 2026-07-14

### Added

- **Idle-slot heartbeat / ack-advance.** On a keepalive with zero transactions in flight, the
  confirmed-flush LSN advances to `wal_end`, so a quiet-but-filtered publication no longer pins
  WAL indefinitely (the #1 real-world logical-replication incident class). Always on, no knob; the
  advance is gated on a transaction-boundary predicate (no open transaction, no in-flight streamed
  txn, checkpoint ≥ last commit) so it can never ack past an undelivered transaction or message.
- **Incremental (resumable) initial snapshot.** `snapshot: [mode: :incremental]` chunks the
  backfill and persists a resume token, so a large snapshot survives a restart without re-copying
  from scratch. The streaming window drops any snapshot chunk row a concurrent change already
  superseded (convergence-safe, effect-once); PK-update, delete, and truncate all taint the
  drop-set correctly.
- PostgreSQL 17+ forward-compatibility: reads the authoritative `invalidation_reason` slot
  column (plus `wal_status`/`conflicting`) on PG17+ for complete invalidation detection.
- Opt-in `failover: true` for PG17 failover slots (HA resume on a promoted standby). Halts
  fail-closed `{:config, :failover_unsupported}` on PG16.
- Fail-closed halt `{:slot_synced_unpromoted}` when pointed at an unpromoted standby's synced slot.
- GitHub Actions CI matrix testing PG16 and PG17.
- **Multi-publication per pipeline.** `publication:` accepts a single validated name **or a list**
  (`publication: ["p1", "p2"]`) to stream the union of several publications through one slot. Every
  name is identifier-validated; `start_replication/3` and the four discovery queries bind
  `DISTINCT ... pubname = ANY($1)`, and `publication_exists/1` interpolates a validated `IN (...)`
  list (the connect-chain simple-query protocol can't bind `$1`). A new connect-chain
  `:publication_check` step **halts fail-closed if the found-pubnames set ≠ the requested set** — a
  `START_REPLICATION` that names a missing publication would otherwise silently stream the subset.
  pgoutput de-dupes overlapping tables across publications on the wire.
- **Logical-decoding messages** (`pg_logical_emit_message`). Opt-in via `messages: true` (the sink
  must implement `handle_message/2`, else the pipeline is rejected at start as `:messages_unsupported`
  rather than silently dropping messages later). The guarantee is stated honestly per message kind:
  a **transactional** message (`transactional => true`) rides `%Transaction{messages: [...]}` and is
  **effect-once** (inherits the txn `commit_lsn` dedup); a **non-transactional** message routes to
  `handle_message/2` and is **at-least-once — duplicates possible on reconnect** (no dedup key). Two
  durability seams prevent silent loss: the idle-ack `track_txn` bump (§8.1 — a non-txn message in
  flight blocks the idle slot advance, so a keepalive cannot advance `confirmed_flush` past an
  undelivered message) and the batch-boundary `{:flush_before_message}` seam (§8.4 — a non-txn
  message flushes an open sink-owned batch in delivery order). New `%Message{}` struct
  (`transactional?`, `lsn`, `prefix`, `content`, `xid`, `ordinal`) decoded by v1 + streamed clauses;
  the `messages` flag threads through to `start_replication`. A message's `content` and `prefix` are
  user bytes (Critical Rule 1: never logged or surfaced in telemetry).

## [0.1.0] - 2026-07-08

First public release: the complete v1 zero-loss streaming CDC core plus every
delivery slice — initial snapshot/backfill, the lib-owned checkpoint store for
non-transactional sinks, batched checkpointing, sink-owned atomic batch
delivery, `pgoutput` proto-v2 in-progress-transaction streaming, and
consumer-side disk spill for oversized transactions — each closeout-reviewed
against a real-PG16 crash-injection suite (loss = 0, effect-dup = 0).

### Added — Consumer-side disk spill for oversized transactions (`replicant-streaming-spill`)

- **Consumer-side disk spill** (opt-in `streaming: [spill: [dir: …, max_spill_bytes: …]]`): a single
  in-progress streamed transaction **larger than `max_inflight_lag`** reassembles partly on disk and
  delivers effect-once as a lazy, single-pass, disk-backed `%Transaction{changes: …}` (an
  `Enumerable.t()`, `Replicant.Spill.Reader`) — instead of hitting the §4 fail-closed halt. Two
  ceilings: resident RAM `max_inflight_lag` (the spill trigger) and disk `max_spill_bytes`
  (`:spill_exhausted` halt; default `16 × max_inflight_lag`, `dir` default a `0700` subdir of
  `System.tmp_dir!()`). The §4 in-flight-lag numerator is `received − floor − spilled` (spilled bytes
  are on disk, not RAM, so a legitimately-spilling txn is not counted toward the RAM halt) compared
  to `max_inflight_lag + max_spill_bytes` (RAM + disk); resident RAM is bounded by the spill trigger.
  A new
  `Replicant.Spill` module is the sole `File.*` + at-rest boundary (`0700` dir / `0600` per-txn files,
  length-prefixed frames, per-slot startup sweep, value-free `:spill_io_failed`); spill files are
  ephemeral non-fsync'd scratch, deleted on commit/abort/reset/halt. **Delivery obligation:** a spilled
  txn's `changes` is single-pass and valid only during the `handle_transaction/1`/`handle_batch/1` call —
  iterate it with `Enum`/`Stream` (never `length`/`Enum.to_list`, which would force the whole txn into
  RAM); do not retain it past the call. Composes with the batch modes (a spilled txn buffered into a
  sink-owned batch migrates its file to the batch, delivered/deleted at flush). Emits
  `[:replicant, :stream, :spilled]` and `[:replicant, :stream, :spill_exhausted]` (both value-free).
  No in-lib encryption — a persistent `dir` is the operator's to
  place on a secure/encrypted volume and to clean on decommission.

### Added — Sink-owned atomic batch delivery (`replicant-batch-delivery`)

- **Sink-owned atomic batch delivery** (`batch_delivery:`): an optional `handle_batch/1` sink
  callback delivering N committed transactions as one atomic unit, amortizing a transactional
  sink's per-commit cost. Preserves effect-once (dup=0, loss=0). Opt-in via a top-level
  `batch_delivery: [max_transactions: 100, max_delay_ms: 1000]` (sink-owned only; mutually
  exclusive with `checkpoint_store`). Emits `[:replicant, :sink, :batch_committed]`.

### Added — Batched checkpointing (lib mode) (`replicant-batching`)

- **Batched checkpointing (lib mode).** Opt-in `checkpoint_store: [batch: [max_transactions: 100, max_delay_ms: 1000]]` defers the lib-owned checkpoint write + slot ack to once per batch, amortizing the per-transaction store round-trip. Sink delivery stays per-transaction; the sink contract is unchanged. loss=0 is unconditional; the crash/stop dup bound widens to one batch. An auto LSN-span cap (`max_inflight_lag/4`) keeps a batch from self-tripping the §4 in-flight-lag halt.

### Added — Bounded-retry-then-halt on checkpoint-store faults (`replicant-store-fault-retry`)

- `:checkpoint_store` gains two retry-policy keys: `max_retries` (default 5, non-negative
  integer; `0` = halt-now) and `retry_backoff_ms` (default 1000, positive integer). A
  **transient** connect-read store fault now paces `max_retries` FRESH reconnects (each
  re-runs the full connect chain, so slot invalidation is re-checked every attempt) instead
  of retrying UNPACED forever; a **transient** mid-stream checkpoint write fault retries
  `max_retries` times — blocking the serial applier, so **duplicate-bounded-to-one is
  preserved** — instead of halting on the first fault. A **permanent** fault
  (`:checkpoint_store_schema_mismatch` / `:config_invalid`) halts immediately, 0 retries. On
  exhaustion the pipeline halts fail-closed via `Supervisor.halt` (**loss = 0 preserved**).
  The default policy tolerates ~5s of store outage before halting. Sink-owned mode is
  untouched. Resolves the checkpoint-store closeout design-decision **F3**.
- New value-free telemetry `[:replicant, :checkpoint_store, :retrying]` (`slot_name`,
  `attempt`, `max_retries`) fires on each retry; `attempt` + `max_retries` added to the
  value-free telemetry allowlist.

### Added — Lib-owned checkpoint store (non-transactional sinks) (`replicant-checkpoint-store`)

- A second checkpoint **mode**, selected once in `Replicant.Config` by the presence of a
  `:checkpoint_store` option. **Absent** → today's sink-owned path, unchanged. **Present**
  → the library owns the checkpoint for a **non-transactional** sink (files, S3, Kafka,
  external APIs) by writing it to a durable Postgres table **after** the sink confirms
  persist. Guarantee: **at-least-once, duplicate bounded to one transaction, never loss —
  NOT effect-once** (a non-transactional sink cannot dedup).
- `Replicant.CheckpointStore` — a supervised GenServer over a normal Postgrex connection
  owning one `replicant_checkpoints` row per slot (`commit_lsn bigint`; validated table
  identifier, values bound `$n`). Lazy `CREATE TABLE IF NOT EXISTS` + `information_schema`
  shape-probe (a wrong pre-existing `commit_lsn` type halts `:checkpoint_store_schema_mismatch`),
  non-sync connect for boot resilience, all behind the value-free error boundary (Critical Rule 1).
- The checkpoint **write** lives in the `Assembler`'s `apply_sink`, after the sink returns
  `{:ok, _}` and **before** the `[:replicant, :sink, :committed]` telemetry / ack
  (checkpoint-after-persist); a write fault halts `:checkpoint_store_failed`, never announcing
  commit. The three checkpoint **reads** redirect to the store: the connect-time authority
  (a store read fault fail-closes to a retryable disconnect, never streaming past an unknown
  checkpoint), the go-forward guard (deferred from config to connect), and the watermark
  pre-skip (an in-memory watermark seeded once from the connect read).
- `snapshot: true` composes with lib mode: the snapshot handoff LSN is written to the store
  (the sink's `handle_snapshot_complete/1` is not called in lib mode), ordered before streaming;
  a handoff write fault halts `:snapshot_handoff_failed` (whole-snapshot redo on operator restart).
- `Replicant.Sink`: `checkpoint/0` is now an `@optional_callbacks` entry — a lib-mode sink
  implements only `handle_transaction/1` (persisting data; its returned LSN is ignored).
  `Config` enforces `checkpoint/0` presence at start for sink-owned mode only.
- New telemetry `[:replicant, :checkpoint_store, :written | :read | :failed]`; new `Replicant.Error`
  reasons `:checkpoint_store_failed` and `:checkpoint_store_schema_mismatch`; `slot_name` added to
  the value-free telemetry allowlist.

### Added — Initial snapshot / backfill (`replicant-snapshot`)

- `snapshot: true` start mode: `Replicant.start_link/1` bootstraps a `:state_mirror`
  (or any snapshot-capable) sink from an already-populated source and hands off to
  streaming at the snapshot LSN — **gap-free and dup-free** by the existing transaction
  watermark. Composes with `go_forward_only` and resume (both `true` → `:conflicting_start_mode`).
- `Replicant.Sink` gains two `@optional_callbacks`: `handle_snapshot/2` (batch upsert;
  `first_for_table?` triggers the per-table reset — a hard redo-safety obligation) and
  `handle_snapshot_complete/1` (the durable checkpoint handoff). `%Change{op: :snapshot}`
  carries backfill rows.
- `Replicant.Snapshotter` reads the publication's tables at the exported
  `consistent_point` via a `REPEATABLE READ` cursor on a separate connection, behind a
  value-free error boundary (Critical Rule 1). `EXPORT_SNAPSHOT` slot creation +
  `SET TRANSACTION SNAPSHOT` (snapshot-name-literal validated, not the identifier
  allowlist) + server-side `format('%I.%I')` table quoting.
- Fail-closed crash recovery: a mid-COPY crash halts `:snapshot_incomplete` (never
  auto-drops a slot); a checkpoint read fault in snapshot mode halts
  `:checkpoint_unreadable`. The operator drops the slot to retry.
- `Replicant.lsn_from_string/1`; `Replicant.Error` reason `:snapshot_failed`.

### Added — Plan 1: offline CDC core (decode / assemble / validate / redact)

- Vendored `pgoutput` byte parser behind a **value-free-error decode boundary**
  (`Replicant.Decoder.decode/1`) — catches every raise and scrubs raw bytes so no
  row value ever reaches an error, log, or telemetry event (Critical Rule 1).
- Type-aware value casting (`Replicant.Casting.Types` / `Replicant.Casting.ArrayParser`)
  and the OID→type database (`Replicant.Decoder.OidDatabase`), vendored from walex
  (MIT; credited in `NOTICE`).
- `Replicant.Assembler` — pgoutput message stream → `%Replicant.Transaction{}`:
  transaction-granularity commit LSN, unchanged-TOAST extraction (the sentinel is a
  first-class `unchanged` list, never in `record`), watermark skip
  (`commit_lsn <= checkpoint`), additive/destructive schema-change classification
  with fail-closed halt, and synchronous per-transaction sink apply.
- Data contract structs: `Replicant.Transaction`, `Replicant.Change` (+ `Change.Column`),
  `Replicant.SchemaChange`; the LSN facade (`t:Replicant.lsn/0` uint64, `lsn_to_string/1`).
- `Replicant.Sink` behaviour with `@optional_callbacks` (a minimal sink compiles clean).
- Identifier allowlist (`Replicant.Identifier`) + validated slot/publication SQL builder
  (`Replicant.QueryBuilder`), hardening walex's raw interpolation against injection
  (Critical Rule 2).
- Structure-only telemetry allowlist (`Replicant.Telemetry`) — LSNs/counts/table
  names/durations/error classes only, never row values (Critical Rule 1).
- Typed, value-free `Replicant.Error`.
- Real-`pgoutput` byte conformance suite (walex-captured fixtures) covering every
  message type including the unchanged-TOAST sentinel and all replica-identity modes —
  runs with no live database.

### Fixed — Plan 1 closeout review (2026-07-04)

- The assembler's value-free boundary now catches sink `throw`/`exit` (not only
  raises), scrubbing them value-free — a sink exit reason (e.g. a `GenServer.call`
  timeout) can embed the transaction's row values (Critical Rule 1).
- A row or truncate for a relation never seen in the stream halts fail-closed
  instead of emitting a table-less empty change checkpointed as success.
- A replica-identity change expressed via the `:key`-flagged column set (a
  `REPLICA IDENTITY USING INDEX` / primary-key swap with the enum unchanged) now
  classifies `:destructive` (spec §7/§9), not silently unhandled.
- `old_record` is key-only under non-FULL replica identity — the NULL placeholders
  a key tuple carries for non-key columns are dropped (spec §7).
- A multi-relation `Truncate` assigns each relation a unique, monotonic `ordinal`
  (previously all shared one, colliding with a following change's ordinal).
- Sink raise/throw/exit failures are labeled `:sink_failed` (distinguishable from a
  casting `:decode_failure`).

### Added — Plan 2: live streaming + exactly-once

- `Replicant.Connection` (`Postgrex.ReplicationConnection`) — owns the replication
  slot and advances it only after the sink durably commits: keepalive replies and
  the async ack report the **last durably-checkpointed LSN** as the flush position
  (never the received `wal_end` — fixes walex's fire-and-forget `wal_end+1`
  at-most-once ack). Decodes each WAL message behind the value-free boundary and
  forwards decoded messages to the assembler; never blocks on the sink.
- **Slot-invalidation fail-closed halt** (spec §8 R-ISO) — detects `wal_status = 'lost'`
  or `conflicting` on PG16 (not `invalidation_reason`, which is PG17+) and halts the
  pipeline permanently rather than silently recreating the slot.
- `Replicant.AssemblerServer` — a serial process that applies the sink synchronously
  off the keepalive path; halts fail-closed on a destructive schema change or a sink
  write fault. `Replicant.Pipeline` (`:one_for_all`) + `Replicant.Supervisor`
  (`DynamicSupervisor`) + the OTP `Application` callback + a named `Registry`.
- **Go-forward-only start guard** (`Replicant.Config`) — refuses a `:state_mirror`
  sink resuming from an empty checkpoint without `go_forward_only: true`.
- **Bounded in-flight window + fail-closed "sink cannot keep up" lag-halt** (spec §4) —
  the Connection tracks un-checkpointed WAL lag and halts fail-closed past a
  configurable `:max_inflight_lag` (default 64 MiB backlog ceiling) rather than
  growing the assembler mailbox unboundedly; keepalive-safe (never blocks the
  Connection).
- `byte_size` + `lag_ms` on `[:replicant, :transaction, :assembled]`; the
  `[:replicant, :connection, *]` and `[:replicant, :checkpoint, :advanced]` events.
- Gated crash-injection integration suite (real PG16, `wal_level=logical`): baseline
  exactly-once, crash-and-resume (loss = 0), re-delivery dedup (effect-dup = 0),
  mid-transaction + during-keepalive kills, the §4 backpressure spike, and an
  independent PG16 `pgoutput`-conformance capture.
- `postgrex ~> 0.22.2` dependency (co-resolves with `decimal ~> 3.1`; floor is
  0.22.2 for CVE-2026-32687).

### Fixed — Plan 2 closeout review (2026-07-05)

- **Data loss on a missing slot with a live checkpoint** — an absent
  `pg_replication_slots` row was unconditionally recreated; with a non-empty sink
  checkpoint the fresh slot streamed from its creation LSN, silently skipping the
  WAL between the checkpoint and now. Now halts fail-closed with a `:data_gap`
  signal when the checkpoint is non-empty; an empty checkpoint (first run /
  go-forward) still creates the slot (spec §8 / §14.19).
- **Over-advance on a sink-returned LSN** — the ack advanced to whatever LSN
  `handle_transaction/1` returned; a value higher than the transaction's own
  commit LSN would advance the slot past un-persisted WAL. The ack now uses the
  known `txn.commit_lsn` (spec §2 / §14.20).
- **Go-forward guard fail-open on an invalid `sink_kind`** — an unrecognized
  `sink_kind/0` return was treated as the laxer `:append_log`; a typo could let an
  empty `:state_mirror` sink start and partial-deliver. Unknown kinds now coerce to
  the strict `:state_mirror` default.
- **Caller `:connection` opts could override library control opts** — a caller
  `sync_connect`/`name`/`auto_reconnect` in `:connection` won over the library's,
  breaking the non-blocking facade or Registry wiring. The library's control opts
  now take precedence.
- **Sink write-fault recovery contract clarified** — a sink write fault is a
  **permanent** fail-closed halt (operator restart required), not auto-retry
  (spec §6 / §14.18).

[Unreleased]: https://github.com/baselabs/replicant/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/baselabs/replicant/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/baselabs/replicant/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/baselabs/replicant/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/baselabs/replicant/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/baselabs/replicant/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/baselabs/replicant/releases/tag/v0.1.0

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Post-release package CI now accepts only a coherent published identity.** The throwaway
  package workflow previously required the current version's tag, GitHub release, and Hex release
  to be absent, so the first documentation commit after a successful publication made `main` CI
  fail. Check mode now admits either a wholly free namespace or a fully published local/remote
  tag plus GitHub and Hex release, with the published tag required to be an ancestor of the
  checked source. The GitHub target and release state and Hex version, tracked package checksum,
  and documentation availability must all match. Candidate minting still requires complete
  absence; partial publication and an unrelated tag remain fail-closed.

## [1.2.0] - 2026-08-19

### Added

- **Proven support for PostgreSQL 15, 16, 17, and 18, with version-gated capabilities.** The CI
  matrix now runs the full suite (Docker-only, `wal_level=logical`) against all four majors on the
  operator-approved port mappings (`5615`/`5599`/`5617`/`5618`; `localhost:5432` is never used),
  each matrix row asserting its live `server_version_num` matches the expected major and grepping
  for an `R05-SUBSTRATE-RECEIPT pg=<major>` line emitted by a live integration test — so a skipped
  or mis-wired row reds rather than passing vacuously. A new
  `test/integration/version_behavior_test.exs` runs against whatever major the substrate is and
  branches on the live version: failover slots are proved created on PG17/18 and structurally
  rejected on PG15/16 (`{:config, :failover_unsupported}` — those majors reject the `FAILOVER`
  slot option).

- **Typed logical-slot consistent-point callback for go-forward append consumers.** The optional
  `Replicant.Sink` callback `handle_slot_origin/2` receives the LSN a go-forward
  stream begins at, on every connect and reconnect, before `START_REPLICATION`, for BOTH a
  freshly-created and a reused slot (distinguished by `context.reused?`). For a new slot the origin
  is the `CREATE_REPLICATION_SLOT` `consistent_point` (previously parsed internally and discarded on
  the plain go-forward path); for a reused slot it is the greater of the durable checkpoint and the
  slot's live `pg_replication_slots.confirmed_flush_lsn` — PostgreSQL's effective
  `START_REPLICATION` origin. `context` is value-free (`%{slot_name, reused?}`), carrying no row
  bytes. Returning `:ok`
  accepts; any other return, raise, throw, or exit halts the pipeline fail-closed
  (`:slot_origin_rejected`) — an append consumer that detects a gap past its last appended LSN gets
  a veto instead of silently skipping WAL. Missing, NULL, or malformed logical-slot state halts
  before callback/streaming as `:slot_origin_unavailable`; origin `0` is never fabricated. A sink
  that does not implement the callback is completely unaffected: no extra query, byte-identical
  streaming. Covered by red-first unit tripwires (the
  fail-closed veto proven RED by a `start_streaming` mutation) plus a live-PostgreSQL suite that
  proves the new-slot origin falls in the source-WAL creation window and the reused origin advances
  and is bracketed by the live slot state across a forced reconnect.

### Fixed

- **The slot-invalidation query no longer errors on PostgreSQL 15.**
  `pg_replication_slots.conflicting` was added in PG16; the previous PG<17 query selected
  `wal_status, conflicting`, so on PG15 it errored `column "conflicting" does not exist` — crashing
  a PG15 pipeline at the invalidation check into a reconnect storm. The query is now gated in three
  tiers by `server_version_num` (PG15 → `wal_status`; PG16 → `+ conflicting`; PG17+ → `+
  invalidation_reason, synced`), and `classify_slot_status/1` handles the PG15 single-column row
  (`wal_status = 'lost'` is PG15's sole invalidation signal). Proven red-first at the unit level and
  verified against live PostgreSQL 15/16/17/18.

- **An unknown checkpoint with an absent replication slot now halts fail-closed instead of
  silently creating a fresh slot (data-integrity, fail-closed).** In sink-owned mode a
  checkpoint read fault (`sink.checkpoint/0` raising or erroring) reads as `checkpoint_lsn 0`
  with `checkpoint_state: :fault`. The §14.15 streaming fail-open (resume-from-0, the
  idempotent sink dedups the re-stream) is safe only when the slot is **present** — a resume
  clamps to the slot's server-side `confirmed_flush_lsn`, so nothing is skipped. With the slot
  **absent** there is nothing to resume: the connect path previously treated the fault-as-0 as
  a genuine empty first run and created a fresh `CREATE_REPLICATION_SLOT`, which begins
  streaming at its own creation LSN and silently skips every transaction between the (unknown)
  real checkpoint and now — an unrecoverable data gap. That path now halts fail-closed in the
  `:data_gap` family with a distinct, value-free telemetry reason
  (`[:replicant, :connection, :slot_invalidated]`, `reason: :checkpoint_unknown`) and never
  emits `CREATE_REPLICATION_SLOT`, including in incremental-snapshot mode when its separate
  progress token is empty. A genuinely **empty** checkpoint (`checkpoint_state: :empty` — a real
  first activation / go-forward) still creates the slot as before. Covered by red-first
  connect-decision unit tests across plain and incremental modes and a live PostgreSQL fault probe
  (raising-checkpoint sink, absent slot → structural halt, zero slots created on the server).

### Security

- **Telemetry metadata and measurements are now validated by a closed key set AND a per-key
  value-shape contract, not key-closure alone (value-free hardening, Critical Rule 1).**
  `Replicant.Telemetry` previously checked only that metadata keys were on the value-free
  allowlist; a row/column value smuggled into an allowlisted key with the wrong shape (a
  string where an LSN/count/duration belongs) would still ship downstream, and measurements
  (the `:telemetry` event's 2nd argument) were not validated at all. Every permitted key now
  carries a type contract — LSNs are a non-negative integer or nil, counts and durations are
  non-negative integers, `transactional` is a boolean, `table`/`slot_name` are strings or
  value-free nil absent markers,
  `reason`/`error_class`/`kind` are atoms; measurement `duration`/`byte_size`/`change_count`
  are non-negative integers and `lag` is a signed integer (WAL-byte arithmetic). An off-list
  key or a wrong-shape value raises rather than emitting. Shape errors render only an allowed
  atom key and the value's TYPE; off-list errors elide arbitrary rejected keys as well as values,
  so the guard cannot leak attacker-controlled bytes. No emission site changed: all current
  events pass.
  Covered by red-first mutation tripwires (a string in each numeric/boolean field, a non-atom
  reason, an off-list measurement key, and a value-free-error assertion) plus the full live
  PostgreSQL suite.

- **Logical-decoding message `prefix`/`content` bytes are now guarded against leaking into any
  failure surface by an adversarial regression suite (value-free hardening, Critical Rule 1).**
  A message's `prefix` and `content` are user-controlled bytes (a `pg_logical_emit_message`
  caller chooses them), so they can carry a secret or a row value. `test/replicant/message_value_safety_test.exs`
  drives a hostile prefix/content — including a NUL and an invalid-UTF-8 byte — through every
  replicant-owned failure surface (a malformed `'M'` frame decode, a sink `raise`/`throw`/`exit`/
  non-`:ok` return, the transactional-message-before-`Begin` halt, and the `[:message, :received]`
  / `[:sink, :failed]` telemetry events) and asserts the bytes never appear in the resulting
  `%Replicant.Error{}`, its `Exception.message/1`, or the telemetry event — in either the printable
  or the raw byte-list rendering. Replicant remains prefix-blind: an unknown/hostile prefix yields
  only the structural reason atom. No production behavior changed (the boundary already held); each
  assertion is proven non-vacuous by a documented mutation of the exact scrub it guards.

- **Release publication preserves the exact reviewed package bytes and minimizes credential
  exposure.** The guarded R07 path rejects artifact/receipt/witness overrides in publish mode,
  validates the exact version-and-digest authorization before reading `HEX_API_KEY`, uploads the
  immutable candidate without rebuilding, verifies Hex's checksum through the unauthenticated
  public release endpoint, and requires the refetched tarball to match the witnessed SHA-256
  before its fresh-consumer smoke. Shipped source guidance names the exact-tag push and guarded
  uploader rather than the rebuilding `mix hex.publish` task.

### Changed

- **Retry-guidance for retryable non-transactional messages is reconciled to idempotency, not a
  one-time nonce.** `docs/INVARIANTS.md` §3 previously suggested `strategy :one_time_nonce` keyed
  on a message's `{lsn, ordinal}` to make its effect once. Because non-transactional delivery is
  at-least-once, a lawful reconnect re-delivers the same message, and a one-time-nonce admission
  would *reject* that replay and fail the retry — dropping the effect. The guidance now recommends
  `strategy :idempotency` keyed on the message's **LSN** (the `handle_message/2` context is
  `%{lsn: lsn}`, unique per non-transactional WAL record; `ordinal` is a transactional-message
  field and is absent here) so the replay is a durable no-op, agreeing with `README.md` and
  ADR-0001; the `handle_message/2` docstring states the same.

## [1.1.0] - 2026-08-13

### Fixed

- **Float-array casting no longer halts the pipeline on valid Postgres output (Critical-Rule-1
  path, fail-closed).** The `_float4`/`_float8` array clauses called `String.to_float/1`, which
  raises on the exact text Postgres `float4out`/`float8out` emits for whole numbers (`"1"`),
  scientific notation (`"1e+20"`), and the special values `NaN`/`Infinity`/`-Infinity`. So a
  `double precision[]` / `real[]` column holding an ordinary whole-valued element raised inside
  `Casting.Types.cast_record/2`; the decode boundary caught it and halted the pipeline
  fail-closed (no data loss, no value leak), but a schema with a whole-number float-array element
  permanently stalled the consumer. The array clauses now mirror the scalar `float*` clause
  (`Float.parse` fallback + the `:nan`/`:infinity`/`:neg_infinity` atoms) and never raise. For
  symmetry the `_int*` array clause is likewise lenient (`Integer.parse` fallback) instead of
  `String.to_integer/1`. The `Casting.Types` moduledoc's raise-site list is now accurate (the
  array bangs it omitted are gone). Covered by a new red-first float-array unit test (whole
  numbers, scientific notation, special values, NULL, multidimensional nesting).

### Changed

- **Post-halt incremental-window calls are rejected with `{:error, :window_reset}`.** After a
  fail-closed halt, the `{:message, ...}` cast was already dropped, but the three incremental
  snapshot-window `handle_call` clauses (`open_snapshot_window`, `deliver_snapshot_chunk`,
  `finish_snapshot_table`) still ran — so `apply_ready_chunks` could call `sink.handle_snapshot`
  once during the async teardown window. Idempotent sinks bound the impact, but this contradicted
  the halt contract the module states. All three now carry a `halted: true` guard returning
  `{:error, :window_reset}` (the reload/stop signal the reader already handles). Covered by a
  red-first halted-guard unit test.
- **Snapshotter / incremental-reader connection opts use `Keyword.merge` (library wins).** Both
  reader call sites used `conn_opts ++ [pool_size: 1]`, so a caller-supplied `pool_size` won over
  the library's and duplicate keys reached Postgrex. Now `Keyword.merge(conn_opts, pool_size: 1)`
  — parity with the checkpoint-store and connection merges, which document the library-wins rule
  as load-bearing.

## [1.0.0] - 2026-08-13

### Fixed

- **Incremental-reader "exactly-one" made structural (was comment-defended).** The
  incremental-backfill reader is `spawn_link`'d to the Connection, and the "exactly one reader
  per slot" invariant was previously defended only by comments (`reader_pid` carried across
  reconnect + `retire_reader/1` on every reconnect path). A future reconnect path that forgot
  `retire_reader` would spawn a second reader → double delivery of snapshot chunks. The reader
  now registers under `{:incremental_reader, slot}` in `Replicant.Registry` (`:unique`) at start;
  a live prior registration halts fail-closed (`:duplicate_reader`) instead of double-delivering.
  Registry auto-frees the key on the owner's death, so the normal retire+restart flow is unchanged.
- **v1 snapshot value-type convergence.** `snapshot: true` shipped Postgrex's native row
  decode (`SELECT *`), so a typed column delivered a different runtime type from the snapshot
  than from the stream — e.g. a `timestamp` column arrived as `%NaiveDateTime{}` from the v1
  snapshot and `%DateTime{}` from the stream (which casts through `Casting.Types.cast_record/2`).
  The incremental snapshot was fixed; v1 was not. v1 now projects `<col>::text` and casts each
  value through the SAME path the stream uses, so the v1 snapshot and the stream deliver
  byte-identical `%Change{}.record` values for every type. Also extends `cast_record` to
  recognize bool's full-word `::text` form (`"true"`/`"false"`) — PG `bool::text` emits the word
  form while pgoutput emits `"t"`/`"f"`, so both `::text` snapshot paths (v1 AND incremental)
  previously delivered the string `"true"` for a bool column where the stream delivers boolean
  `true`; both now converge to boolean. (Critical Rule 1 boundary preserved; the stream never
  sends the word form, so the new clauses fire only on snapshot paths.)
- **postgrex CVE bump (0.22.2 → 0.22.4).** `mix hex.audit` reported two advisories on
  postgrex 0.22.2: CVE-2026-58225 (LOW, dollar-quote in `Postgrex.Notifications` reconnect
  replay, fixed 0.22.3) and CVE-2026-66838 (MEDIUM, SQLi via the `:comment` option in
  `Postgrex.stream/4`, fixed 0.22.4). Replicant's call sites use neither vector (the snapshotter's
  `Postgrex.stream` passes no `:comment`; there is no `Postgrex.Notifications` usage), but the floor
  moves to `~> 0.22.4` so the audit is clean and transitive consumers are not exposed. No
  `ReplicationConnection` API change across 0.22.2 → 0.22.4 (security patches only).
- **Integration suite was silently masked.** Every integration module started a NAMED Postgres
  pool in `setup` and never stopped it; ExUnit's `async: false` one-process model then made test 2+
  fail with `{:error, {:already_started, _}}`, cascading to setup failure. The full integration
  suite was 66 tests / 31 failures — roughly half the live-PG16 crash-injection marquees (the
  project's primary correctness evidence) were not running. `PG16.named_conn/2` now centralizes
  per-test isolation (start the pool unlinked + register an `on_exit` that stops it); all 23
  named-pool sites route through it. The full suite is now 66/0.
- **Bound the lib-mode incremental-snapshot concurrency marquee's writer** and
  assert real chunk/stream overlap before quiescing it, so constrained CI
  runners prove convergence instead of being starved by an unbounded WAL source.

### Added

- **Actual replication-session identity.** Every connect and reconnect now runs
  `IDENTIFY_SYSTEM` on the exact `Postgrex.ReplicationConnection` before reading
  any sink-owned or library-owned checkpoint. The public
  `%Replicant.SessionIdentity{}` and optional `handle_session_identity/2`
  callback let a source-aware sink reject drift synchronously; malformed identity
  or any callback failure halts with a fixed value-free reason.

- **Foundational ADRs + published Critical Rules.** Four ADRs record the load-bearing 1.0
  posture decisions a bare-clone maintainer cannot recover from code alone: [0003](docs/adr/0003-value-free-error-boundary.md)
  the value-free error/log/telemetry boundary, [0004](docs/adr/0004-commit-lsn-transaction-watermark.md)
  the commit-LSN transaction-granularity watermark, [0005](docs/adr/0005-spill-is-ephemeral-scratch.md)
  spill as ephemeral non-fsync'd scratch, [0006](docs/adr/0006-fail-closed-supervision.md) the
  `:one_for_all` + `:temporary` fail-closed supervision. The 5 Critical Rules are published as
  [`docs/INVARIANTS.md`](docs/INVARIANTS.md) (sink-author-facing; `AGENTS.md` was removed from the
  tarball in 0.2.1 as an agent contract, so this is the published home for the binding invariants).
  Both ship in the Hex tarball (`docs` added to package `files`) and render on HexDocs.

### Changed

- **`Replicant.Assembler` split into three modules.** The assembler was a 1633-LOC god module;
  it is now `Replicant.Assembler` (Core: v1 router + sink-dispatch/scrub cluster + change-building +
  watermark, 1072 LOC), its streaming helper (proto-v2 reassembly + spill,
  398 LOC), and its batch helper (batched-checkpoint buffering + flush, 230 LOC). The
  `%Assembler{}` struct is UNCHANGED; every moved function is a pure function on that struct, and
  the 545-unit + 613-integration suites are the preservation net (green at every commit, no
  behavior change). The cross-cutting Rule-1 scrub cluster stays intact (every sink-call site keeps
  its value-free `rescue`/`catch`); the moved batch-flush scrub travels as one tamper-tested unit.
  The Core↔Streaming/Core↔Batch call cycles are runtime-resolved in Elixir; the 8 widened `defp`→`def`
  internal seams carry `@doc false`. Closeout: fresh-context diff-review CLEAN; cross-vendor
  codex+claude CLEAN; crash-injection marquees loss=0 / effect-dup=0 green against live PG16.
- **Batch spill-IO fault now labeled `:spill_io_failed` (was `:sink_failed`).** A spill-IO fault
  (a lazy Reader raising `Spill.Error` while the sink forced its enumeration during sink-owned batch
  delivery) is now distinguished from a sink fault — parity with the per-transaction `deliver_now`
  path, which already labeled it `:spill_io_failed`. Still fail-closed and value-free (Critical Rule
  1); only the surfaced error reason / telemetry `:reason` becomes more specific for triage.
- **Batch flush-trigger deduplicated.** The lib-batch and sink-owned-batch flush triggers were
  two identical `cond` blocks (count cap OR LSN-span cap OR buffer); extracted to a shared
  `Assembler.maybe_trip_batch/3` so the two modes cannot drift (a drift would silently change the
  dup bound in one mode). Pure refactor, behavior unchanged.
- **Conformance suite tamper-evidence is now machine-checked.** A parametric byte-flip test
  (type byte + sampled payload, per message class) proves each real-captured fixture goes red on
  mutation — previously tamper-red by construction (strict pattern-matches), not by test.
- **Install constraint corrected.** The README and getting-started Livebook shipped
  `{:replicant, "~> 0.2"}` (= `< 0.3.0`), locking users out of every 0.3 feature; the
  prepared 1.0 source release now shows `~> 1.0`. The coordinated AshReplicant 1.0
  release will require Replicant 1.x; that consumer dependency change lands separately.
- **Release hygiene.** A `.tool-versions` pins Elixir 1.20.3-otp-29 / Erlang 29.0.3; CI's `setup-beam`
  is aligned to it (it was otp-27 / elixir-1.17, and the formatter's list-wrap heuristic is
  version-sensitive — `mix format --check-formatted` was red on the dev toolchain). `mix audit`
  (the declared-but-unenforced `deps.unlock --check-unused` + `hex.audit` + `deps.audit` alias) is
  now a CI gate before build, and the cache key binds the pinned toolchain + `mix.lock` + `.tool-versions`.
- **Hex package boundary.** Package files now enumerate the published docs and
  ADR directories instead of including all of `docs/`, so ignored local Forge
  specs, plans, reviews, and handoffs cannot leak into release bytes.
- **`Replicant.Config.t` no longer advertises a `:batch` key.** It is derived from
  `checkpoint_store[:batch]`; a top-level `batch:` option is rejected with `:config_invalid`, so the
  public type advertising it was a trapdoor.
- **`%Transaction.changes` typed as the union it is.** Was `Enumerable.t()` (broad enough to hide
  that a spilled streamed txn delivers a single-pass `Replicant.Spill.Reader`, not a re-iterable
  List); now `[Change.t()] | Spill.Reader.t()` with a strengthened moduledoc naming the forbidden
  calls (`length/1`, `Enum.to_list/1`, re-iteration) that force a spilled txn back into RAM.
- **Hex description states the delivery guarantee honestly.** Was an unqualified "exactly-once
  delivery"; now "zero-loss delivery — exactly-once for transactional sinks, at-least-once
  (duplicate-bounded) for non-transactional sinks" (Critical Rule 3).
- **The three vendored public functions are specced.** `Casting.Types.cast_record/2`,
  `Casting.ArrayParser.parse/1`, `Decoder.OidDatabase.name_for_type_id/1` now carry `@spec`; the
  frozen public surface is fully specced.

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

[Unreleased]: https://github.com/baselabs/replicant/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/baselabs/replicant/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/baselabs/replicant/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/baselabs/replicant/compare/v0.3.1...v1.0.0
[0.3.1]: https://github.com/baselabs/replicant/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/baselabs/replicant/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/baselabs/replicant/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/baselabs/replicant/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/baselabs/replicant/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/baselabs/replicant/releases/tag/v0.1.0

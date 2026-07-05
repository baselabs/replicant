# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
  `Replicant.SchemaChange`; the LSN facade (`Replicant.lsn/0` uint64, `lsn_to_string/1`).
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
  (`DynamicSupervisor`) + `Replicant.Application` + `Replicant.Registry`.
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
- `postgrex ~> 0.22` dependency (co-resolves with `decimal ~> 3.1`).

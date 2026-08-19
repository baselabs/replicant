# Replicant

A framework-agnostic Elixir CDC consumer for Postgres logical replication
(`pgoutput`), delivering committed row changes to a pluggable **sink** with
**zero data loss**: the replication slot advances only after the sink has
durably persisted the transaction.

Replicant is **tenant-blind and classification-blind** — the reliable CDC
consumer sibling to [`arcadic`](https://github.com/baselabs/arcadic).
Multitenancy, classification, and Ash resources live one layer up, in the
[`ash_replicant`](https://hex.pm/packages/ash_replicant) sink adapter.

> **Status:** 1.1.0 is published on Hex (tagged `v1.1.0`). Replicant owns
> the replication slot via `Postgrex.ReplicationConnection`, acks only after the
> sink durably commits (ack-after-checkpoint), halts fail-closed on slot
> invalidation, and is proven by a real-PG16 crash-injection suite
> (loss = 0, effect-dup = 0). The 1.0 contract includes initial snapshot/backfill (incl. a
> resumable incremental mode), a lib-owned checkpoint
> store for non-transactional sinks, batched checkpointing, sink-owned atomic
> batch delivery, in-progress-transaction streaming, consumer-side disk
> spill for oversized transactions, multi-publication per pipeline, and
> logical-decoding messages have all shipped. See "How it streams" below.

## Highlights

- **Sink-owned, transaction-granularity exactly-once** — the unit of delivery
  and of the watermark is the *transaction*, keyed by its single `commit_lsn`
  (every row in a pgoutput proto-v1 transaction shares one commit LSN). A sink
  skips any transaction whose `commit_lsn <= checkpoint` and upserts rows by
  table PK; that is at-least-once plus an idempotent sink, which is the only
  honest way to reach exactly-once without two-phase commit.
- **Value-free errors, logs, and telemetry** — every row value is assumed to
  be PII or a secret. Decode failures are caught and scrubbed into a
  `Replicant.Error` that never carries raw WAL bytes; telemetry metadata is
  allowlisted to LSNs, table names, counts, durations, and error classes.
- **Identifier-validated SQL** — slot and publication names pass through
  `Replicant.Identifier.validate/1` (a strict Postgres-identifier allowlist)
  before they reach SQL, closing the raw-interpolation surface in the
  upstream parser this library vendors from.
- **TOAST-sentinel aware** — an UPDATE that doesn't touch a TOASTed column
  sends a sentinel, not the value. Replicant surfaces it as a first-class
  `unchanged: [col]` list on `Replicant.Change`, so a sink knows exactly which
  columns to leave untouched on upsert, instead of overwriting them with a
  placeholder.
- **Fail-closed on destructive schema drift** — a replica-identity change or a
  dropped column is classified `:destructive` and halts, rather than silently
  emitting incomplete or misattributed rows.
- **Actual replication-session identity** — `IDENTIFY_SYSTEM` runs on the exact
  replication connection before checkpoint lookup. A source-aware sink can
  accept or reject `%Replicant.SessionIdentity{}` synchronously on every connect
  and reconnect; no separate-connection preflight is treated as authoritative.
- **Column names stay strings** — never `String.to_atom`, so a wide or
  attacker-influenced schema cannot exhaust the atom table.

## LSN representation

A Postgres LSN is exposed as a single `non_neg_integer` — the 64-bit value
`(xlog_file <<< 32) ||| xlog_offset` — so that ordinary integer comparison is
correct WAL ordering, and the same value feeds the wire-level standby status
update:

```elixir
Replicant.lsn_to_string(0x16E3778)
#=> "0/16E3778"
```

Use `Replicant.lsn_to_string/1` for display; LSNs are WAL positions, not row
data, so they are permitted in telemetry metadata. The exactly-once watermark
check is plain integer comparison: `txn.commit_lsn <= checkpoint`.

## How it streams

A running pipeline is two processes under a `:one_for_all` supervisor (three in
lib/checkpoint-store mode, which adds `Replicant.CheckpointStore` as the first child):

- **`Replicant.Connection`** (`Postgrex.ReplicationConnection`) owns the
  replication slot and the socket. It answers a keepalive with the **last
  durably-checkpointed LSN** while a published transaction is in flight (never
  advancing the slot past un-persisted data); when the pipeline is idle it
  advances the slot to the server WAL position so a quiet-but-filtered
  publication does not pin WAL. It decodes each WAL message behind the
  value-free boundary and forwards it to the assembler — it never runs the sink,
  so it is always free to answer keepalives. It advances the ack asynchronously
  when the sink signals a durable commit, and halts fail-closed on slot
  invalidation (`wal_status = 'lost'` / `conflicting`), a decode failure, or a
  sustained sink-lag backlog (the bounded in-flight window).
- **`Replicant.AssemblerServer`** applies the sink synchronously, off the
  keepalive path, and halts fail-closed on a destructive schema change or a
  sink write fault.

Because the ack reports the durable checkpoint while a transaction is in flight
(advancing over filtered WAL only when idle, which carries no publication data),
a crash between dispatch and persist re-delivers from the durable `confirmed_flush`
and the idempotent sink dedups — the exactly-once seam that `walex`'s
fire-and-forget `wal_end + 1` ack does not have.

## PostgreSQL version support

Replicant is **tested on PostgreSQL 15, 16, 17, and 18** — the CI matrix runs the full suite
against all four majors (Docker-only, `wal_level=logical`). Capabilities are gated by the
server's `server_version_num`, so a single build runs correctly across the range:

| Capability | PG15 | PG16 | PG17 | PG18 |
|---|:---:|:---:|:---:|:---:|
| Logical streaming, snapshot, checkpoint, exactly-once | ✅ | ✅ | ✅ | ✅ |
| Slot-invalidation columns queried | `wal_status` | `+ conflicting` | `+ invalidation_reason, synced` | same as 17 |
| Failover slots (`failover: true`) | ❌ rejected | ❌ rejected | ✅ | ✅ |

The slot-invalidation query selects only the columns that exist on the connected major
(`conflicting` was added in PG16, `invalidation_reason`/`synced` in PG17), so it never errors
on an older server. On PG17+ Replicant reads the authoritative `invalidation_reason` column (a
superset of the PG15/16 signals) and supports **failover slots** for HA.

### Failover slots (PG17+)

Pass `failover: true` to `Replicant.start_link/1` to create the replication slot with the
`FAILOVER` option, so PostgreSQL syncs it to physical standbys:

    Replicant.start_link(
      connection: [hostname: "primary.internal", ...],
      slot_name: "replicant_orders",
      publication: "orders_pub",
      sink: MyApp.OrdersSink,
      failover: true            # PG17+ only; on PG15/16 the pipeline halts {:config, :failover_unsupported}
    )

After a failover, repoint the connection at the promoted primary — the slot already exists
there with its `confirmed_flush` position, so replication resumes with zero loss. Slot sync
(`sync_replication_slots`), promotion, and DNS/endpoint changes are operator concerns.
**Do not point Replicant at an unpromoted standby's synced slot** — it cannot be consumed
until promotion, and Replicant halts fail-closed (`{:slot_synced_unpromoted}`) rather than
looping.

## The 5 critical rules (see [`docs/INVARIANTS.md`](docs/INVARIANTS.md) for the full text)

1. **No row value in an error, log, or telemetry event.**
2. **Validate identifiers** before they reach SQL.
3. **Exactly-once is at-least-once + a transaction-watermark-idempotent
   sink** — never claim a naked exactly-once.
4. **Unchanged TOAST is a sentinel, not a value** — never overwrite it.
5. **Stay tenant-blind** — multitenancy and classification live in
   `ash_replicant`, never here.

## Installation

```elixir
def deps do
  [
    {:replicant, "~> 1.0"}
  ]
end
```

Contributors working from a source checkout can point Mix at the local checkout
via a path override, as shown in the Livebook.

## Interactive tour (Livebook)

[![Run in Livebook](https://livebook.dev/badge/v1/blue.svg)](https://livebook.dev/run?url=https%3A%2F%2Fraw.githubusercontent.com%2Fbaselabs%2Freplicant%2Fmain%2Fnotebooks%2Fgetting_started.livemd)

[`notebooks/getting_started.livemd`](notebooks/getting_started.livemd) is a
runnable, self-verifying tour: it starts a live pipeline against a local Postgres,
streams `INSERT`/`UPDATE`/`DELETE` through a tiny sink you can watch, then
demonstrates the unchanged-TOAST sentinel, transaction-granularity exactly-once,
snapshot/backfill, and logical-decoding messages. Click the badge to open it in
[Livebook](https://livebook.dev), or read it rendered on
[HexDocs](https://hexdocs.pm/replicant/getting_started.html). The notebook's code
is executed against live PostgreSQL 15/16/17/18 on every CI run
(`test/integration/livebook_getting_started_test.exs`), so it never drifts from the
library.

## Usage

Start a pipeline against a standby with `Replicant.start_link/1`, pointing it at
a sink that implements `checkpoint/0` + `handle_transaction/1`:

```elixir
Replicant.start_link(
  connection: [hostname: "standby.internal", port: 5432, username: "u",
               password: "p", database: "orders", ssl: true],
  slot_name: "replicant_orders",
  publication: "orders_pub",
  sink: MyApp.OrdersSink,
  go_forward_only: false
)

defmodule MyApp.OrdersSink do
  @behaviour Replicant.Sink

  @impl true
  def handle_session_identity(identity, %{slot_name: slot, publication: publications}) do
    # Before checkpoint/0 runs: bind or compare {identity.system_identifier,
    # identity.database, slot, publications}. Return :ok only when safe to resume.
    :ok
  end

  @impl true
  def checkpoint, do: {:ok, MyApp.Repo.last_committed_lsn()}

  @impl true
  def handle_transaction(%Replicant.Transaction{commit_lsn: lsn} = txn) do
    # In ONE DB transaction: skip if lsn <= checkpoint, else upsert txn.changes
    # by table PK and persist lsn as the new checkpoint. Then:
    {:ok, lsn}
  end
end
```

**Start modes.** A `:state_mirror` sink starting from an empty checkpoint must declare
its intent — `go_forward_only: true` (stream only new changes), or `snapshot: true`
(**backfill** the current state, then hand off to streaming at the snapshot LSN with zero
gap and zero duplication). A non-empty checkpoint simply resumes. `snapshot: true`
requires the sink to also implement `handle_snapshot/2` (batch upsert; clear the table on
`first_for_table?`) and `handle_snapshot_complete/1` (durably persist the handoff
checkpoint); a mid-snapshot crash halts fail-closed (`:snapshot_incomplete`) for an
operator to drop the slot and retry.

**Resumable incremental snapshot.** For large tables where an all-or-nothing `snapshot: true`
(a crash at 99% of a multi-hour COPY redoes everything) is the adoption blocker, opt into the
**incremental** mode instead:

```elixir
snapshot: [mode: :incremental, chunk_rows: 1000, max_pending_chunks: 4]
```

It creates a **durable** slot and streams immediately, while a linked reader backfills the
publication's tables in PK-ordered keyset **chunks** on its own connection — each chunk
consistency-bracketed by read-only LSN watermarks and collision-corrected against the live
stream (a concurrent write to a backfilling row wins; the stale chunk row is dropped). Progress
is durable per chunk, so a crash, halt, or reconnect **resumes from the last applied chunk**
rather than restarting. Chunks arrive through the same `handle_snapshot/2` callback; sink-owned
mode gives effect-once chunks, lib mode dup ≤ 1 chunk (never loss). PK-less tables fall back to a
bounded whole-table redo. `snapshot: true` remains the point-in-time option and is untouched.

**Lib-owned checkpoint (non-transactional sinks).** Pass a `:checkpoint_store`
(`[connection: <postgrex opts>, table: "replicant_checkpoints"]`) to flip the pipeline
into **lib mode**: the library writes the checkpoint to a durable Postgres table **after**
the sink persists (checkpoint-after-persist), so a non-transactional sink (files, S3,
Kafka, external APIs) needs to implement only `handle_transaction/1`. The guarantee is
**at-least-once, duplicate bounded to one transaction, never loss** — not effect-once (a
non-transactional sink cannot dedup). A store outage (connect-read or mid-stream write) is
bounded: the pipeline retries `max_retries` times (default 5) `retry_backoff_ms` apart
(default 1000 ms — ~5s of outage tolerated) then halts fail-closed; a permanent fault
(schema mismatch / invalid config) halts immediately.

**Persistent replication-command errors (`max_command_retries`, default 5).** A replication
command that fails *before the stream starts* — e.g. `CREATE_REPLICATION_SLOT` when the
server's `max_replication_slots` are exhausted, a second consumer already holding the slot,
or a forward-incompatible result shape — otherwise reconnects forever (`auto_reconnect`).
Replicant bounds this: after `max_command_retries` failed connect cycles without the stream
establishing, the pipeline **halts fail-closed and stays idle** instead of livelocking, and
emits `[:replicant, :connection, :command_error_halt]` (metadata: `attempt`, `max_retries`,
`slot_name` — value-free, no error content, Critical Rule 1). Set `max_command_retries: 0`
to halt on the first such fault. The bound is a *cycle count*, not a wall-clock time (the
failing reconnect is a hot loop). Only errors *before the first replication frame* are
bounded — once the stream is flowing, a later transient outage resets the counter and
still self-heals, and a server that is simply down (connection refused) keeps retrying
untouched, exactly as before.

**Sink-owned atomic batch delivery (transactional sinks).** For a **transactional sink** that
can persist multiple rows + checkpoint in one database transaction, pass a top-level
`batch_delivery: [max_transactions: 100, max_delay_ms: 1000]` to accumulate committed
transactions and deliver them as a batch:

```elixir
Replicant.start_link(
  connection: [hostname: "standby.internal", port: 5432, username: "u",
               password: "p", database: "orders", ssl: true],
  slot_name: "replicant_orders",
  publication: "orders_pub",
  sink: MyApp.OrdersSink,
  batch_delivery: [max_transactions: 100, max_delay_ms: 1000],
  go_forward_only: false
)

defmodule MyApp.OrdersSink do
  @behaviour Replicant.Sink

  @impl true
  def checkpoint, do: {:ok, MyApp.Repo.last_committed_lsn()}

  @impl true
  def handle_batch(transactions) do
    # In ONE DB transaction: skip any commit_lsn <= checkpoint, else upsert all
    # rows from all transactions by table PK, and persist the batch's highest
    # commit_lsn as the new checkpoint. Then:
    {:ok, List.last(transactions).commit_lsn}
  end
end
```

The batch flushes when it reaches `max_transactions` transactions, after `max_delay_ms`
milliseconds idle, or when the batch's WAL span (LSN-span lag) hits an auto-derived
safety cap (derived from `:max_inflight_lag`). Because the rows + checkpoint write is
atomic, effect-once is **preserved** (dup=0, loss=0) — stronger than lib-mode's
`checkpoint_store: [batch: …]` which is per-transaction delivery with batched checkpointing.
The sink must implement both `checkpoint/0` (resume) and `handle_batch/1`; it cannot
use `checkpoint_store`, and any `handle_transaction/1` implementation is ignored.
Emits `[:replicant, :sink, :batch_committed]` telemetry once per flush.

**Consumer-side disk spill (oversized transactions).** By default a single in-progress streamed
transaction is bounded by the in-flight window: one larger than `max_inflight_lag` halts
fail-closed. Opt into **disk spill** to reassemble such a transaction partly on disk and still deliver
it effect-once:

```elixir
Replicant.start_link(
  connection: [...], slot_name: "replicant_orders", publication: "orders_pub",
  sink: MyApp.OrdersSink, go_forward_only: false,
  max_inflight_lag: 64 * 1024 * 1024,
  streaming: [
    max_concurrent_txns: 64,
    spill: [dir: "/var/lib/replicant/spill", max_spill_bytes: 1024 * 1024 * 1024]
  ]
)
```

A transaction whose resident bytes cross `max_inflight_lag` spills its oldest changes to a per-txn file
under `dir`; at commit it is delivered as a **lazy, single-pass, disk-backed** `%Transaction{}` whose
`changes` streams the spilled frames + the resident tail. There are **two ceilings**: the resident RAM
bound `max_inflight_lag` (the spill trigger) and the disk bound `max_spill_bytes` (a transaction
exceeding it halts `:spill_exhausted`). Defaults: `max_spill_bytes` is `16 × max_inflight_lag`; `dir`
is a `0700` subdir of the OS temp dir.

**Delivery obligation.** A spilled transaction's `changes` is a **single-pass** `Enumerable` valid only
*during* the `handle_transaction/1` (or `handle_batch/1`) call — iterate it with `Enum`/`Stream` and do
**not** call `length/1`, `Enum.to_list/1`, or re-iterate it (any of which forces the whole transaction
back into RAM, defeating spill), and do not retain it past the call. The usual List-backed `changes`
still works exactly as before; only an oversized spilled transaction delivers the lazy form.

**Operator guidance.** Spill files are ephemeral non-fsync'd scratch (`0600`, value-free on fault),
deleted on commit/abort/reset/halt and swept per-slot on (re)connect. Replicant does **not** encrypt
them — if the source rows are sensitive, point `dir` at an encrypted/secure volume; a custom persistent
`dir` is yours to clean on decommission (the default OS temp dir is cleared by the OS).

**Multi-publication per pipeline.** `publication:` accepts a single name (the default) **or a list**.
Pass a list to stream the union of several publications through one slot — every name is validated
and deduplicated, and overlapping tables are de-duped by pgoutput on the wire:

```elixir
Replicant.start_link(
  connection: [...],
  slot_name: "replicant_orders",
  publication: ["orders_pub", "audit_pub"],   # was: publication: "orders_pub"
  sink: MyApp.OrdersSink,
  go_forward_only: false
)
```

Every requested publication must exist; a missing one **halts fail-closed** at connect time rather
than silently streaming the subset (a `START_REPLICATION` that names a missing publication would
otherwise stream only the found subset).

**Logical-decoding messages (`pg_logical_emit_message`).** Opt into Postgres logical-decoding
**messages** with `messages: true` to receive outbox-pattern / heartbeat rows emitted via
`pg_logical_emit_message`. The durability guarantee depends on the message's transactional flag
(stated honestly in the `handle_message/2` docs):

- **Transactional** (`transactional => true` to `pg_logical_emit_message`) — rides
  `%Transaction.messages` and inherits the transaction path's `commit_lsn` effect-once dedup
  (effect-once, just like the row changes in the same txn).
- **Non-transactional** (`transactional => false`) — arrives standalone via the
  `handle_message/2` sink callback and is **at-least-once** (no dedup key; duplicates are possible
  on reconnect). This is the honest guarantee — do not claim effect-once for it.

```elixir
Replicant.start_link(
  connection: [...],
  slot_name: "replicant_orders",
  publication: "orders_pub",
  sink: MyApp.OrdersSink,
  messages: true,                            # opt-in; sink MUST implement handle_message/2
  go_forward_only: false
)

defmodule MyApp.OrdersSink do
  @behaviour Replicant.Sink

  @impl true
  def checkpoint, do: {:ok, MyApp.Repo.last_committed_lsn()}

  @impl true
  def handle_transaction(%Replicant.Transaction{commit_lsn: lsn, messages: msgs} = txn) do
    # msgs :: [Replicant.Decoder.Messages.Message.t()] — transactional messages ride the
    # txn and inherit its commit_lsn effect-once dedup. Persist them with the txn's rows.
    {:ok, lsn}
  end

  @impl true
  # context :: %{lsn: lsn}; non-transactional messages ONLY (transactional ride handle_transaction/1).
  # At-least-once: a reconnect can re-deliver — your effect must be idempotent. The message's
  # `content`/`prefix` are USER BYTES (Critical Rule 1: never log or surface them in telemetry).
  def handle_message(%Replicant.Decoder.Messages.Message{} = msg, %{lsn: lsn}) do
    :ok
  end
end
```

`messages: true` requires the sink to implement `handle_message/2`; a sink missing it is rejected at
start (`:messages_unsupported`) rather than silently dropping non-transactional messages later.

### Slot origin for go-forward append consumers

A go-forward **append-log** sink can learn the LSN its slot streams from — the *consistent-point
origin* — by implementing the optional `handle_slot_origin/2` callback. It fires on every connect and
reconnect, before `START_REPLICATION`, for both a freshly-created and a reused slot:

```elixir
@impl true
def handle_slot_origin(origin, %{slot_name: slot, reused?: reused?}) do
  # reused? == false → origin is the CREATE_REPLICATION_SLOT consistent_point (a new slot).
  # reused? == true  → origin is max(durable checkpoint, confirmed_flush_lsn), the
  #                    effective START_REPLICATION origin for a resumed slot.
  # Return :ok to proceed; return {:error, _}/raise to halt fail-closed (e.g. on a gap
  # past your last appended LSN) instead of silently skipping WAL.
  :ok
end
```

`context` is value-free (the slot name and a boolean — never row bytes). A sink that does not
implement the callback is unaffected: no extra query, unchanged streaming. If PostgreSQL cannot
supply a valid logical-slot origin, Replicant halts before the callback and streaming with
`:slot_origin_unavailable`; it never reports a fabricated zero origin.

## Development

```bash
mix deps.get
mix test
mix quality   # format --check-formatted + credo --strict + dialyzer
```

The published redaction, identifier-validation, and tenant-blind invariants live
in [`docs/INVARIANTS.md`](docs/INVARIANTS.md). Repository contributors also follow
the checkout's repository-specific contributor contract.

## Roadmap

The v1 CDC core and every delivery slice have shipped and are closeout-reviewed
against a real-PG16 crash-injection suite:

- **Offline core** — decode / assemble / validate / redact behind the value-free boundary.
- **Live streaming + exactly-once** — the `Postgrex.ReplicationConnection` that owns the slot with ack-after-checkpoint, slot-invalidation fail-closed halt, and the bounded in-flight window (loss = 0, effect-dup = 0).
- **Initial snapshot / backfill** — `EXPORT_SNAPSHOT` → `COPY` → stream-at-snapshot-LSN, gap-free and dup-free.
- **Resumable incremental snapshot** — `snapshot: [mode: :incremental]`: a durable-slot, PK-ordered keyset-chunk backfill interleaved with the live stream and collision-corrected against it, resuming from the last applied chunk after a crash/halt/reconnect (sink-owned effect-once chunks / lib dup ≤ 1 chunk; PK-less whole-table fallback).
- **Lib-owned checkpoint store** — a durable Postgres checkpoint written *after* persist for **non-transactional** sinks (at-least-once, dup bounded to one transaction, never loss), with bounded retry-then-halt on store faults.
- **Batched checkpointing (lib mode)** and **sink-owned atomic batch delivery** — amortize the checkpoint write / the sink's own commit across a batch of transactions.
- **In-progress-transaction streaming** (`pgoutput` v2) and **consumer-side disk spill** — reassemble and deliver a transaction larger than memory, effect-once, instead of halting.
- **Multi-publication per pipeline** — `publication: [p1, p2]` streams the union of several publications through one slot, with fail-closed missing-publication detection.
- **Logical-decoding messages** — opt-in `messages: true` delivers `pg_logical_emit_message` payloads: transactional messages ride `%Transaction.messages` (effect-once); non-transactional via `handle_message/2` (at-least-once).

The sibling libraries live one layer up from this tenant-blind core:

- **[`ash_replicant`](https://github.com/baselabs/ash_replicant)** — the Ash /
  multitenancy / classification sink adapter. Its coordinated 1.0 release will
  require Replicant 1.x so the actual-session identity check cannot be resolved away.
- **[`ash_onetime`](https://hex.pm/packages/ash_onetime)** — the authoritative
  idempotency-key / one-time-nonce admission layer for Ash/Postgres actions.
  AshReplicant uses idempotency, not nonce rejection, for retryable
  non-transactional logical-message effects. Replicant transaction replay remains
  governed by the durable commit-LSN watermark; it is not replaced by AshOnetime.

## Credits

- [**walex**](https://github.com/cpursley/walex) — the `pgoutput` byte parser,
  OID-to-type database, type caster, and array parser this library vendors
  from (MIT). See `NOTICE` for the full attribution chain (cainophile,
  Supabase Realtime, epgsql).
- The `postgrex`/`ash_postgres` split that inspired `arcadic` and
  `ash_arcadic` also shapes the `replicant`/`ash_replicant` layering.

## License

MIT — see [LICENSE](LICENSE). Third-party attributions in [NOTICE](NOTICE).

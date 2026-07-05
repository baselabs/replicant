# Replicant

A framework-agnostic Elixir CDC consumer for Postgres logical replication
(`pgoutput`), delivering committed row changes to a pluggable **sink** with
**zero data loss**: the replication slot advances only after the sink has
durably persisted the transaction.

Replicant is **tenant-blind and classification-blind** — the reliable CDC
consumer sibling to [`arcadic`](https://github.com/baselabs/arcadic) and
`ash_age`. Multitenancy, classification, and Ash resources live one layer up,
in a future `ash_replicant` sink adapter.

> **Status:** live streaming has landed. Replicant owns the replication slot
> via `Postgrex.ReplicationConnection`, acks only after the sink durably
> commits (ack-after-checkpoint), halts fail-closed on slot invalidation, and
> is proven by a real-PG16 crash-injection suite (loss = 0, effect-dup = 0).
> See "How it streams" below.

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

A running pipeline is two processes under a `:one_for_all` supervisor:

- **`Replicant.Connection`** (`Postgrex.ReplicationConnection`) owns the
  replication slot and the socket. It answers every keepalive with the **last
  durably-checkpointed LSN** as the flush position (never the received
  `wal_end`), decodes each WAL message behind the value-free boundary, and
  forwards the decoded message to the assembler — it never runs the sink, so it
  is always free to answer keepalives. It advances the ack asynchronously only
  when the sink signals a durable commit, and halts fail-closed on slot
  invalidation (`wal_status = 'lost'` / `conflicting`), a decode failure, or a
  sustained sink-lag backlog (the bounded in-flight window).
- **`Replicant.AssemblerServer`** applies the sink synchronously, off the
  keepalive path, and halts fail-closed on a destructive schema change or a
  sink write fault.

Because the ack reports only the durable checkpoint, a crash between dispatch
and persist re-delivers from the older `confirmed_flush` and the idempotent
sink dedups — the exactly-once seam that `walex`'s fire-and-forget
`wal_end + 1` ack does not have.

## The 5 critical rules (see `AGENTS.md` for the full text)

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
    {:replicant, "~> 0.1"}
  ]
end
```

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

## Development

```bash
mix deps.get
mix test
mix quality   # format --check-formatted + credo --strict + dialyzer
```

Contributor and agent working rules — including the redaction,
identifier-validation, and tenant-blind invariants — live in
[`AGENTS.md`](AGENTS.md).

## Roadmap

**Plan 1 (offline core)**, **Plan 2 (live streaming + exactly-once)**, and
**initial snapshot / backfill (`replicant-snapshot`)** have all shipped: decode /
assemble / validate / redact, plus the `Postgrex.ReplicationConnection` that owns the
slot with ack-after-checkpoint, slot-invalidation fail-closed halt, the bounded
in-flight window, a real-PG16 crash-injection suite proving loss = 0 / effect-dup = 0,
and the `EXPORT_SNAPSHOT` → `COPY` → stream-at-snapshot-LSN backfill that seeds a mirror
from a populated source gap-free and dup-free.

The remaining slices are the spec §3 non-goals, each a named future subsystem
that composes on this streaming core (the v1 primitive is fail-closed without
it):

- multi-transaction batching (`replicant-batching`),
- `pgoutput` proto ≥ 2 in-progress-transaction streaming (`replicant-streaming`),
- a non-transactional-sink checkpoint store (`replicant-checkpoint-store`), and
- the Ash / tenancy / classification sink (`ash_replicant`, a sibling library).

## Credits

- [**walex**](https://github.com/cpursley/walex) — the `pgoutput` byte parser,
  OID-to-type database, type caster, and array parser this library vendors
  from (MIT). See `NOTICE` for the full attribution chain (cainophile,
  Supabase Realtime, epgsql).
- The `postgrex`/`ash_postgres` split that inspired `arcadic` and
  `ash_arcadic` also shapes the `replicant`/`ash_replicant` layering.

## License

MIT — see [LICENSE](LICENSE). Third-party attributions in [NOTICE](NOTICE).

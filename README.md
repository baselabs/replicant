# Replicant

[![Hex.pm](https://img.shields.io/hexpm/v/replicant.svg)](https://hex.pm/packages/replicant)
[![HexDocs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/replicant)
[![Run in Livebook](https://livebook.dev/badge/v1/blue.svg)](https://livebook.dev/run?url=https%3A%2F%2Fraw.githubusercontent.com%2Fbaselabs%2Freplicant%2Fmain%2Fnotebooks%2Fgetting_started.livemd)

Change data capture for PostgreSQL in Elixir: Replicant streams committed
transactions from a logical replication slot to a sink you write, and advances the
slot only after your sink has durably stored them.

## Why Replicant

- **Zero data loss, by construction.** The replication slot is acknowledged only
  after the sink confirms a durable commit. A crash between delivery and commit
  re-delivers from the last durable position; nothing is skipped. Proven by a
  crash-injection suite on real PostgreSQL (loss = 0, duplicate effects = 0).
- **Snapshot equals stream (1.5).** An initial or incremental snapshot reads exactly
  the columns and rows the publication streams: column lists, row filters and
  generated-column policy are honored, and an ambiguous projection is refused before
  a single row is read.
- **Value-free errors and telemetry.** Every row value is treated as a secret. Decode
  faults become a typed `Replicant.Error` with no row bytes; telemetry metadata is a
  closed allowlist of LSNs, names, counts, durations and reason atoms, enforced at
  emission.
- **Fail-closed on destructive drift.** A dropped column or a replica-identity change
  halts the pipeline instead of emitting incomplete or misattributed rows. Slot
  invalidation, a missing publication and a persistently failing replication command
  halt the same way: stopped and idle, never looping, never dropping data.
- **Tenant-blind core, batteries one layer up.** Replicant knows nothing about
  tenants, classification or Ash. Those live in
  [`ash_replicant`](https://hex.pm/packages/ash_replicant), a sink adapter built on
  this library.

## Install

```elixir
def deps do
  [
    {:replicant, "~> 1.5"}
  ]
end
```

Requires Elixir 1.20 and PostgreSQL 15 through 19 with `wal_level = logical`.

## Quick start

A sink implements `Replicant.Sink`. Two callbacks cover the default mode: `checkpoint/0`
returns the last commit LSN you stored, and `handle_transaction/1` stores one committed
transaction together with its LSN in a single database transaction.

```elixir
defmodule MyApp.OrdersSink do
  @behaviour Replicant.Sink

  alias Replicant.{Change, Transaction}

  @impl true
  def checkpoint do
    case MyApp.Repo.query!("SELECT lsn FROM cdc_checkpoint WHERE id = 1").rows do
      [[lsn]] -> {:ok, lsn}
      [] -> {:ok, nil}
    end
  end

  @impl true
  def handle_transaction(%Transaction{commit_lsn: lsn, changes: changes}) do
    result =
      MyApp.Repo.transaction(fn ->
        {:ok, stored} = checkpoint()

        # Idempotency: a re-delivered transaction at or below the checkpoint is skipped,
        # and the checkpoint never moves backward.
        unless stored && lsn <= stored do
          Enum.each(changes, &apply_change/1)

          MyApp.Repo.query!(
            "INSERT INTO cdc_checkpoint (id, lsn) VALUES (1, $1) " <>
              "ON CONFLICT (id) DO UPDATE SET lsn = EXCLUDED.lsn",
            [lsn]
          )
        end
      end)

    # Ack only after the commit. Any other return, or a raise, halts the pipeline
    # fail-closed and the transaction is re-delivered on restart.
    with {:ok, _} <- result, do: {:ok, lsn}
  end

  defp apply_change(%Change{table: "orders", op: op, record: r, unchanged: unchanged})
       when op in [:insert, :update] do
    # Upsert by primary key. Leave every column in `unchanged` untouched: it is a
    # TOASTed value the UPDATE did not carry, not a NULL.
    MyApp.Orders.upsert(r, skip: unchanged)
  end

  defp apply_change(%Change{table: "orders", op: :delete, old_record: old}) do
    MyApp.Orders.delete(old["id"])
  end

  defp apply_change(%Change{table: "orders", op: :truncate}) do
    MyApp.Orders.delete_all()
  end

  defp apply_change(_other_table), do: :ok
end
```

Start the pipeline. Replicant is an OTP application: `start_link/1` places the pipeline
under Replicant's own supervisor, so one call is the whole wiring. Make it once your Repo
is running, for example at the end of your application's `start/2`.

```elixir
{:ok, _pid} =
  Replicant.start_link(
    connection: [hostname: "db.internal", port: 5432, username: "cdc",
                 password: System.fetch_env!("CDC_PASSWORD"), database: "orders", ssl: true],
    slot_name: "replicant_orders",
    publication: "orders_pub",
    sink: MyApp.OrdersSink,
    go_forward_only: true
  )
```

`go_forward_only: true` streams changes committed from now on. To load the existing rows
first, pass `snapshot: true` instead and add `handle_snapshot/2` and
`handle_snapshot_complete/1` to the sink; the pipeline backfills the published tables and
hands off to streaming at the snapshot's position with no gap and no duplicate. Stop a
pipeline with `Replicant.stop("replicant_orders")`.

The [getting-started Livebook](notebooks/getting_started.livemd) runs all of this against
a live PostgreSQL, including the TOAST sentinel, the duplicate-free ledger, a snapshot
with a column list and a row filter, and logical-decoding messages.

## How it works

A pipeline is two processes under a `:one_for_all` supervisor (three in library
checkpoint mode):

1. **`Replicant.Connection`** owns the replication slot through
   `Postgrex.ReplicationConnection`. It reads `IDENTIFY_SYSTEM` on the replication
   session itself, checks the slot's health, decodes each WAL frame behind the
   value-free boundary and forwards it. It never runs the sink, so it always answers
   keepalives, and the position it reports is the **last durably checkpointed LSN**
   while a transaction is in flight.
2. **`Replicant.AssemblerServer`** assembles complete transactions and calls the sink
   synchronously, off the keepalive path. When the sink returns `{:ok, lsn}` the
   connection advances the acknowledgment.

Every row in a transaction shares one `commit_lsn`, and that LSN is the unit of the
watermark. A sink skips any transaction at or below its checkpoint and upserts by primary
key; at-least-once delivery plus that idempotent sink is exactly-once in effect, and it is
the only honest route there without two-phase commit.

A fail-closed halt emits `[:replicant, :pipeline, :halted]` and stops the pipeline
permanently; restart is an explicit operator action, and it resumes from the durable
checkpoint.

## Guarantees

| Mode | Configuration | Guarantee |
|---|---|---|
| Sink-owned, per transaction (default) | `handle_transaction/1` + `checkpoint/0` | effect-once for a transactional sink: duplicates 0, loss 0 |
| Sink-owned atomic batch | `batch_delivery: [max_transactions: 100, max_delay_ms: 1000]` + `handle_batch/1` | effect-once: the batch's rows and checkpoint commit in one write |
| Library-owned checkpoint | `checkpoint_store: [connection: ..., table: "replicant_checkpoints"]` | at-least-once, duplicates bounded to one transaction, loss 0; for sinks that cannot commit atomically (files, S3, Kafka, HTTP) |
| Point-in-time snapshot | `snapshot: true` | gap-free, duplicate-free handoff to streaming; sink-owned chunks are effect-once |
| Incremental snapshot | `snapshot: [mode: :incremental, chunk_rows: 1000]` | resumes from the last applied chunk after a crash; sink-owned chunks effect-once, library mode duplicates at most one chunk |
| Transactional logical message | `messages: true`, delivered in `%Transaction{}.messages` | effect-once, inherits the transaction's `commit_lsn` |
| Non-transactional logical message | `messages: true` + `handle_message/2` | at-least-once; key your effect on the message LSN |
| Oversized transaction with disk spill | `streaming: [spill: [dir: ..., max_spill_bytes: ...]]` | inherits the active mode's guarantee; delivered as a single-pass lazy `changes` |

Five rules bind both the library and every sink, in full in
[`docs/INVARIANTS.md`](docs/INVARIANTS.md):

1. No row value in an error, log or telemetry event.
2. Identifiers are validated before they reach SQL (`Replicant.Identifier.validate/1`).
3. Exactly-once is at-least-once plus a transaction-watermark-idempotent sink; never a
   naked exactly-once claim.
4. An unchanged TOAST column is a sentinel, not a value; it arrives in
   `%Change{}.unchanged`, never in `record`, and the sink leaves it alone.
5. The library stays tenant-blind.

## Snapshot and stream parity

Since 1.5.0 a snapshot delivers the same shape the stream does. Given a PostgreSQL 15+
publication with a column list and a row filter:

```sql
CREATE TABLE orders (id int PRIMARY KEY, item text, qty int, internal_note text);
CREATE PUBLICATION orders_pub FOR TABLE orders (id, item, qty) WHERE (qty > 0);
```

a `snapshot: true` or incremental backfill reads only `id`, `item` and `qty`, in table
order, and only rows with `qty > 0`. `internal_note` never leaves the server, from the
snapshot or from the stream.

- **Column lists.** On PostgreSQL 15+ the snapshot uses each publication's effective column
  list. Several publications may name the same table with identical lists; differing lists,
  including an unrestricted publication beside a partial list, halt
  `:snapshot_column_list_conflict` before any row is read. pglogical replication sets use
  their `set_att_list` with the same refusal.
- **Row filters.** PostgreSQL 15+ filters from publications that publish INSERT are
  combined with OR, because a snapshot is INSERT-equivalent; an unfiltered INSERT publication
  admits every row, and with no INSERT publication the snapshot delivers no rows. pglogical
  2.4.8 set filters combine with AND, matching the plugin. Predicate text comes only from the
  server catalog (`pg_get_expr`) and is parenthesized before composition; no client text is
  interpolated.
- **Generated columns.** PostgreSQL 15 excludes generated columns from snapshots, as
  pgoutput does from the stream. PostgreSQL 16+ use the publication's effective column list,
  which includes PostgreSQL 18's opt-in `publish_generated_columns = stored`.
- **Keys are never added.** A publication may omit key columns (PostgreSQL itself refuses
  UPDATE and DELETE on such a publication, so this is an INSERT-only shape). The snapshot
  delivers exactly the published projection; an incremental backfill without the complete
  primary key uses a whole-table window instead of keyset pagination.
- **Refusals.** A table whose published projection is empty halts
  `:snapshot_no_publishable_columns` naming the table; a genuinely zero-column table keeps
  its empty reset. On PostgreSQL 15, an explicit list of every live column beside an
  implicit list on a table with dropped attributes passes discovery but is refused by
  pgoutput in the stream, fail-closed; [ADR-0002](docs/adr/0002-multi-publication-per-pipeline.md)
  records why no earlier refusal is possible.

Incremental progress tokens contain primary-key bounds (row data) and `row_filter` text
(publication configuration, which can include literal constants). Persist them; never log
them.

## Compatibility

Replicant connects to PostgreSQL 15 through 19. An unsupported source halts
with `{:config, :server_version_unsupported}` before slot creation.

| Decoder | Table set option | Tested in CI on |
|---|---|---|
| `:pgoutput` (default) | `publication:` (a name or a list) | 15, 16, 17, 18, 19 |
| `:pglogical` (pglogical 2.x, `pglogical_output`) | `replication_sets:` | 15, 16, 17, 18, 19 |

| Capability | Requires |
|---|---|
| Publication column lists and row filters in the stream and in snapshots | `:pgoutput` |
| In-progress transaction streaming (`streaming:`) and disk spill | `:pgoutput` |
| Logical-decoding messages (`messages: true`) | `:pgoutput` |
| Failover slots (`failover: true`) | PostgreSQL 17+; halts `{:config, :failover_unsupported}` below |
| Slot-invalidation columns consulted | `wal_status` on all supported majors, `conflicting` from 16, `invalidation_reason` and `synced` from 17 |

### PostgreSQL version support

Replicant follows the [PGDG support calendar](https://www.postgresql.org/support/versioning/).
The supported majors are **15, 16, 17, 18 and 19**. Each has a real CI lane.
Support ends on the PGDG end-of-life date, with an announcement in the changelog
and this compatibility section. Raising the server floor or refusing a working
configuration requires a Replicant major release.
[ADR-0010](docs/adr/0010-postgresql-server-support.md) records the policy.

CI uses digest-pinned upstream images for 15, 16, 17 and 19. The 18 lane runs
the full suite against the platform engine in `.github/postgres.ref` and runs
the pglogical suite against a pinned upstream 18 companion. CI builds
pglogical 2.4.8 against each supported major and compares its delivery to
pgoutput on the same server. pglogical supports row filters that call
user-defined functions and intersects predicates from multiple replication
sets. Native publications restrict filter expressions and combine them with OR.

A decoder that cannot express a configuration refuses it at start with
`:decoder_capability_unsupported`.

### Upgrading from Replicant 1.x

Replicant 2.0 removes PostgreSQL below 15, `decoder: :wal2json`, and the
`tables:`, `allow_keyless_tables:`, and `schema_check_interval:` options.

Moving from wal2json to pgoutput requires a new slot and a verified checkpoint
handoff. A fresh snapshot can rebuild a current-state mirror. Preserve
append-log history through the handoff or recover it from retained records.
See the [migration procedure](docs/adr/0010-postgresql-server-support.md#wal2json-migration).

## Configuration reference

All options go to `Replicant.start_link/1`; `Replicant.Config` validates them.

| Option | Default | Purpose |
|---|---|---|
| `connection:` | required | Postgrex connection options for the source |
| `slot_name:` | required | replication slot, allowlist-validated |
| `publication:` / `replication_sets:` | required, one per decoder | source tables; a missing publication or set halts at connect |
| `sink:` | required | the `Replicant.Sink` module |
| `go_forward_only:` | `false` | must be `true` to start a state-mirror sink from an empty checkpoint without a snapshot |
| `snapshot:` | `false` | `true` for point-in-time; `[mode: :incremental, chunk_rows: 1000, max_pending_chunks: 4]` for resumable chunks |
| `decoder:` | `:pgoutput` | `:pglogical` for pglogical replication sets |
| `checkpoint_store:` | off | `[connection:, table:, progress_table:, max_retries: 5, retry_backoff_ms: 1000, batch: [...]]`: library-owned checkpoint for non-transactional sinks |
| `batch_delivery:` | off | `[max_transactions: 100, max_delay_ms: 1000]`: atomic batches through `handle_batch/1` |
| `streaming:` | off | `[max_concurrent_txns: 64, spill: [dir:, max_spill_bytes:]]`: in-progress streaming and disk spill |
| `max_inflight_lag:` | 64 MiB | WAL the connection may run ahead of the durable checkpoint before halting `:sink_too_slow` |
| `max_command_retries:` | `5` | connect cycles a pre-stream replication command may fail before the pipeline halts instead of reconnecting forever |
| `messages:` | `false` | deliver `pg_logical_emit_message` payloads; the sink must implement `handle_message/2` |
| `failover:` | `false` | create a `FAILOVER` slot on PostgreSQL 17+ |

Optional sink callbacks extend the contract: `handle_session_identity/2` binds a checkpoint
to the exact source, `handle_slot_origin/2` and `sink_kind/0` serve append-log consumers
that must detect a gap, `handle_schema_change/2` opens a migration window for destructive
DDL, and `snapshot_progress/0` persists incremental progress. Each is documented on
`Replicant.Sink`.

## Telemetry and halts

Every event is value-free by construction: `Replicant.Telemetry` enforces a closed set of
metadata keys and value shapes at emission and raises rather than ship a row value. The
complete event table, the halt-reason table with operator actions, and the start-time
rejections are in [`usage-rules.md`](usage-rules.md). Two to alert on:

- `[:replicant, :pipeline, :halted]` with the structural `reason` atom, on every
  fail-closed halt;
- `[:replicant, :connection, :disconnected]` with `reason: :sink_too_slow` and a signed
  `lag` measurement, when the sink falls behind the in-flight window.

## Documentation

- [Getting started (Livebook)](notebooks/getting_started.livemd): a runnable, self-verifying
  tour, executed in CI against PostgreSQL 15 through 19 on each landing candidate.
- [Usage rules](usage-rules.md): the public surface, casting contract, telemetry and
  halt references.
- [Invariants](docs/INVARIANTS.md) and the
  [architecture decision records](https://github.com/baselabs/replicant/tree/main/docs/adr)
  (on HexDocs: the "Invariants & ADRs" group in the sidebar).
- [Reference deployment](examples/replication_pipeline/README.md): a Postgres-to-Postgres
  bridge as one `docker compose up`, exercised by CI as the public sink API's canary.
- [Changelog](CHANGELOG.md).

Qualification builds the package and docs for the selected release commit. Hex
read-back verifies the exact qualified archives, and the served docs'
`release.json` identifies that commit. GitHub releases retain both archives and
their checksums. See the [publishing and verification guide](CONTRIBUTING.md#publishing-release-operator-only).

## Development

```bash
mix deps.get --check-locked
mix test                 # unit suite; set REPLICANT_TEST_URL for the integration suite
mix quality              # format --check-formatted + credo --strict + dialyzer
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Credits and license

The `pgoutput` byte parser, OID type database, type caster and array parser derive from
[walex](https://github.com/cpursley/walex) (MIT); `NOTICE` carries the full attribution
chain (cainophile, Supabase Realtime, epgsql).

MIT. See [LICENSE](LICENSE).

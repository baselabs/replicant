defmodule Replicant do
  @moduledoc """
  Framework-agnostic Elixir CDC consumer for Postgres logical replication
  (`pgoutput`), delivering committed row changes to a pluggable **sink** with
  **zero data loss**: the replication slot advances only after the sink has
  durably persisted the transaction.

  `replicant` is **tenant-blind and classification-blind** — the reliable CDC
  consumer sibling to `arcadic`. Multitenancy, classification, and Ash resources
  live one layer up, in the `ash_replicant` sink adapter (published on Hex).

  ## LSN representation

  A Postgres LSN is exposed as a single `non_neg_integer` — the 64-bit value
  `(xlog_file <<< 32) ||| xlog_offset` — so that ordinary integer comparison is
  correct WAL ordering, and the same value feeds the wire-level standby status
  update. Use `lsn_to_string/1` for display (`"0/16E3778"`); LSNs are WAL
  positions, not row data, so they are permitted in telemetry metadata.

  ## Starting a pipeline

  Configure each pipeline explicitly at `start_link/1` (no global mutable state):

      Replicant.start_link(
        connection: [hostname: "standby.internal", port: 5432, username: "u",
                     password: "p", database: "orders", ssl: true],  # point at a STANDBY (R-ISO)
        slot_name: "replicant_orders",
        publication: "orders_pub",
        sink: MyApp.OrdersSink,        # implements Replicant.Sink
        go_forward_only: false         # must be true to start a :state_mirror sink from empty
      )

  The `Replicant.Connection` owns the slot, reports the exact replication-session
  identity to the sink before checkpoint lookup, and advances the slot only after
  the sink has durably persisted a transaction (ack-after-checkpoint); the
  `Replicant.AssemblerServer` applies the sink synchronously off the keepalive path.
  """

  @typedoc """
  A Postgres LSN as a single 64-bit integer `(file <<< 32) ||| offset`.

  Integer comparison (`<=`, `>`) is correct WAL ordering, so the exactly-once
  watermark is `txn.commit_lsn <= checkpoint`.
  """
  @type lsn :: non_neg_integer()

  @doc """
  The hex display form of an LSN, e.g. `lsn_to_string(0x16E3778) == "0/16E3778"`.
  Postgres displays `pg_lsn` as uppercase `file/offset` hex with no padding.
  """
  @spec lsn_to_string(lsn()) :: String.t()
  def lsn_to_string(lsn) when is_integer(lsn) and lsn >= 0 do
    file = Bitwise.bsr(lsn, 32)
    offset = Bitwise.band(lsn, 0xFFFFFFFF)
    "#{Integer.to_string(file, 16)}/#{Integer.to_string(offset, 16)}"
  end

  @doc """
  Parse a Postgres `pg_lsn` display string (`"0/16E3778"`) into the uint64 LSN —
  the inverse of `lsn_to_string/1`. The two hex halves are `file`/`offset`;
  `String.to_integer/2` accepts either case (Postgres renders uppercase).
  """
  @spec lsn_from_string(String.t()) :: lsn()
  def lsn_from_string(string) when is_binary(string) do
    [file, offset] = String.split(string, "/", parts: 2)
    Bitwise.bsl(String.to_integer(file, 16), 32) + String.to_integer(offset, 16)
  end

  @doc """
  Start a CDC pipeline. Validates `opts`, enforces the go-forward-only start guard,
  and supervises a `Replicant.Connection` + `Replicant.AssemblerServer` under the
  pipeline `DynamicSupervisor`.

  Options: `:connection` (a Postgrex connection keyword list — point at a STANDBY
  per R-ISO), `:slot_name`, `:publication` (both allowlist-validated identifiers),
  `:sink` (a module implementing `Replicant.Sink`), and `:go_forward_only`
  (default `false`; must be `true` to start a `:state_mirror` sink from an empty
  checkpoint — see `Replicant.Config`).

  `:snapshot` bootstraps the sink from a populated source: `snapshot: true` is the
  point-in-time backfill (`EXPORT_SNAPSHOT` → `COPY` → hand off; requires
  `handle_snapshot/2` + `handle_snapshot_complete/1`). For large tables, the resumable
  form `snapshot: [mode: :incremental, chunk_rows: 1000, max_pending_chunks: 4]` streams
  from a durable slot while a linked reader backfills in PK-ordered keyset chunks through
  `handle_snapshot/2`, resuming from the last applied chunk after a crash/halt/reconnect
  (sink-owned mode requires `snapshot_progress/0`; lib mode carries progress in the
  checkpoint store). A sink-owned adapter returns `{:ok, :backfill_pending}` after it
  durably arms a backfill but before the first chunk token exists; Replicant then resumes
  discovery from the live slot origin after a pre-chunk crash. Both snapshot modes are
  mutually exclusive with `go_forward_only: true`.

  Returns `{:ok, pipeline_pid}` or `{:error, reason}` where `reason` includes
  `:invalid_identifier` / `:invalid_sink` / `:config_invalid` / `:go_forward_required`,
  plus the start-gate rejections `:conflicting_start_mode`, `:snapshot_unsupported`,
  `:batch_unsupported`, and `:messages_unsupported` (e.g. `messages: true` without a
  `handle_message/2` callback in the sink).
  """
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) when is_list(opts) do
    with {:ok, config} <- Replicant.Config.validate(opts),
         :ok <- Replicant.Config.guard(config) do
      Replicant.Supervisor.start_pipeline(config)
    end
  end

  @doc "Stop a running pipeline by slot name (idempotent)."
  @spec stop(String.t()) :: :ok
  def stop(slot_name) when is_binary(slot_name) do
    Replicant.Supervisor.stop_pipeline(slot_name)
  end
end

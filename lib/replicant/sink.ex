defmodule Replicant.Sink do
  @moduledoc """
  The pluggable sink contract (spec §6). A sink durably persists a transaction
  AND its checkpoint atomically, returns the commit LSN, and is idempotent on the
  transaction-granularity watermark (skip any `txn.commit_lsn <= checkpoint`;
  upsert rows by table PK).

  ## Callbacks

  | callback | required | purpose |
  |---|---|---|
  | `c:checkpoint/0` | in sink-owned mode | last durably-persisted commit LSN (`nil` = never). In **lib mode** (`:checkpoint_store` configured) the library owns the checkpoint and this callback is not required. |
  | `c:handle_transaction/1` | **yes** | persist the txn + checkpoint atomically, return `{:ok, lsn}` |
  | `c:handle_schema_change/2` | no | accept/decline a `SchemaChange`; default halts destructive |
  | `c:sink_kind/0` | no | `:state_mirror` (default) or `:append_log` |
  | `c:handle_snapshot/2` | no | persist a snapshot batch; redo-safe reset on `first_for_table?` |
  | `c:handle_snapshot_complete/1` | no | snapshot handoff commit; persist `checkpoint := snapshot_lsn` |

  These are all `@optional_callbacks` so a minimal sink implementing only the two
  mandatory callbacks compiles with zero warnings under `--warnings-as-errors`. The
  Assembler dispatches the optional ones via `function_exported?/3`, providing the
  documented default. The two snapshot callbacks come as a pair — `supports_snapshot?/1`
  gates `snapshot: true` on BOTH being present.

  ## Lib mode (non-transactional sinks)

  When the pipeline is configured with `:checkpoint_store`, the library owns the
  checkpoint: it reads it on connect and writes it (to a Postgres table) AFTER
  `handle_transaction/1` returns `{:ok, _}`, then advances the ack. A lib-mode sink
  therefore implements ONLY `handle_transaction/1` (persisting DATA; it still returns
  `{:ok, lsn}` — the value is ignored, per the §14.20 discipline) and need not
  implement `checkpoint/0`. `Config` enforces `checkpoint/0` presence at start for
  sink-owned mode. The guarantee is at-least-once, dup bounded to one transaction,
  never loss — NOT effect-once (a non-transactional sink cannot dedup).
  """

  @doc "Last durably-persisted commit LSN, for resume AND as the dedup watermark."
  @callback checkpoint() :: {:ok, Replicant.lsn() | nil} | {:error, term()}

  @doc """
  Persist the whole transaction AND its checkpoint atomically; return the commit
  LSN. Idempotency: skip if `txn.commit_lsn <= checkpoint()`; upsert rows by PK.
  """
  @callback handle_transaction(Replicant.Transaction.t()) ::
              {:ok, Replicant.lsn()} | {:error, term()}

  @doc """
  Optional. When a sink does not implement this, the Assembler (Task 13) applies
  the default: an `:additive` change auto-applies; a `:destructive` change halts
  the pipeline fail-closed. Implement it to accept (`:ok`) or decline (`{:error, _}`)
  a `Replicant.SchemaChange`.
  """
  @callback handle_schema_change(Replicant.SchemaChange.t(), map()) :: :ok | {:error, term()}

  @doc "Optional (default `:state_mirror`). `:append_log` sinks receive appends."
  @callback sink_kind() :: :state_mirror | :append_log

  @doc """
  Optional. Persist a batch of snapshot (backfill) rows for `context.table` — each a
  `%Replicant.Change{op: :snapshot}` — upserting by table PK. Do NOT advance the
  checkpoint here (that is `c:handle_snapshot_complete/1`).

  REDO-SAFETY — a hard obligation, not advisory: when `context.first_for_table?` is
  `true`, the sink MUST clear this table's prior mirror state atomically with (or before)
  applying the batch. A sink that ignores `first_for_table?` forfeits redo-safety — a row
  deleted upstream between a failed attempt and its retry survives as a ghost. The library
  guarantees at least one call per publication table (even a zero-row table), so the reset
  always fires. `context.snapshot_lsn` is the snapshot's consistent point.

  A non-`:ok` return (or a raise/throw/exit) aborts and re-runs the WHOLE snapshot from
  scratch — it is NOT a per-batch retry.
  """
  @callback handle_snapshot([Replicant.Change.t()], context) :: :ok | {:error, term()}
            when context: %{
                   snapshot_lsn: Replicant.lsn(),
                   table: String.t(),
                   first_for_table?: boolean()
                 }

  @doc """
  Optional. The snapshot handoff commit, called once after all batches. Durably persist
  `checkpoint := snapshot_lsn` and return it. Until this succeeds the checkpoint stays
  `nil`, so a crash before it re-runs the whole snapshot (spec §8). A non-`{:ok, _}`
  return (or a raise/throw/exit) halts the pipeline fail-closed.
  """
  @callback handle_snapshot_complete(snapshot_lsn :: Replicant.lsn()) ::
              {:ok, Replicant.lsn()} | {:error, term()}

  @optional_callbacks [
    checkpoint: 0,
    handle_schema_change: 2,
    sink_kind: 0,
    handle_snapshot: 2,
    handle_snapshot_complete: 1
  ]

  @doc """
  The sink's kind, defaulting to `:state_mirror` when `c:sink_kind/0` is not
  implemented. (Plan 2's start guard refuses a `:state_mirror` sink from an empty
  checkpoint without `go_forward_only: true`.)
  """
  @spec sink_kind(module()) :: :state_mirror | :append_log
  def sink_kind(module) do
    if function_exported?(module, :sink_kind, 0) do
      # Coerce ANY unrecognized return to the strict :state_mirror default. sink_kind
      # gates the go-forward start guard; treating a typo'd/invalid kind as the laxer
      # :append_log would fail OPEN on a safety check and let an empty state-mirror
      # sink partial-deliver. Only an explicit :append_log bypasses the guard.
      case module.sink_kind() do
        :append_log -> :append_log
        _other -> :state_mirror
      end
    else
      :state_mirror
    end
  end

  @doc """
  True when `module` implements BOTH snapshot callbacks — the config gate for
  `snapshot: true` (spec §7). A partial implementation is rejected at start
  (`:snapshot_unsupported`) rather than half-running a backfill.
  """
  @spec supports_snapshot?(module()) :: boolean()
  def supports_snapshot?(module) do
    function_exported?(module, :handle_snapshot, 2) and
      function_exported?(module, :handle_snapshot_complete, 1)
  end
end

defmodule Replicant.Sink do
  @moduledoc """
  The pluggable sink contract (spec §6). A sink durably persists a transaction
  AND its checkpoint atomically, returns the commit LSN, and is idempotent on the
  transaction-granularity watermark (skip any `txn.commit_lsn <= checkpoint`;
  upsert rows by table PK).

  ## Callbacks

  | callback | required | purpose |
  |---|---|---|
  | `c:checkpoint/0` | **yes** | last durably-persisted commit LSN (`nil` = never) |
  | `c:handle_transaction/1` | **yes** | persist the txn + checkpoint atomically, return `{:ok, lsn}` |
  | `c:handle_schema_change/2` | no | accept/decline a `SchemaChange`; default halts destructive |
  | `c:sink_kind/0` | no | `:state_mirror` (default) or `:append_log` |

  `handle_schema_change/2` and `sink_kind/0` are `@optional_callbacks` so a
  minimal sink implementing only the two mandatory callbacks compiles with zero
  warnings under `--warnings-as-errors`. The Assembler dispatches the optional
  ones via `function_exported?/3`, providing the documented default.
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

  @optional_callbacks [handle_schema_change: 2, sink_kind: 0]

  @doc """
  The sink's kind, defaulting to `:state_mirror` when `c:sink_kind/0` is not
  implemented. (Plan 2's start guard refuses a `:state_mirror` sink from an empty
  checkpoint without `go_forward_only: true`.)
  """
  @spec sink_kind(module()) :: :state_mirror | :append_log
  def sink_kind(module) do
    if function_exported?(module, :sink_kind, 0) do
      module.sink_kind()
    else
      :state_mirror
    end
  end
end

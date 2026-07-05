defmodule Replicant.Config do
  @moduledoc """
  Validates `Replicant.start_link/1` options and enforces the **go-forward-only
  start guard** (spec §3/§6): a `:state_mirror` sink resuming from an empty
  checkpoint without `go_forward_only: true` would silently deliver partial data
  from the slot's creation point, so it is refused at start.

  Pure — no processes, no connection. `Replicant.start_link/1` (the facade) calls
  `validate/1` then `guard/1` before spawning a pipeline.
  """

  alias Replicant.{Connection, Identifier, Sink}

  @type t :: %{
          connection: keyword(),
          slot_name: String.t(),
          publication: String.t(),
          sink: module(),
          go_forward_only: boolean(),
          max_inflight_lag: pos_integer()
        }

  @doc """
  Validate raw `start_link` options into a normalised config map, or return a
  plain-atom error: `:config_invalid` (missing/mis-shaped connection or opts),
  `:invalid_identifier` (slot/publication fails the Postgres-identifier
  allowlist), `:invalid_sink` (sink is not a module exporting the two mandatory
  callbacks).
  """
  @spec validate(keyword()) ::
          {:ok, t()}
          | {:error, :config_invalid | :invalid_identifier | :invalid_sink}
  def validate(opts) when is_list(opts) do
    with {:ok, connection} <- fetch_connection(opts),
         {:ok, slot_name} <- fetch_identifier(opts, :slot_name),
         {:ok, publication} <- fetch_identifier(opts, :publication),
         {:ok, sink} <- fetch_sink(opts),
         {:ok, max_inflight_lag} <- fetch_max_inflight_lag(opts) do
      {:ok,
       %{
         connection: connection,
         slot_name: slot_name,
         publication: publication,
         sink: sink,
         go_forward_only: Keyword.get(opts, :go_forward_only, false) == true,
         max_inflight_lag: max_inflight_lag
       }}
    end
  end

  def validate(_opts), do: {:error, :config_invalid}

  @doc """
  The go-forward-only start guard. Refuses ONLY the exact unsafe triple — a
  `:state_mirror` sink (default kind) with a **definitively empty** checkpoint
  (`{:ok, nil}`) and `go_forward_only: false`. An `:append_log` sink, a non-nil
  checkpoint, `go_forward_only: true`, OR a checkpoint READ fault (raise/exit/
  `{:error, _}`) all pass — the read-fault path is deliberately fail-open (spec
  §14.15: a re-dispatched already-persisted txn is deduped by the §6 idempotent
  sink; only a definitive empty checkpoint proves partial-delivery risk).
  """
  @spec guard(t()) :: :ok | {:error, :go_forward_required}
  def guard(%{sink: sink, go_forward_only: go_forward_only}) do
    cond do
      go_forward_only == true -> :ok
      Sink.sink_kind(sink) != :state_mirror -> :ok
      checkpoint_definitively_empty?(sink) -> {:error, :go_forward_required}
      true -> :ok
    end
  end

  defp checkpoint_definitively_empty?(sink) do
    case safe_checkpoint(sink) do
      {:ok, nil} -> true
      _other -> false
    end
  end

  # A checkpoint read fault (raise/exit) is fail-open (§14.15) and value-free: we
  # never inspect the reason. Return a sentinel that is NOT {:ok, nil}, so the
  # guard treats "unknown" as "not definitively empty" → allow start.
  defp safe_checkpoint(sink) do
    sink.checkpoint()
  rescue
    _ -> :read_fault
  catch
    _kind, _reason -> :read_fault
  end

  # The bounded in-flight window ceiling (WAL bytes, spec §4). Omitted → the
  # Connection default. A present value must be a positive integer; anything else is
  # a config error (never a silent fallback that would mask a mis-set bound).
  defp fetch_max_inflight_lag(opts) do
    case Keyword.fetch(opts, :max_inflight_lag) do
      :error -> {:ok, Connection.default_max_inflight_lag()}
      {:ok, n} when is_integer(n) and n > 0 -> {:ok, n}
      {:ok, _bad} -> {:error, :config_invalid}
    end
  end

  defp fetch_connection(opts) do
    case Keyword.get(opts, :connection) do
      conn when is_list(conn) and conn != [] -> {:ok, conn}
      _ -> {:error, :config_invalid}
    end
  end

  defp fetch_identifier(opts, key) do
    value = Keyword.get(opts, key)

    case Identifier.validate(value) do
      :ok -> {:ok, value}
      {:error, :invalid_identifier} = err -> err
    end
  end

  defp fetch_sink(opts) do
    sink = Keyword.get(opts, :sink)

    if is_atom(sink) and sink != nil and Code.ensure_loaded?(sink) and
         function_exported?(sink, :checkpoint, 0) and
         function_exported?(sink, :handle_transaction, 1) do
      {:ok, sink}
    else
      {:error, :invalid_sink}
    end
  end
end

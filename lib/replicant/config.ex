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
          snapshot: boolean(),
          max_inflight_lag: pos_integer(),
          checkpoint_store: keyword() | nil
        }

  @doc """
  Validate raw `start_link` options into a normalised config map, or return a
  plain-atom error: `:config_invalid` (missing/mis-shaped connection or opts),
  `:invalid_identifier` (slot/publication fails the Postgres-identifier
  allowlist), `:invalid_sink` (sink is not a module exporting the two mandatory
  callbacks), `:conflicting_start_mode` (`go_forward_only: true` AND
  `snapshot: true` — mutually exclusive start intents), `:snapshot_unsupported`
  (`snapshot: true` but the sink is missing one/both snapshot callbacks).
  """
  @spec validate(keyword()) ::
          {:ok, t()}
          | {:error,
             :config_invalid
             | :invalid_identifier
             | :invalid_sink
             | :conflicting_start_mode
             | :snapshot_unsupported}
  def validate(opts) when is_list(opts) do
    with {:ok, connection} <- fetch_connection(opts),
         {:ok, slot_name} <- fetch_identifier(opts, :slot_name),
         {:ok, publication} <- fetch_identifier(opts, :publication),
         {:ok, checkpoint_store} <- fetch_checkpoint_store(opts),
         {:ok, sink} <- fetch_sink(opts, checkpoint_store != nil),
         {:ok, max_inflight_lag} <- fetch_max_inflight_lag(opts),
         go_forward_only = Keyword.get(opts, :go_forward_only, false) == true,
         snapshot = Keyword.get(opts, :snapshot, false) == true,
         :ok <- validate_start_mode(go_forward_only, snapshot),
         :ok <- validate_snapshot_support(snapshot, sink) do
      {:ok,
       %{
         connection: connection,
         slot_name: slot_name,
         publication: publication,
         sink: sink,
         go_forward_only: go_forward_only,
         snapshot: snapshot,
         max_inflight_lag: max_inflight_lag,
         checkpoint_store: checkpoint_store
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
  `snapshot: true` ALSO bypasses the empty-checkpoint refusal (alongside
  `go_forward_only: true`): the backfill IS the safe seed, so an empty checkpoint
  is the expected first-run state, not a partial-delivery risk.
  """
  @spec guard(t()) :: :ok | {:error, :go_forward_required}
  # Lib mode (a present :checkpoint_store): the library owns the checkpoint, so the
  # empty-checkpoint go-forward enforcement moves to connect time — the guard defers here.
  def guard(%{checkpoint_store: store}) when is_list(store), do: :ok

  def guard(%{sink: sink, go_forward_only: go_forward_only, snapshot: snapshot}) do
    cond do
      go_forward_only == true -> :ok
      snapshot == true -> :ok
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

  # go_forward_only and snapshot are mutually exclusive explicit start intents.
  defp validate_start_mode(true, true), do: {:error, :conflicting_start_mode}
  defp validate_start_mode(_gfo, _snapshot), do: :ok

  # snapshot: true requires BOTH snapshot callbacks — a partial sink is refused rather
  # than half-running a backfill (spec §7).
  defp validate_snapshot_support(false, _sink), do: :ok

  defp validate_snapshot_support(true, sink) do
    if Sink.supports_snapshot?(sink), do: :ok, else: {:error, :snapshot_unsupported}
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

  # A present :checkpoint_store must be a keyword with a non-empty :connection list and,
  # if given, a :table that passes the identifier allowlist (Critical Rule 2). Absent →
  # nil (sink-owned mode). A mis-shaped value is a config error, never a silent fallback.
  defp fetch_checkpoint_store(opts) do
    case Keyword.fetch(opts, :checkpoint_store) do
      :error ->
        {:ok, nil}

      {:ok, store} when is_list(store) ->
        with conn when is_list(conn) and conn != [] <- Keyword.get(store, :connection),
             :ok <- validate_store_table(Keyword.get(store, :table)) do
          {:ok, store}
        else
          {:error, :invalid_identifier} = err -> err
          _ -> {:error, :config_invalid}
        end

      {:ok, _bad} ->
        {:error, :config_invalid}
    end
  end

  defp validate_store_table(nil), do: :ok
  defp validate_store_table(table), do: Identifier.validate(table)

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

  # In lib mode (a present :checkpoint_store) the library owns the checkpoint, so the sink
  # need not implement checkpoint/0 — only handle_transaction/1 is mandatory. Sink-owned
  # mode still requires both callbacks.
  defp fetch_sink(opts, lib_mode?) do
    sink = Keyword.get(opts, :sink)

    cond do
      not (is_atom(sink) and sink != nil and Code.ensure_loaded?(sink)) ->
        {:error, :invalid_sink}

      not function_exported?(sink, :handle_transaction, 1) ->
        {:error, :invalid_sink}

      lib_mode? ->
        {:ok, sink}

      function_exported?(sink, :checkpoint, 0) ->
        {:ok, sink}

      true ->
        {:error, :invalid_sink}
    end
  end
end

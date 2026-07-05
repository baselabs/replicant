defmodule Replicant.CheckpointStore do
  @moduledoc """
  The lib-owned checkpoint store (spec §7) for non-transactional sinks. A GenServer
  over a normal Postgrex connection that owns one `replicant_checkpoints` row per
  slot and reads/writes `commit_lsn` behind the value-free boundary (Critical Rule
  1 — the store carries slot_name + LSN only; a Postgrex fault is scrubbed to a
  structural `%Replicant.Error{}`, never echoing a parameter).

  The connection uses a **non-sync connect** so a transient store blip at boot does
  not fail the `:temporary` pipeline (Postgrex reconnects in the background); a
  genuine outage surfaces as a query fault that halts fail-closed. The table is
  created + shape-probed lazily on the first read/write (once), so `init/1` never
  blocks on the DB.
  """
  use GenServer

  alias Replicant.{Error, QueryBuilder, Telemetry}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    slot_name = Keyword.fetch!(opts, :slot_name)
    GenServer.start_link(__MODULE__, opts, name: via(slot_name))
  end

  @doc "The Registry via-name a pipeline's CheckpointStore registers under."
  @spec via(String.t()) :: {:via, module(), term()}
  def via(slot_name), do: {:via, Registry, {Replicant.Registry, {slot_name, :checkpoint_store}}}

  @doc "Read the durable checkpoint for this slot (`nil` = never written) or a value-free error."
  @spec read(GenServer.server()) :: {:ok, Replicant.lsn() | nil} | {:error, Error.t()}
  def read(server) do
    GenServer.call(server, :read)
  catch
    # A store-call EXIT (call timeout, or :noproc if the store died / never started) must
    # NOT crash the calling Connection — convert it to a value-free error so the connect
    # :fault (read) and snapshot-handoff halt (write) fail-closed paths engage. The store's
    # own guarded/1 already scrubs a RETURNED fault; this covers the EXIT class it cannot.
    :exit, _ -> {:error, %Error{reason: :checkpoint_store_failed}}
  end

  @doc "Write the checkpoint (`INSERT ... ON CONFLICT`) or return a value-free error. Never advance the ack on `{:error, _}`."
  @spec write(GenServer.server(), Replicant.lsn()) :: :ok | {:error, Error.t()}
  def write(server, lsn) when is_integer(lsn) and lsn >= 0 do
    GenServer.call(server, {:write, lsn})
  catch
    :exit, _ -> {:error, %Error{reason: :checkpoint_store_failed}}
  end

  @impl true
  def init(opts) do
    slot_name = Keyword.fetch!(opts, :slot_name)
    store = Keyword.fetch!(opts, :checkpoint_store)
    conn_opts = Keyword.fetch!(store, :connection)
    table = Keyword.get(store, :table, "replicant_checkpoints")
    # Library control opts win (Keyword.merge, second wins): non-sync connect so a
    # boot blip self-heals; a single pooled conn (low volume, serialized here).
    merged = Keyword.merge(conn_opts, sync_connect: false, pool_size: 1)
    {:ok, conn} = Postgrex.start_link(merged)
    {:ok, %{conn: conn, table: table, slot_name: slot_name, ensured: false}}
  end

  @impl true
  def handle_call(:read, _from, state) do
    with {:ok, state} <- ensure(state),
         {:ok, lsn} <- guarded(fn -> do_read(state) end) do
      Telemetry.event([:replicant, :checkpoint_store, :read], %{}, %{
        slot_name: state.slot_name,
        commit_lsn: lsn
      })

      {:reply, {:ok, lsn}, state}
    else
      {:error, %Error{} = e} -> reply_failed(e, state)
    end
  end

  def handle_call({:write, lsn}, _from, state) do
    with {:ok, state} <- ensure(state),
         :ok <- guarded(fn -> do_write(state, lsn) end) do
      Telemetry.event([:replicant, :checkpoint_store, :written], %{}, %{
        slot_name: state.slot_name,
        commit_lsn: lsn
      })

      {:reply, :ok, state}
    else
      {:error, %Error{} = e} -> reply_failed(e, state)
    end
  end

  # Ensure the table exists AND has the expected shape, once. A create/probe fault
  # (incl. a wrong pre-existing column type) is fail-closed.
  defp ensure(%{ensured: true} = state), do: {:ok, state}

  defp ensure(%{ensured: false, conn: conn, table: table} = state) do
    result =
      guarded(fn ->
        with {:ok, create} <- table_ok(QueryBuilder.checkpoint_ensure_table(table)),
             {:ok, _} <- Postgrex.query(conn, create, []) do
          probe_shape(conn, table)
        end
      end)

    case result do
      :ok -> {:ok, %{state | ensured: true}}
      {:error, %Error{}} = err -> err
    end
  end

  defp probe_shape(conn, table) do
    case Postgrex.query(conn, QueryBuilder.checkpoint_column_probe(), [table]) do
      {:ok, %Postgrex.Result{rows: [["bigint"]]}} ->
        :ok

      {:ok, %Postgrex.Result{rows: [[other]]}} ->
        {:error, %Error{reason: :checkpoint_store_schema_mismatch, shape: "commit_lsn=#{other}"}}

      {:ok, %Postgrex.Result{rows: []}} ->
        {:error, %Error{reason: :checkpoint_store_schema_mismatch, shape: "commit_lsn=absent"}}

      {:error, _} = err ->
        err
    end
  end

  defp do_read(%{conn: conn, table: table, slot_name: slot}) do
    with {:ok, sql} <- table_ok(QueryBuilder.checkpoint_read(table)) do
      case Postgrex.query(conn, sql, [slot]) do
        {:ok, %Postgrex.Result{rows: [[lsn]]}} when is_integer(lsn) -> {:ok, lsn}
        {:ok, %Postgrex.Result{rows: []}} -> {:ok, nil}
        {:error, _} = err -> err
      end
    end
  end

  defp do_write(%{conn: conn, table: table, slot_name: slot}, lsn) do
    with {:ok, sql} <- table_ok(QueryBuilder.checkpoint_upsert(table)) do
      case Postgrex.query(conn, sql, [slot, lsn]) do
        {:ok, %Postgrex.Result{}} -> :ok
        {:error, _} = err -> err
      end
    end
  end

  # A QueryBuilder identifier rejection is a config error, surfaced value-free.
  defp table_ok({:ok, _} = ok), do: ok

  defp table_ok({:error, :invalid_identifier}),
    do: {:error, %Error{reason: :config_invalid, shape: "checkpoint_store table"}}

  # The value-free boundary: a store fault (RAISED or RETURNED) can embed a parameter in
  # its message or `.postgres` map — scrub to a structural Error, never inspecting the
  # message (Critical Rule 1; mirrors Snapshotter.snapshot_error/1). This must scrub EVERY
  # exception a query can surface, not only `%Postgrex.Error{}`: a genuine connection outage
  # RETURNS `{:error, %DBConnection.ConnectionError{}}` (the boot-blip / persistent-outage
  # path the design relies on), which is neither a `%Postgrex.Error{}` nor a raise — leaving
  # it unscrubbed would fall through to `ensure/1`'s `case` and crash the store
  # (CaseClauseError). A returned `%Error{}` (schema mismatch, config) passes through unchanged.
  defp guarded(fun) do
    case fun.() do
      {:error, %Error{}} = err -> err
      {:error, e} when is_exception(e) -> {:error, store_error(e)}
      other -> other
    end
  rescue
    e -> {:error, store_error(e)}
  catch
    _kind, _reason -> {:error, %Error{reason: :checkpoint_store_failed}}
  end

  # Every caller passes an exception struct: the `{:error, e} when is_exception(e)` branch
  # above (a returned `%Postgrex.Error{}` / `%DBConnection.ConnectionError{}` / any query
  # exception), or the `rescue` clause (a rescued value is always normalized to an exception
  # struct). So the module name is always available — no non-struct fallback (it would be
  # dead code dialyzer flags). This diverges from `Snapshotter.snapshot_error/1`, whose
  # bare-atom clause is live only because `do_snapshot` calls it with a raw validation
  # reason; no such call path exists here.
  defp store_error(%{__struct__: mod}),
    do: %Error{reason: :checkpoint_store_failed, shape: inspect(mod)}

  defp reply_failed(%Error{} = e, state) do
    Telemetry.event([:replicant, :checkpoint_store, :failed], %{}, %{
      slot_name: state.slot_name,
      reason: e.reason
    })

    {:reply, {:error, e}, state}
  end
end

defmodule Replicant.Snapshotter do
  @moduledoc """
  Reads a consistent snapshot of the publication's tables (spec §4) on its own normal
  Postgrex connection, at the LSN a durable slot exported via `EXPORT_SNAPSHOT`, and
  pushes the rows to the sink as `%Change{op: :snapshot}` batches — then durably sets the
  sink checkpoint to the snapshot's consistent point (the handoff commit). Spawned and
  monitored by `Replicant.Connection`, which holds the exported snapshot valid by staying
  idle (proven safe past `wal_sender_timeout`, spec §11).

  ## Value-free boundary (Critical Rule 1)
  A Postgrex query/cursor fault — raised OR returned — can embed row/column values in its
  message. Every fault is scrubbed to a value-free `%Replicant.Error{reason:
  :snapshot_failed}` (only a structural module name kept) before the Connection is told.
  On success it sends `{:snapshot_done, consistent_point}`; on any fault
  `{:snapshot_failed, %Error{}}`. It reads on a SEPARATE connection, so it never blocks
  the Connection's keepalive path.
  """

  alias Replicant.{Change, Error, QueryBuilder, Telemetry}

  # Cursor page size — bounds memory (Postgrex.stream FETCHes `max_rows` at a time).
  @batch 1000

  @type args :: %{
          snapshot_name: String.t(),
          consistent_point: Replicant.lsn(),
          connection: keyword(),
          publication: String.t(),
          sink: module(),
          reply_to: pid()
        }

  @doc "Spawn + monitor the snapshotter. Returns `{pid, monitor_ref}`."
  @spec start(args()) :: {pid(), reference()}
  def start(args), do: spawn_monitor(__MODULE__, :run, [args])

  @doc false
  @spec run(args()) :: :ok
  def run(%{reply_to: reply_to, consistent_point: cp} = args) do
    Telemetry.event([:replicant, :snapshot, :started], %{}, %{commit_lsn: cp})

    case snapshot(args) do
      {:ok, lsn} ->
        send(reply_to, {:snapshot_done, lsn})

      {:error, %Error{} = e} ->
        Telemetry.event([:replicant, :snapshot, :failed], %{}, %{reason: e.reason})
        send(reply_to, {:snapshot_failed, e})
    end

    :ok
  end

  # The whole snapshot runs inside the value-free boundary (scrub raises AND catches).
  defp snapshot(args) do
    do_snapshot(args)
  rescue
    e -> {:error, snapshot_error(e)}
  catch
    _kind, _reason -> {:error, %Error{reason: :snapshot_failed}}
  end

  defp do_snapshot(%{
         connection: conn_opts,
         publication: publication,
         sink: sink,
         snapshot_name: name,
         consistent_point: cp
       }) do
    start_mono = System.monotonic_time(:millisecond)
    {:ok, set_snapshot_sql} = QueryBuilder.set_transaction_snapshot(name)
    {:ok, pub_tables_sql} = QueryBuilder.publication_tables(publication)
    {:ok, db} = Postgrex.start_link(conn_opts ++ [pool_size: 1])

    try do
      result =
        Postgrex.transaction(
          db,
          fn c ->
            Postgrex.query!(c, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", [])
            Postgrex.query!(c, set_snapshot_sql, [])
            tables = Postgrex.query!(c, pub_tables_sql, []).rows

            Enum.reduce(tables, 0, fn [schema, table, qualified], acc ->
              acc + copy_table(c, sink, cp, schema, table, qualified)
            end)
          end,
          timeout: :infinity
        )

      case result do
        {:ok, total} -> complete(sink, cp, total, start_mono)
        {:error, reason} -> {:error, snapshot_error(reason)}
      end
    after
      GenServer.stop(db)
    end
  end

  # Stream one table's rows via a server-side cursor (bounded memory), delivering each
  # non-empty batch to the sink. Guarantees >= 1 dispatch per table (an empty one with
  # first_for_table? = true when the table has no rows), so the sink's per-table reset
  # always fires (redo-safety, spec §6.1).
  defp copy_table(c, sink, cp, schema, table, qualified) do
    stream = Postgrex.stream(c, "SELECT * FROM #{qualified}", [], max_rows: @batch)

    {dispatched?, count} =
      Enum.reduce(stream, {false, 0}, fn %Postgrex.Result{columns: cols, rows: rows},
                                         {dispatched?, count} ->
        if rows == [] do
          {dispatched?, count}
        else
          changes = Enum.map(rows, &build_change(schema, table, cols, &1))
          dispatch_batch!(c, sink, changes, cp, schema, table, not dispatched?)
          {true, count + length(rows)}
        end
      end)

    unless dispatched?, do: dispatch_batch!(c, sink, [], cp, schema, table, true)

    Telemetry.event([:replicant, :snapshot, :table_completed], %{}, %{
      table: "#{schema}.#{table}",
      change_count: count
    })

    count
  end

  # Deliver one batch. A sink {:error, _} rolls the read transaction back with a
  # value-free token → the transaction returns {:error, :sink_snapshot_error}, scrubbed
  # by do_snapshot. A sink RAISE propagates out and is scrubbed by snapshot/1's rescue.
  defp dispatch_batch!(c, sink, changes, cp, schema, table, first?) do
    ctx = %{snapshot_lsn: cp, table: "#{schema}.#{table}", first_for_table?: first?}

    case sink.handle_snapshot(changes, ctx) do
      :ok -> :ok
      {:error, _reason} -> Postgrex.rollback(c, :sink_snapshot_error)
    end
  end

  # The handoff commit: durably set checkpoint := cp. Ack the KNOWN consistent point
  # (never a value the sink returns — spec §14.20 discipline), scrubbing any fault.
  # `total` is the snapshot's total row count; `start_mono` seeds the duration measurement.
  defp complete(sink, cp, total, start_mono) do
    case sink.handle_snapshot_complete(cp) do
      {:ok, lsn} when is_integer(lsn) ->
        duration = System.monotonic_time(:millisecond) - start_mono

        Telemetry.event([:replicant, :snapshot, :completed], %{duration: duration}, %{
          commit_lsn: cp,
          change_count: total
        })

        {:ok, cp}

      _other ->
        {:error, %Error{reason: :snapshot_failed}}
    end
  rescue
    e -> {:error, snapshot_error(e)}
  catch
    _kind, _reason -> {:error, %Error{reason: :snapshot_failed}}
  end

  @doc false
  @spec build_change(String.t(), String.t(), [String.t()], [term()]) :: Change.t()
  def build_change(schema, table, columns, values) do
    record = columns |> Enum.zip(values) |> Map.new()
    %Change{op: :snapshot, schema: schema, table: table, record: record, commit_lsn: nil}
  end

  @doc false
  @spec snapshot_error(term()) :: Error.t()
  def snapshot_error(%{__struct__: mod}),
    do: %Error{reason: :snapshot_failed, shape: inspect(mod)}

  def snapshot_error(_other), do: %Error{reason: :snapshot_failed}
end

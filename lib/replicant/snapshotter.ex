defmodule Replicant.Snapshotter do
  @moduledoc """
  Reads a consistent snapshot of the publication's tables (spec §4) on its own normal
  Postgrex connection, at the LSN a durable slot exported via `EXPORT_SNAPSHOT`, and
  pushes the rows to the sink as `%Change{op: :snapshot}` batches — then durably sets the
  sink checkpoint to the snapshot's consistent point (the handoff commit). Spawned and
  LINKED to `Replicant.Connection`, which holds the exported snapshot valid by staying
  idle (proven safe past `wal_sender_timeout`, spec §11). The link binds the snapshotter's
  lifetime to the pipeline: a Connection/pipeline teardown mid-snapshot tears the
  snapshotter down with it, so an orphan can never mutate the sink after the pipeline is
  gone. Graceful completion exits `:normal` (which never propagates over the link), so the
  `{:snapshot_done, lsn}` / `{:snapshot_failed, err}` messages still drive the handoff.

  ## Value-free boundary (Critical Rule 1)
  A Postgrex query/cursor fault — raised OR returned — can embed row/column values in its
  message. Every fault is scrubbed to a value-free `%Replicant.Error{reason:
  :snapshot_failed}` (only a structural module name kept) before the Connection is told.
  On success it sends `{:snapshot_done, consistent_point}`; on any fault
  `{:snapshot_failed, %Error{}}`. It reads on a SEPARATE connection, so it never blocks
  the Connection's keepalive path.

  ## Source-side cost
  The consistent-snapshot read holds a single long-running `REPEATABLE READ` transaction
  open for the whole backfill (a slow sink extends it), which pins `xmin` on the source
  and defers VACUUM there until the snapshot completes.
  """

  alias Replicant.{Change, Error, QueryBuilder, Telemetry}

  # Cursor page size — bounds memory (Postgrex.stream FETCHes `max_rows` at a time).
  @batch 1000

  @type args :: %{
          required(:snapshot_name) => String.t(),
          required(:consistent_point) => Replicant.lsn(),
          required(:connection) => keyword(),
          required(:publication) => [String.t()],
          required(:sink) => module(),
          required(:reply_to) => pid(),
          optional(:mode) => :sink_owned | :lib
        }

  @doc "Spawn + LINK the snapshotter to the caller (the Connection) so it is torn down with the pipeline. Returns the pid."
  @spec start(args()) :: pid()
  def start(args), do: spawn_link(__MODULE__, :run, [args])

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

  defp do_snapshot(
         %{
           connection: conn_opts,
           publication: publication,
           sink: sink,
           snapshot_name: name,
           consistent_point: cp
         } = args
       ) do
    start_mono = System.monotonic_time(:millisecond)
    mode = Map.get(args, :mode, :sink_owned)

    # Route a QueryBuilder validation failure (`{:error, reason}`) through the value-free
    # `snapshot_error/1` (never a raw MatchError that could leak the reason), before any
    # connection is opened.
    with {:ok, set_snapshot_sql} <- QueryBuilder.set_transaction_snapshot(name),
         {:ok, pub_tables_sql} <- QueryBuilder.publication_tables(publication) do
      run_snapshot_txn(
        conn_opts,
        sink,
        cp,
        set_snapshot_sql,
        pub_tables_sql,
        publication,
        start_mono,
        mode
      )
    else
      {:error, reason} -> {:error, snapshot_error(reason)}
    end
  end

  defp run_snapshot_txn(
         conn_opts,
         sink,
         cp,
         set_snapshot_sql,
         pub_tables_sql,
         publication,
         start_mono,
         mode
       ) do
    {:ok, db} = Postgrex.start_link(conn_opts ++ [pool_size: 1])

    try do
      result =
        Postgrex.transaction(
          db,
          fn c ->
            Postgrex.query!(c, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", [])
            Postgrex.query!(c, set_snapshot_sql, [])
            tables = Postgrex.query!(c, pub_tables_sql, [publication]).rows

            Enum.reduce(tables, 0, fn [schema, table, qualified], acc ->
              acc + copy_table(c, sink, cp, schema, table, qualified)
            end)
          end,
          timeout: :infinity
        )

      case result do
        {:ok, total} -> complete(sink, cp, total, start_mono, mode)
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
    qualified_display = "#{schema}.#{table}"

    {dispatched?, count} =
      Enum.reduce(stream, {false, 0}, fn %Postgrex.Result{columns: cols, rows: rows},
                                         {dispatched?, count} ->
        if rows == [] do
          {dispatched?, count}
        else
          changes = Enum.map(rows, &build_change(schema, table, cols, &1))
          dispatch_batch!(c, sink, changes, cp, qualified_display, not dispatched?)
          {true, count + length(rows)}
        end
      end)

    unless dispatched?, do: dispatch_batch!(c, sink, [], cp, qualified_display, true)

    Telemetry.event([:replicant, :snapshot, :table_completed], %{}, %{
      table: qualified_display,
      change_count: count
    })

    count
  end

  # Deliver one batch. A sink {:error, _} rolls the read transaction back with a
  # value-free token → the transaction returns {:error, :sink_snapshot_error}, scrubbed
  # by do_snapshot. A sink RAISE propagates out and is scrubbed by snapshot/1's rescue.
  defp dispatch_batch!(c, sink, changes, cp, qualified_display, first?) do
    ctx = %{snapshot_lsn: cp, table: qualified_display, first_for_table?: first?}

    case sink.handle_snapshot(changes, ctx) do
      :ok -> :ok
      {:error, _reason} -> Postgrex.rollback(c, :sink_snapshot_error)
    end
  end

  # The handoff commit. Ack the KNOWN consistent point (never a value the sink returns —
  # spec §14.20 discipline), scrubbing any fault. `total` is the snapshot's total row
  # count; `start_mono` seeds the duration measurement.
  #
  # Sink-owned: the sink's handle_snapshot_complete/1 is the durable handoff. Lib mode:
  # the sink does NOT own the checkpoint — return {:ok, cp} and let the Connection write
  # the store handoff on {:snapshot_done, cp} (ordered before streaming; fault → halt →
  # whole re-run, since the store checkpoint stays nil until then).
  defp complete(sink, cp, total, start_mono, :sink_owned) do
    case sink.handle_snapshot_complete(cp) do
      {:ok, lsn} when is_integer(lsn) -> complete_ok(cp, total, start_mono)
      _other -> {:error, %Error{reason: :snapshot_failed}}
    end
  rescue
    e -> {:error, snapshot_error(e)}
  catch
    _kind, _reason -> {:error, %Error{reason: :snapshot_failed}}
  end

  defp complete(_sink, cp, total, start_mono, :lib), do: complete_ok(cp, total, start_mono)

  defp complete_ok(cp, total, start_mono) do
    duration = System.monotonic_time(:millisecond) - start_mono

    Telemetry.event([:replicant, :snapshot, :completed], %{duration: duration}, %{
      commit_lsn: cp,
      change_count: total
    })

    {:ok, cp}
  end

  @doc false
  @spec complete_for_test(module(), Replicant.lsn(), :sink_owned | :lib) ::
          {:ok, Replicant.lsn()} | {:error, term()}
  def complete_for_test(sink, cp, mode),
    do: complete(sink, cp, 0, System.monotonic_time(:millisecond), mode)

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

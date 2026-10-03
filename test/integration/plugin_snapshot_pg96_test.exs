defmodule Replicant.PluginSnapshotPg96Test do
  @moduledoc """
  A plugin-decoder snapshot on a real PostgreSQL 9.6: `decoder: :wal2json` with
  `snapshot: true` back-fills the table under the snapshot the slot exported, then
  streams strictly after the slot's consistent point, so every row arrives exactly
  once across the two (ADR-0009 §8's pre-15 snapshot path).

  9.6 exports a two-part snapshot name (`%08X-%d`, e.g. `00004E57-1`), unlike the
  three-part name of 10 and later; the back-fill must adopt it.

  A writer inserts continuously from before the slot exists until after the snapshot
  completes, so commits land between the slot's export and the back-fill's read.
  Adopting the exported snapshot is what makes those rows arrive exactly once; a
  back-fill under any later snapshot would also read rows the stream delivers.

  Target server, as the pgoutput-on-9.6 refusal leg resolves it: the PG96 secondary
  (`REPLICANT_PG96_URL`) when set, else the primary itself when it is pre-10. A row
  with neither is skipped by ExUnit (reported as skipped, never counted as a pass).
  """

  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag timeout: 120_000

  alias Replicant.Test.{PG16, SnapshotRecordingSink}

  import Replicant.Test.DecoderParity, only: [conn_opts: 1]

  @table "snap96"

  @target (cond do
             System.get_env("REPLICANT_PG96_URL", "") != "" ->
               System.fetch_env!("REPLICANT_PG96_URL")

             Replicant.TestHelper.server_version_num() in 1..99_999 ->
               System.fetch_env!("REPLICANT_TEST_URL")

             true ->
               nil
           end)

  if is_nil(@target), do: @moduletag(skip: "no pre-10 server on this row")

  test "a wal2json snapshot on 9.6 back-fills under the two-part exported name, then streams exactly after it" do
    snapshot_then_stream(@target)
  end

  defp snapshot_then_stream(url) do
    {:ok, _} = SnapshotRecordingSink.start_link()
    SnapshotRecordingSink.reset()

    {:ok, ctrl} = Postgrex.start_link(conn_opts(url) ++ [pool_size: 2, backoff_type: :stop])

    [[version]] =
      Postgrex.query!(ctrl, "SELECT current_setting('server_version_num')::int", []).rows

    assert version < 100_000, "the 9.6 leg resolved a #{version} server"

    slot = "rep_snap96_#{System.unique_integer([:positive])}"
    drop_slot(ctrl, slot)
    Postgrex.query!(ctrl, "DROP TABLE IF EXISTS #{@table}", [])
    Postgrex.query!(ctrl, "CREATE TABLE #{@table} (id int PRIMARY KEY, note text)", [])

    Postgrex.query!(
      ctrl,
      "INSERT INTO #{@table} SELECT g, 'before-' || g FROM generate_series(1, 50) g",
      []
    )

    writer = start_writer(url)

    on_exit(fn ->
      Replicant.stop(slot)
      {:ok, c} = Postgrex.start_link(conn_opts(url))
      drop_slot(c, slot)
      Postgrex.query!(c, "DROP TABLE IF EXISTS #{@table}", [])
      GenServer.stop(c)
    end)

    {:ok, _} =
      Replicant.start_link(
        connection: conn_opts(url),
        slot_name: slot,
        sink: SnapshotRecordingSink,
        decoder: :wal2json,
        tables: [{"public", @table}],
        snapshot: true
      )

    PG16.wait_until(fn -> SnapshotRecordingSink.recorded().complete? end, 600)

    # Keep writing past the handoff, then stop the writer and commit one last row.
    Process.sleep(300)
    send(writer, :stop)
    written_through = receive do: ({:written_through, n} -> n)
    last = written_through + 1
    Postgrex.query!(ctrl, "INSERT INTO #{@table} VALUES ($1, 'last')", [last])

    PG16.wait_until(fn -> last in streamed_ids(SnapshotRecordingSink.recorded()) end, 600)

    # Read the record only after the pipeline stops, so every delivery is in it.
    Replicant.stop(slot)
    PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 200)
    recorded = SnapshotRecordingSink.recorded()

    table_ids = Postgrex.query!(ctrl, "SELECT id FROM #{@table} ORDER BY id", []).rows
    GenServer.stop(ctrl)

    snapshot_ids = Enum.map(recorded.snapshot, & &1.record["id"])
    streamed = streamed_ids(recorded)
    delivered = snapshot_ids ++ streamed

    # Every row in the table arrives exactly once across snapshot and stream.
    assert Enum.sort(delivered) == List.flatten(table_ids)
    assert length(delivered) == length(Enum.uniq(delivered))

    # The writer's commits straddled the handoff: some rows rode the snapshot,
    # some the stream, so the window between export and read was exercised.
    assert Enum.any?(snapshot_ids, &(&1 > 50))
    assert Enum.any?(streamed, &(&1 > 50 and &1 < last))

    assert is_integer(recorded.watermark)
    assert Enum.all?(recorded.txns, &(&1.commit_lsn > recorded.watermark))
  end

  # Inserts ids 51, 52, ... one transaction each until told to stop, then reports
  # the last id it committed.
  defp start_writer(url) do
    parent = self()

    spawn_link(fn ->
      {:ok, c} = Postgrex.start_link(conn_opts(url) ++ [backoff_type: :stop])
      write_loop(c, parent, 51)
    end)
  end

  defp write_loop(c, parent, id) do
    receive do
      :stop ->
        send(parent, {:written_through, id - 1})
        GenServer.stop(c)
    after
      0 ->
        Postgrex.query!(c, "INSERT INTO #{@table} VALUES ($1, 'during')", [id])
        write_loop(c, parent, id + 1)
    end
  end

  defp streamed_ids(recorded) do
    for txn <- recorded.txns, change <- txn.changes, change.table == @table do
      change.record["id"]
    end
  end

  defp drop_slot(ctrl, slot) do
    Postgrex.query!(
      ctrl,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  end
end

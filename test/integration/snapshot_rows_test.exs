defmodule Replicant.SnapshotRowsTest do
  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag timeout: 30_000
  if Replicant.TestHelper.server_version_num() < 150_000,
    do: @moduletag(skip: "publication row filters require PostgreSQL 15+")

  alias Replicant.Test.{PG16, SnapshotRecordingSink}

  setup do
    start_supervised!(SnapshotRecordingSink)
    {:ok, db} = Postgrex.start_link(PG16.pg_opts())
    table = "rep_rows_#{System.unique_integer([:positive])}"
    pubs = ["#{table}_a", "#{table}_b"]
    slot = "#{table}_slot"
    cp = "#{table}_cp"
    Postgrex.query!(db, "CREATE TABLE #{table} (id int PRIMARY KEY, note text)", [])

    Postgrex.query!(
      db,
      "INSERT INTO #{table} SELECT i, 'snapshot' FROM generate_series(1,5) i",
      []
    )

    handler = {__MODULE__, slot}

    :telemetry.attach(
      handler,
      [:replicant, :snapshot, :completed],
      &__MODULE__.completed/4,
      self()
    )

    on_exit(fn ->
      Replicant.stop(slot)
      :telemetry.detach(handler)
      {:ok, cleanup} = Postgrex.start_link(PG16.pg_opts())

      PG16.wait_until(fn ->
        Postgrex.query!(cleanup, "SELECT active FROM pg_replication_slots WHERE slot_name = $1", [
          slot
        ]).rows in [[], [[false]]]
      end)

      Postgrex.query!(
        cleanup,
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
        [slot]
      )

      for pub <- pubs, do: Postgrex.query!(cleanup, "DROP PUBLICATION IF EXISTS #{pub}", [])

      for t <- [table, cp, "#{cp}_progress"],
          do: Postgrex.query!(cleanup, "DROP TABLE IF EXISTS #{t}", [])

      GenServer.stop(cleanup)
    end)

    %{db: db, table: table, pubs: pubs, slot: slot, cp: cp}
  end

  for snapshot <- [true, [mode: :incremental, chunk_rows: 1]],
      checkpoint <- [:sink_owned, :lib],
      {label, filters, before_ids, after_ids} <- [
        {"one filter", ["id % 2 = 0"], [2, 4], [6, 8, 10]},
        {"unfiltered overrides", ["id % 2 = 0", nil], [1, 2, 3, 4, 5], [6, 7, 8, 9, 10]},
        {"OR filters", ["id % 2 = 0", "id % 3 = 0"], [2, 3, 4], [6, 8, 9, 10]}
      ] do
    @snapshot snapshot
    @checkpoint checkpoint
    @filters filters
    @before_ids before_ids
    @after_ids after_ids
    test "#{inspect(snapshot)} #{checkpoint}: #{label} matches snapshot and stream rows", ctx do
      for {pub, filter} <- Enum.zip(ctx.pubs, @filters) do
        predicate = if filter, do: " WHERE (#{filter})", else: ""

        Postgrex.query!(
          ctx.db,
          "CREATE PUBLICATION #{pub} FOR TABLE #{ctx.table}#{predicate} WITH (publish = 'insert')",
          []
        )
      end

      store =
        if @checkpoint == :lib,
          do: [
            checkpoint_store: [
              connection: PG16.pg_opts(),
              table: ctx.cp,
              progress_table: "#{ctx.cp}_progress"
            ]
          ],
          else: []

      assert {:ok, _} =
               Replicant.start_link(
                 [
                   connection: PG16.pg_opts(),
                   slot_name: ctx.slot,
                   publication: Enum.take(ctx.pubs, length(@filters)),
                   sink: SnapshotRecordingSink,
                   snapshot: @snapshot
                 ] ++ store
               )

      assert_receive :snapshot_completed, 10_000

      assert Enum.sort(Enum.map(SnapshotRecordingSink.recorded().snapshot, & &1.record["id"])) ==
               @before_ids

      Postgrex.query!(
        ctx.db,
        "INSERT INTO #{ctx.table} SELECT i, 'stream' FROM generate_series(6,10) i",
        []
      )

      PG16.wait_until(fn -> SnapshotRecordingSink.recorded().txns != [] end)

      ids =
        for txn <- SnapshotRecordingSink.recorded().txns,
            change <- txn.changes,
            do: change.record["id"]

      assert Enum.sort(ids) == @after_ids
    end
  end

  for mode <- [true, [mode: :incremental, chunk_rows: 1]] do
    @mode mode
    test "#{inspect(mode)} UPDATE/DELETE-only publication does not override INSERT filter", ctx do
      [a, b] = ctx.pubs

      Postgrex.query!(
        ctx.db,
        "CREATE PUBLICATION #{a} FOR TABLE #{ctx.table} WITH (publish = 'update,delete')",
        []
      )

      Postgrex.query!(
        ctx.db,
        "CREATE PUBLICATION #{b} FOR TABLE #{ctx.table} WHERE (id % 2 = 0) WITH (publish = 'insert')",
        []
      )

      assert {:ok, _} =
               Replicant.start_link(
                 connection: PG16.pg_opts(),
                 slot_name: ctx.slot,
                 publication: ctx.pubs,
                 sink: SnapshotRecordingSink,
                 snapshot: @mode
               )

      assert_receive :snapshot_completed, 10_000

      assert Enum.sort(Enum.map(SnapshotRecordingSink.recorded().snapshot, & &1.record["id"])) ==
               [2, 4]

      Postgrex.query!(
        ctx.db,
        "INSERT INTO #{ctx.table} SELECT i, 'stream' FROM generate_series(6,10) i",
        []
      )

      PG16.wait_until(fn -> SnapshotRecordingSink.recorded().txns != [] end)

      ids =
        for txn <- SnapshotRecordingSink.recorded().txns,
            change <- txn.changes,
            do: change.record["id"]

      assert Enum.sort(ids) == [6, 8, 10]
    end
  end

  test "incremental whole-table window filters rows when the published projection omits the PK",
       ctx do
    [pub | _] = ctx.pubs

    Postgrex.query!(
      ctx.db,
      "CREATE PUBLICATION #{pub} FOR TABLE #{ctx.table} (note) WHERE (id % 2 = 0) WITH (publish = 'insert')",
      []
    )

    assert {:ok, _} =
             Replicant.start_link(
               connection: PG16.pg_opts(),
               slot_name: ctx.slot,
               publication: pub,
               sink: SnapshotRecordingSink,
               snapshot: [mode: :incremental, chunk_rows: 1]
             )

    assert_receive :snapshot_completed, 10_000
    assert length(SnapshotRecordingSink.recorded().snapshot) == 2

    assert Enum.all?(
             SnapshotRecordingSink.recorded().snapshot,
             &(Map.keys(&1.record) == ["note"])
           )
  end

  def completed(_, _, _, pid), do: send(pid, :snapshot_completed)
end

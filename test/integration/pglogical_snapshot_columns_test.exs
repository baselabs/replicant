defmodule Replicant.PglogicalSnapshotColumnsTest do
  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag timeout: 30_000
  unless Replicant.TestHelper.primary_carries_plugins?(),
    do: @moduletag(skip: "pglogical is unavailable on the configured server")

  alias Replicant.Snapshotter
  alias Replicant.Test.{PG16, SnapshotRecordingSink}

  setup do
    start_supervised!(SnapshotRecordingSink)
    {:ok, db} = Postgrex.start_link(PG16.pg_opts())
    suffix = System.unique_integer([:positive])
    table = "rep_pgcols_#{suffix}"
    sets = ["#{table}_a", "#{table}_b"]
    slot = "#{table}_slot"

    Postgrex.query!(
      db,
      "CREATE TABLE #{table} (id int PRIMARY KEY, private_secret text, visible text)",
      []
    )

    Postgrex.query!(db, "INSERT INTO #{table} VALUES (1, 'test-only-secret', 'before')", [])

    for set <- sets do
      Postgrex.query!(db, "SELECT pglogical.create_replication_set($1)", [set])
    end

    on_exit(fn ->
      Replicant.stop(slot)
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

      for set <- sets,
          do: Postgrex.query!(cleanup, "SELECT pglogical.drop_replication_set($1)", [set])

      Postgrex.query!(cleanup, "DROP TABLE #{table}", [])
      GenServer.stop(cleanup)
    end)

    %{db: db, table: table, sets: sets, slot: slot}
  end

  for mode <- [true, [mode: :incremental, chunk_rows: 1]] do
    @mode mode
    test "#{inspect(mode)} snapshot and INSERT stream both honor set_att_list", ctx do
      for set <- ctx.sets, do: add_table(ctx, set, ["visible", "id"])
      assert [[_, _, _, ["id", "visible"], _, _]] = columns(ctx)

      assert {:ok, _} =
               Replicant.start_link(
                 connection: PG16.pg_opts(),
                 slot_name: ctx.slot,
                 decoder: :pglogical,
                 replication_sets: ctx.sets,
                 sink: SnapshotRecordingSink,
                 snapshot: @mode
               )

      PG16.wait_until(fn -> SnapshotRecordingSink.recorded().complete? end)
      [snapshot] = SnapshotRecordingSink.recorded().snapshot
      assert Enum.sort(Map.keys(snapshot.record)) == ["id", "visible"]

      Postgrex.query!(
        ctx.db,
        "INSERT INTO #{ctx.table} VALUES (2, 'test-only-secret', 'after')",
        []
      )

      PG16.wait_until(fn -> SnapshotRecordingSink.recorded().txns != [] end)
      changes = for txn <- SnapshotRecordingSink.recorded().txns, c <- txn.changes, do: c
      assert [change] = changes
      assert Map.keys(change.record) == Map.keys(snapshot.record)
    end
  end

  test "different set lists are refused instead of combined", ctx do
    [a, b] = ctx.sets
    add_table(ctx, a, ["id", "visible"])
    add_table(ctx, b, ["id", "private_secret"])
    assert_raise Replicant.Error, ~r/snapshot_column_list_conflict/, fn -> columns(ctx) end
  end

  test "unfiltered sets preserve every column", ctx do
    for set <- ctx.sets, do: add_table(ctx, set, nil)
    assert [[_, _, _, ["id", "private_secret", "visible"], _, _]] = columns(ctx)
  end

  defp add_table(ctx, set, names) do
    Postgrex.query!(
      ctx.db,
      "SELECT pglogical.replication_set_add_table($1, $2::regclass, false, $3::text[])",
      [set, ctx.table, names]
    )
  end

  defp columns(ctx) do
    Snapshotter.table_columns(ctx.db, %{
      tables: [{"public", ctx.table}],
      replication_sets: ctx.sets
    })
  end
end

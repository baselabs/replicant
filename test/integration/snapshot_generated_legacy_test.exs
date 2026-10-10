defmodule Replicant.SnapshotGeneratedLegacyTest do
  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag timeout: 30_000
  unless Replicant.TestHelper.server_version_num() in 120_000..159_999,
    do: @moduletag(skip: "legacy generated-column regression requires PG12-15")

  alias Replicant.Test.{PG16, SnapshotRecordingSink}

  setup do
    start_supervised!(SnapshotRecordingSink)
    {:ok, db} = Postgrex.start_link(PG16.pg_opts())
    table = "rep_generated_#{System.unique_integer([:positive])}"
    pub = "#{table}_pub"
    slot = "#{table}_slot"

    Postgrex.query!(
      db,
      "CREATE TABLE #{table} (id int PRIMARY KEY, derived int GENERATED ALWAYS AS (id * 2) STORED)",
      []
    )

    Postgrex.query!(db, "INSERT INTO #{table} (id) VALUES (1)", [])
    Postgrex.query!(db, "CREATE PUBLICATION #{pub} FOR TABLE #{table}", [])

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

      Postgrex.query!(cleanup, "DROP PUBLICATION #{pub}", [])
      Postgrex.query!(cleanup, "DROP TABLE #{table}", [])
      GenServer.stop(cleanup)
    end)

    %{db: db, table: table, pub: pub, slot: slot}
  end

  for mode <- [true, [mode: :incremental, chunk_rows: 1]] do
    @mode mode
    test "#{inspect(mode)} PG12-15 snapshot omits generated columns exactly as INSERT stream",
         ctx do
      assert {:ok, _} =
               Replicant.start_link(
                 connection: PG16.pg_opts(),
                 slot_name: ctx.slot,
                 publication: ctx.pub,
                 sink: SnapshotRecordingSink,
                 snapshot: @mode
               )

      PG16.wait_until(fn -> SnapshotRecordingSink.recorded().complete? end)
      assert [%{record: %{"id" => 1} = record}] = SnapshotRecordingSink.recorded().snapshot
      assert Map.keys(record) == ["id"]
      Postgrex.query!(ctx.db, "INSERT INTO #{ctx.table} (id) VALUES (2)", [])
      PG16.wait_until(fn -> SnapshotRecordingSink.recorded().txns != [] end)
      assert [%{changes: [%{record: stream}]}] = SnapshotRecordingSink.recorded().txns
      assert stream == %{"id" => 2}
    end
  end
end

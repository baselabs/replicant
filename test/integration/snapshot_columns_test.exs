defmodule Replicant.SnapshotColumnsTest do
  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag timeout: 30_000
  if Replicant.TestHelper.server_version_num() < 150_000,
    do: @moduletag(skip: "publication column lists require PostgreSQL 15+")

  alias Replicant.{QueryBuilder, Snapshotter}
  alias Replicant.Snapshotter.Incremental
  alias Replicant.Test.{PG16, SnapshotRecordingSink}

  setup do
    start_supervised!(SnapshotRecordingSink)
    {:ok, db} = Postgrex.start_link(PG16.pg_opts())
    suffix = System.unique_integer([:positive])
    table = "rep_columns_#{suffix}"
    pubs = ["#{table}_a", "#{table}_b"]
    slot = "#{table}_slot"

    Postgrex.query!(
      db,
      "CREATE TABLE #{table} (id int PRIMARY KEY, private_secret text, visible text)",
      []
    )

    Postgrex.query!(db, "INSERT INTO #{table} VALUES (1, 'test-only-secret', 'before')", [])

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

      for pub <- pubs, do: Postgrex.query!(cleanup, "DROP PUBLICATION IF EXISTS #{pub}", [])
      Postgrex.query!(cleanup, "DROP TABLE #{table}", [])
      GenServer.stop(cleanup)
    end)

    %{db: db, table: table, pubs: pubs, slot: slot}
  end

  for mode <- [true, [mode: :incremental, chunk_rows: 1]] do
    @mode mode
    test "#{inspect(mode)} snapshot excludes secret and INSERT stream has exactly the same columns",
         ctx do
      publish(ctx, hd(ctx.pubs), "(visible, id)")
      start_pipeline(ctx, [hd(ctx.pubs)], @mode)
      PG16.wait_until(fn -> SnapshotRecordingSink.recorded().complete? end)
      [snapshot] = SnapshotRecordingSink.recorded().snapshot
      assert Enum.sort(Map.keys(snapshot.record)) == ["id", "visible"]

      Postgrex.query!(
        ctx.db,
        "INSERT INTO #{ctx.table} VALUES (2, 'test-only-secret', 'after')",
        []
      )

      PG16.wait_until(fn -> streamed_changes() != [] end)
      [change] = streamed_changes()
      assert Map.keys(change.record) == Map.keys(snapshot.record)
    end
  end

  test "column metadata keeps attnum order even when publication lists visible before id", ctx do
    publish(ctx, hd(ctx.pubs), "(visible, id)")
    [[_, _, _, raw, quoted, types, _]] = columns(ctx, [hd(ctx.pubs)])
    assert raw == ["id", "visible"]
    assert quoted == ["id", "visible"]
    assert types == [23, 25]
  end

  test "no column list keeps all columns", ctx do
    publish(ctx, hd(ctx.pubs), "")
    [[_, _, _, raw, _, _, _]] = columns(ctx, [hd(ctx.pubs)])
    assert raw == ["id", "private_secret", "visible"]
    start_pipeline(ctx, [hd(ctx.pubs)], true)
    PG16.wait_until(fn -> SnapshotRecordingSink.recorded().complete? end)
    [snapshot] = SnapshotRecordingSink.recorded().snapshot
    assert Enum.sort(Map.keys(snapshot.record)) == Enum.sort(raw)
  end

  test "two publications with identical lists deliver one filtered snapshot", ctx do
    for pub <- ctx.pubs, do: publish(ctx, pub, "(id, visible)")
    start_pipeline(ctx, ctx.pubs, true)
    PG16.wait_until(fn -> SnapshotRecordingSink.recorded().complete? end)
    [snapshot] = SnapshotRecordingSink.recorded().snapshot
    assert Enum.sort(Map.keys(snapshot.record)) == ["id", "visible"]
  end

  test "implicit and explicit full lists agree with the pgoutput stream", ctx do
    [a, b] = ctx.pubs
    publish(ctx, a, "")
    publish(ctx, b, "(visible, private_secret, id)")
    start_pipeline(ctx, ctx.pubs, true)
    PG16.wait_until(fn -> SnapshotRecordingSink.recorded().complete? end)
    [snapshot] = SnapshotRecordingSink.recorded().snapshot
    assert Enum.sort(Map.keys(snapshot.record)) == ["id", "private_secret", "visible"]

    Postgrex.query!(
      ctx.db,
      "INSERT INTO #{ctx.table} VALUES (2, 'test-only-secret', 'after')",
      []
    )

    PG16.wait_until(fn -> streamed_changes() != [] end)
    [change] = streamed_changes()
    assert Map.keys(change.record) == Map.keys(snapshot.record)
  end

  for second_list <- ["(id, private_secret)", ""] do
    @second_list second_list
    test "different lists (second #{inspect(second_list)}) match pgoutput refusal and deliver nothing",
         ctx do
      [a, b] = ctx.pubs
      publish(ctx, a, "(id, visible)")
      publish(ctx, b, @second_list)

      Postgrex.query!(ctx.db, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", [
        ctx.slot
      ])

      Postgrex.query!(
        ctx.db,
        "INSERT INTO #{ctx.table} VALUES (2, 'test-only-secret', 'after')",
        []
      )

      assert {:error, %Postgrex.Error{postgres: postgres}} =
               Postgrex.query(
                 ctx.db,
                 "SELECT count(*) FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)",
                 [ctx.slot, Enum.join(ctx.pubs, ",")]
               )

      assert postgres.message ==
               "cannot use different column lists for table \"public.#{ctx.table}\" in different publications"

      assert_raise Replicant.Error, ~r/snapshot_column_list_conflict/, fn ->
        columns(ctx, ctx.pubs)
      end

      Postgrex.query!(ctx.db, "SELECT pg_drop_replication_slot($1)", [ctx.slot])
      parent = self()
      handler = {__MODULE__, ctx.slot}

      :telemetry.attach(
        handler,
        [:replicant, :snapshot, :failed],
        fn _, _, meta, pid ->
          send(pid, {:snapshot_failed, meta.reason})
        end,
        parent
      )

      on_exit(fn -> :telemetry.detach(handler) end)
      start_pipeline(ctx, ctx.pubs, true)
      assert_receive {:snapshot_failed, :snapshot_column_list_conflict}, 10_000
      assert SnapshotRecordingSink.recorded().snapshot == []
      refute SnapshotRecordingSink.recorded().complete?
      Replicant.stop(ctx.slot)
    end
  end

  test "incremental snapshot refuses conflicting lists before any snapshot callback", ctx do
    [a, b] = ctx.pubs
    publish(ctx, a, "(id, visible)")
    publish(ctx, b, "(id, private_secret)")
    handler = {__MODULE__, ctx.slot}

    :telemetry.attach(
      handler,
      [:replicant, :snapshot, :failed],
      fn _, _, meta, pid ->
        send(pid, {:snapshot_failed, meta.reason})
      end,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    start_pipeline(ctx, ctx.pubs, mode: :incremental, chunk_rows: 1)
    assert_receive {:snapshot_failed, :snapshot_column_list_conflict}, 10_000
    assert SnapshotRecordingSink.recorded().snapshot == []
    refute SnapshotRecordingSink.recorded().complete?
    Replicant.stop(ctx.slot)
  end

  test "incremental snapshot uses a whole-table window when the primary key is unpublished",
       ctx do
    publish(ctx, hd(ctx.pubs), "(visible)")
    start_pipeline(ctx, [hd(ctx.pubs)], mode: :incremental, chunk_rows: 1)
    PG16.wait_until(fn -> SnapshotRecordingSink.recorded().complete? end)
    [snapshot] = SnapshotRecordingSink.recorded().snapshot
    assert Map.keys(snapshot.record) == ["visible"]
  end

  test "unpublished primary key is never added to the snapshot projection", ctx do
    publish(ctx, hd(ctx.pubs), "(visible)")
    start_pipeline(ctx, [hd(ctx.pubs)], true)
    PG16.wait_until(fn -> SnapshotRecordingSink.recorded().complete? end)
    [snapshot] = SnapshotRecordingSink.recorded().snapshot
    assert Map.keys(snapshot.record) == ["visible"]

    assert {:error, %Postgrex.Error{postgres: %{code: :invalid_column_reference}}} =
             Postgrex.query(ctx.db, "UPDATE #{ctx.table} SET visible = 'changed'", [])

    assert {:error, %Postgrex.Error{postgres: %{code: :invalid_column_reference}}} =
             Postgrex.query(ctx.db, "DELETE FROM #{ctx.table}", [])

    pk_rows = Postgrex.query!(ctx.db, QueryBuilder.pk_columns(), [[hd(ctx.pubs)]]).rows
    col_rows = columns(ctx, [hd(ctx.pubs)])

    refs =
      Incremental.parse_pk_rows(pk_rows, col_rows, [
        ["public", ctx.table, "public.#{ctx.table}"]
      ])

    assert [%{pk_raw: [], pk_quoted: [], col_quoted: ["visible"]}] = refs
  end

  test "server attnames is non-NULL and attnum ordered even for a no-list publication", ctx do
    [a, b] = ctx.pubs
    publish(ctx, a, "")
    publish(ctx, b, "(visible, private_secret, id)")

    assert [[a, ["id", "private_secret", "visible"]], [b, ["id", "private_secret", "visible"]]] ==
             Postgrex.query!(
               ctx.db,
               "SELECT pubname, attnames FROM pg_publication_tables WHERE pubname = ANY($1) ORDER BY pubname",
               [ctx.pubs]
             ).rows
  end

  for stored <- [false, true] do
    @stored stored
    @tag :snapshot_generated
    @tag skip: Replicant.TestHelper.server_version_num() < if(stored, do: 180_000, else: 160_000)
    test "generated stored column snapshot matches stream (publish stored: #{stored})", ctx do
      Postgrex.query!(
        ctx.db,
        "ALTER TABLE #{ctx.table} ADD COLUMN derived int GENERATED ALWAYS AS (id * 2) STORED",
        []
      )

      pub = hd(ctx.pubs)
      option = if @stored, do: " WITH (publish_generated_columns = stored)", else: ""
      publish(ctx, pub, option)
      start_pipeline(ctx, [pub], true)
      PG16.wait_until(fn -> SnapshotRecordingSink.recorded().complete? end)
      [snapshot] = SnapshotRecordingSink.recorded().snapshot
      assert Map.has_key?(snapshot.record, "derived") == @stored

      Postgrex.query!(
        ctx.db,
        "INSERT INTO #{ctx.table} (id, private_secret, visible) VALUES (2, 'test-only-secret', 'after')",
        []
      )

      PG16.wait_until(fn -> streamed_changes() != [] end)
      [stream] = streamed_changes()
      assert Map.keys(snapshot.record) == Map.keys(stream.record)
    end
  end

  test "a table with no publishable columns is refused by name", ctx do
    Postgrex.query!(
      ctx.db,
      "ALTER TABLE #{ctx.table} DROP COLUMN id, DROP COLUMN private_secret, DROP COLUMN visible",
      []
    )

    publish(ctx, hd(ctx.pubs), "")
    error = assert_raise Replicant.Error, fn -> columns(ctx, [hd(ctx.pubs)]) end
    assert error.reason == :snapshot_no_publishable_columns
    assert error.table == "public.#{ctx.table}"
  end

  for mode <- [true, [mode: :incremental, chunk_rows: 1]] do
    @mode mode
    @tag :empty_projection
    test "#{inspect(mode)} empty-projection halt exposes the refused table", ctx do
      Postgrex.query!(
        ctx.db,
        "ALTER TABLE #{ctx.table} DROP COLUMN id, DROP COLUMN private_secret, DROP COLUMN visible",
        []
      )

      publish(ctx, hd(ctx.pubs), "")
      handler = {__MODULE__, ctx.slot}

      :telemetry.attach(
        handler,
        [:replicant, :snapshot, :failed],
        &__MODULE__.empty_refusal/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)
      start_pipeline(ctx, [hd(ctx.pubs)], @mode)
      table = "public.#{ctx.table}"
      assert_receive {:empty_refusal, :snapshot_no_publishable_columns, ^table}, 1_000
      assert SnapshotRecordingSink.recorded().snapshot == []
    end
  end

  def empty_refusal(_, _, meta, pid),
    do: send(pid, {:empty_refusal, meta.reason, Map.get(meta, :table)})

  defp publish(ctx, pub, list) do
    Postgrex.query!(ctx.db, "CREATE PUBLICATION #{pub} FOR TABLE #{ctx.table} #{list}", [])
  end

  defp columns(ctx, pubs),
    do:
      Snapshotter.table_columns(ctx.db, %{
        publication: pubs,
        server_version_num: Replicant.TestHelper.server_version_num()
      })

  defp start_pipeline(ctx, pubs, mode) do
    assert {:ok, _} =
             Replicant.start_link(
               connection: PG16.pg_opts(),
               slot_name: ctx.slot,
               publication: pubs,
               sink: SnapshotRecordingSink,
               snapshot: mode
             )
  end

  defp streamed_changes do
    for txn <- SnapshotRecordingSink.recorded().txns, change <- txn.changes, do: change
  end
end

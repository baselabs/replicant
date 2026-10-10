defmodule Replicant.SnapshotColumnsUnitTest do
  use ExUnit.Case, async: true
  alias Replicant.{QueryBuilder, Snapshotter}
  alias Replicant.Snapshotter.Incremental

  test "only PostgreSQL 15+ queries attnames; 13 and 14 retain the legacy query" do
    for version <- [100_000, 120_000, 130_000, 140_000, 149_999] do
      sql = QueryBuilder.table_columns(version)
      assert sql =~ "SELECT DISTINCT schemaname, tablename FROM pg_publication_tables"
      refute sql =~ "attnames"
    end

    for version <- [150_000, 160_000, 170_000, 180_006] do
      sql = QueryBuilder.table_columns(version)
      assert sql =~ "a.attname = ANY(p.attnames)"
      assert sql =~ "ORDER BY a.attnum"
      assert sql =~ "GROUP BY p.schemaname, p.tablename, p.attnames"
    end
  end

  test "pglogical filters real set_att_list catalog rows scoped to requested local sets" do
    assert {:ok, sql} = QueryBuilder.table_columns_for([{"public", "orders"}], ["alpha", "beta"])
    assert sql =~ "pglogical.replication_set_table"
    assert sql =~ "r.set_att_list"
    assert sql =~ "s.set_name = ANY($1)"
    assert sql =~ "pglogical.local_node"
    assert sql =~ "s.set_nodeid = l.node_id"
    assert sql =~ "a.attname = ANY(r.set_att_list)"
    assert sql =~ "ORDER BY a.attnum"
    assert sql =~ "SELECT DISTINCT"
    assert sql =~ "GROUP BY n.nspname, c.relname, r.set_id"
  end

  test "wal2json keeps all explicit-table columns and has no replication-set bind" do
    assert {:ok, sql} = QueryBuilder.table_columns_for([{"public", "orders"}])
    assert sql =~ "NOT a.attisdropped"
    refute sql =~ "pglogical"
    refute sql =~ "$1"
  end

  test "invalid explicit identifiers raise a typed value-free error before any query" do
    error =
      assert_raise Replicant.Error, fn ->
        Snapshotter.table_columns(:unused, %{tables: [{"public", "bad; secret"}]})
      end

    assert error.reason == :invalid_identifier
    refute Exception.message(error) =~ "secret"
    assert Snapshotter.snapshot_error(error).reason == :invalid_identifier
  end

  test "table_columns requires an explicit probed server version" do
    refute {:table_columns, 0} in QueryBuilder.__info__(:functions)
    assert {:table_columns, 1} in QueryBuilder.__info__(:functions)
  end

  test "snapshot halt reason is observable at the supervisor boundary" do
    handler = {__MODULE__, make_ref()}
    :telemetry.attach(handler, [:replicant, :pipeline, :halted], &__MODULE__.halted/4, self())
    on_exit(fn -> :telemetry.detach(handler) end)

    assert {:disconnect, :snapshot_column_list_conflict} =
             Replicant.Connection.handle_info(
               {:snapshot_failed, %Replicant.Error{reason: :snapshot_column_list_conflict}},
               %Replicant.Connection{step: :snapshotting, slot_name: "columns_offline"}
             )

    assert_receive {:halted,
                    %{
                      slot_name: "columns_offline",
                      kind: :snapshot,
                      reason: :snapshot_column_list_conflict
                    }}
  end

  test "discovery rows refuse a conflicting projection before scanning any table" do
    rows = [
      ["public", "orders", "public.orders", ["id"], ["id"], [23], nil],
      ["public", "orders", "public.orders", ["note"], ["note"], [25], nil]
    ]

    assert_raise Replicant.Error, ~r/snapshot_column_list_conflict/, fn ->
      Snapshotter.validate_columns!(rows)
    end

    assert Snapshotter.validate_columns!([hd(rows)]) == [hd(rows)]
  end

  test "NULL and empty publishable projections refuse the named table" do
    for columns <- [nil, []] do
      error =
        assert_raise Replicant.Error, fn ->
          Snapshotter.validate_columns!([
            ["public", "empty", "public.empty", columns, columns, columns, nil]
          ])
        end

      assert error.reason == :snapshot_no_publishable_columns
      assert error.table == "public.empty"

      assert Snapshotter.snapshot_error(error) == %Replicant.Error{
               reason: :snapshot_no_publishable_columns,
               table: "public.empty"
             }
    end
  end

  test "an omitted primary-key component downgrades to a whole-table window" do
    pk = [["public", "orders", "public.orders", ["id", "part"], ["id", "part"]]]

    cols = [
      [
        "public",
        "orders",
        "public.orders",
        ["part", "note"],
        ["part", "note"],
        [23, 25],
        "(part > 2)"
      ]
    ]

    all = [["public", "orders", "public.orders"]]

    assert [%{pk_raw: [], pk_quoted: [], col_quoted: ["part", "note"], row_filter: "(part > 2)"}] =
             Incremental.parse_pk_rows(pk, cols, all)

    full_pk = [["public", "orders", "public.orders", ["part"], ["part"]]]

    assert [%{pk_raw: ["part"], pk_quoted: ["part"]}] =
             Incremental.parse_pk_rows(full_pk, cols, all)
  end

  test "incremental args preserve selected pglogical sets and the probed version" do
    state = %Replicant.Connection{
      slot_name: "sets_offline",
      server_version_num: 120_000,
      decoder: %{
        kind: :pglogical,
        discovered: [{"public", "orders"}],
        replication_sets: ["alpha", "beta"]
      }
    }

    args = Replicant.Connection.incremental_args(state, nil)
    assert args.replication_sets == ["alpha", "beta"]
    assert args.server_version_num == 120_000
    assert {sql, [["alpha", "beta"]]} = Snapshotter.columns_query(args)
    assert sql =~ "set_row_filter"
    assert sql =~ "pg_get_expr"
    assert sql =~ "' AND '"
    assert args.tables == [{"public", "orders"}]
  end

  test "incremental rescue preserves typed refusal while scrubbing arbitrary failures" do
    for reason <- [
          :snapshot_column_list_conflict,
          :snapshot_no_publishable_columns,
          :invalid_identifier
        ] do
      assert {:error, %Replicant.Error{reason: ^reason, message: nil}} =
               Incremental.with_error_boundary(fn ->
                 raise Replicant.Error, reason: reason, message: "secret"
               end)
    end

    assert {:error, %Replicant.Error{reason: :snapshot_failed, shape: "ArgumentError"}} =
             Incremental.with_error_boundary(fn ->
               raise ArgumentError, "secret"
             end)
  end

  test "known server version selects discovery without another server query" do
    assert {sql, [["pub"]]} =
             Snapshotter.columns_query(%{publication: ["pub"], server_version_num: 140_000})

    refute sql =~ "rowfilter"

    assert {sql, [["pub"]]} =
             Snapshotter.columns_query(%{publication: ["pub"], server_version_num: 180_006})

    assert sql =~ "rowfilter"
    assert sql =~ "bool_or(rowfilter IS NULL)"
    assert sql =~ "' OR '"
  end

  test "server predicates stay parenthesized beside keyset bounds" do
    filter = "id = 2 OR id = 4"
    assert {:ok, first} = QueryBuilder.keyset_chunk("public.orders", ["id"], ["id"], 0, filter)
    assert first =~ "WHERE (id = 2 OR id = 4) ORDER BY"
    assert {:ok, next} = QueryBuilder.keyset_chunk("public.orders", ["id"], ["id"], 1, filter)
    assert next =~ "WHERE (public.orders.id) > ($2) AND (id = 2 OR id = 4) ORDER BY"

    assert QueryBuilder.keyless_scan("public.orders", ["id"], filter) =~
             "WHERE (id = 2 OR id = 4)"
  end

  def halted(_, _, meta, pid), do: send(pid, {:halted, meta})

  test "snapshot conflict errors preserve only their allowlisted reason" do
    error = %Replicant.Error{
      reason: :snapshot_column_list_conflict,
      message: "sensitive",
      table: "sensitive",
      shape: "sensitive"
    }

    assert Snapshotter.snapshot_error(error) == %Replicant.Error{
             reason: :snapshot_column_list_conflict
           }
  end
end

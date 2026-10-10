defmodule Replicant.SnapshotColumnsUnitTest do
  use ExUnit.Case, async: true
  alias Replicant.{QueryBuilder, Snapshotter}

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

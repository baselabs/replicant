defmodule Replicant.QueryBuilderCompatTest do
  # 1.5.0 removed `table_columns/0` and widened `table_columns_for/1` rows, which
  # broke AshReplicant.Coverage (it calls both and matches the six-column 1.4.x
  # row). 1.5.1 restores both forms byte for byte; snapshots keep using the
  # version-aware `table_columns/1` and `table_columns_for/3`.
  use ExUnit.Case, async: true

  alias Replicant.QueryBuilder

  @table_columns_1_4 "SELECT p.schemaname, p.tablename, format('%I.%I', p.schemaname, p.tablename) AS qualified, " <>
                       "array_agg(a.attname ORDER BY a.attnum) AS col_raw, " <>
                       "array_agg(quote_ident(a.attname) ORDER BY a.attnum) AS col_quoted, " <>
                       "array_agg(a.atttypid::int ORDER BY a.attnum) AS col_type_oids " <>
                       "FROM (SELECT DISTINCT schemaname, tablename FROM pg_publication_tables WHERE pubname = ANY($1)) p " <>
                       "JOIN pg_class c ON c.relname = p.tablename " <>
                       "JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = p.schemaname " <>
                       "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped " <>
                       "GROUP BY p.schemaname, p.tablename"

  test "table_columns/0 is the unchanged 1.4.x census query" do
    assert QueryBuilder.table_columns() == @table_columns_1_4
  end

  test "table_columns_for/1 keeps the 1.4.x six-column row shape" do
    assert {:ok, sql} = QueryBuilder.table_columns_for([{"public", "orders"}])

    assert sql ==
             "SELECT n.nspname AS schemaname, c.relname AS tablename, " <>
               "format('%I.%I', n.nspname, c.relname) AS qualified, " <>
               "array_agg(a.attname ORDER BY a.attnum) AS col_raw, " <>
               "array_agg(quote_ident(a.attname) ORDER BY a.attnum) AS col_quoted, " <>
               "array_agg(a.atttypid::int ORDER BY a.attnum) AS col_type_oids " <>
               "FROM (VALUES ('public','orders')) AS p0(nsp, rel) " <>
               "JOIN pg_namespace n ON n.nspname = p0.nsp " <>
               "JOIN pg_class c ON c.relname = p0.rel AND c.relnamespace = n.oid " <>
               "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped " <>
               "GROUP BY n.nspname, c.relname"

    refute sql =~ "row_filter"
    assert {:error, :invalid_identifier} = QueryBuilder.table_columns_for([])
  end

  test "snapshots use the version-aware forms, never the 1.4.x census" do
    refute QueryBuilder.table_columns(180_000) == QueryBuilder.table_columns()
    assert {:ok, sql} = QueryBuilder.table_columns_for([{"public", "orders"}], nil, 180_000)
    assert sql =~ "row_filter"
  end
end

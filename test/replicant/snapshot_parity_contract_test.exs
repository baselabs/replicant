defmodule Replicant.SnapshotParityContractTest do
  use ExUnit.Case, async: true

  alias Replicant.{Error, QueryBuilder, Snapshotter}
  alias Replicant.Snapshotter.Incremental

  test "generated columns are excluded only on PG12-15 in publication discovery" do
    for version <- [100_000, 110_999, 120_000, 140_000, 150_000, 159_999, 160_000, 180_006] do
      sql = QueryBuilder.table_columns(version)
      assert String.contains?(sql, "AND a.attgenerated = ''") == version in 120_000..159_999
    end
  end

  test "INSERT snapshot predicates use only INSERT publications, including legacy versions" do
    for version <- [100_000, 140_000, 150_000, 180_006] do
      sql = QueryBuilder.table_columns(version)
      assert sql =~ "JOIN pg_publication pub ON pub.pubname = p.pubname"
      assert sql =~ "pub.pubinsert"

      if version >= 150_000 do
        [cte, _] = String.split(sql, ") SELECT", parts: 2)
        assert cte =~ "pub.pubinsert"
        assert cte =~ "bool_or(rowfilter IS NULL)"
      end
    end
  end

  test "zero-column discovery is distinct from a filtered-out projection" do
    empty = ["public", "empty", "public.empty", [], [], [], nil]
    assert Snapshotter.validate_columns!([empty]) == [empty]

    error =
      assert_raise Error, fn ->
        Snapshotter.validate_columns!([["public", "hidden", "public.hidden", nil, nil, nil, nil]])
      end

    assert error.reason == :snapshot_no_publishable_columns
    assert error.table == "public.hidden"
  end

  test "discovery keeps tables whose projection is empty and removes NULL aggregate entries" do
    for sql <- [
          QueryBuilder.table_columns(140_000),
          QueryBuilder.table_columns(180_006),
          elem(QueryBuilder.table_columns_for([{"public", "orders"}], ["alpha"], 120_000), 1)
        ] do
      assert sql =~ "LEFT JOIN pg_attribute a"
      assert length(Regex.scan(~r/FILTER \(WHERE a.attnum IS NOT NULL\)/, sql)) == 3
    end
  end

  test "both pglogical catalog paths scope sets to the local node and gate inserts on any set" do
    {:ok, sql} = QueryBuilder.table_columns_for([{"public", "orders"}], ["alpha"], 120_000)

    assert length(Regex.scan(~r/JOIN pglogical.local_node l ON s.set_nodeid = l.node_id/, sql)) ==
             2

    # Inserts flow when any selected set replicates them; the filters of every
    # selected set then apply (pglogical 2.4.8), so the AND is not insert-scoped.
    assert length(Regex.scan(~r/s.replicate_insert/, sql)) == 1
    assert sql =~ "bool_or(s.replicate_insert)"
    refute sql =~ "FILTER (WHERE s.replicate_insert)"
  end

  test "keyless discovery preserves its server-rendered filter" do
    cols = [["public", "orders", "public.orders", ["id"], ["id"], [23], "id % 2 = 0"]]

    assert [%{pk_raw: [], row_filter: "id % 2 = 0"} = table] =
             Incremental.parse_pk_rows([], cols, [["public", "orders", "public.orders"]])

    assert QueryBuilder.keyless_scan(table.qualified, table.col_quoted, table.row_filter) =~
             "WHERE (id % 2 = 0)"
  end

  test "no-publishable-columns telemetry retains the structural table name" do
    error = %Error{reason: :snapshot_no_publishable_columns, table: "public.orders"}
    assert Snapshotter.error_metadata(error) == %{reason: error.reason, table: "public.orders"}
  end

  test "an unprobed version refuses discovery before opening a database connection" do
    for args <- [
          %{publication: ["pub"], server_version_num: 0},
          %{publication: ["pub"]},
          %{tables: [{"public", "orders"}], replication_sets: ["alpha"], server_version_num: 0}
        ] do
      error = assert_raise Error, fn -> Snapshotter.columns_query(args) end
      assert error.reason == :snapshot_failed
    end

    assert_raise Error, fn -> QueryBuilder.table_columns(0) end
  end

  test "pglogical generated-column selection uses the probed version while wal2json stays unchanged" do
    for version <- [90_600, 110_000, 120_000, 150_000, 160_000, 180_006] do
      assert {sql, [["alpha"]]} =
               Snapshotter.columns_query(%{
                 tables: [{"public", "orders"}],
                 replication_sets: ["alpha"],
                 server_version_num: version
               })

      assert String.contains?(sql, "AND a.attgenerated = ''") == version in 120_000..159_999

      assert {json_sql, []} =
               Snapshotter.columns_query(%{
                 tables: [{"public", "orders"}],
                 server_version_num: version
               })

      refute json_sql =~ "attgenerated"
    end
  end

  for {kind, args} <- [
        publication: %{publication: ["bad; secret"]},
        tables: %{tables: [{"public", "bad; secret"}]}
      ] do
    @args args
    test "incremental #{kind} discovery converts identifier failure to a typed error" do
      error = assert_raise Error, fn -> Incremental.discovery_queries(@args) end
      assert error.reason == :invalid_identifier
      refute Exception.message(error) =~ "secret"
    end
  end

  test "incremental halt emits the exact typed reason and structural table" do
    handler = {__MODULE__, make_ref()}

    :telemetry.attach_many(
      handler,
      [[:replicant, :pipeline, :halted], [:replicant, :snapshot, :failed]],
      &__MODULE__.event/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    error = %Error{reason: :snapshot_no_publishable_columns, table: "public.empty"}
    assert :ok = Incremental.halt_snapshot(%{slot_name: "parity_offline_halt"}, error)

    assert_receive {[:replicant, :snapshot, :failed],
                    %{reason: :snapshot_no_publishable_columns, table: "public.empty"}}

    assert_receive {[:replicant, :pipeline, :halted],
                    %{kind: :snapshot, reason: :snapshot_no_publishable_columns}}
  end

  def event(name, _, metadata, receiver), do: send(receiver, {name, metadata})

  # Offline wiring checks complement generated SQL assertions and the live tests.
  # They inspect parsed calls (not comments) because the reader's database boundary
  # cannot be replaced by a canned result or a fake connection.
  test "initial scan passes the discovered row filter to generated SQL" do
    assert call_args("lib/replicant/snapshotter.ex", :copy_table, :keyless_scan) ==
             ["qualified", "col_quoted", "row_filter"]

    assert QueryBuilder.keyless_scan("public.orders", ["id"], "id % 2 = 0") ==
             "SELECT id::text AS id FROM public.orders WHERE (id % 2 = 0)"
  end

  test "keyset chunk passes the discovered row filter to generated SQL" do
    assert List.last(
             call_args("lib/replicant/snapshotter/incremental.ex", :run_chunk, :keyset_chunk)
           ) ==
             "table.row_filter"

    for bound <- [0, 1] do
      assert {:ok, sql} =
               QueryBuilder.keyset_chunk("public.orders", ["id"], ["id"], bound, "id % 2 = 0")

      assert sql =~ "(id % 2 = 0)"
    end
  end

  test "whole-table window passes the discovered row filter to generated SQL" do
    assert call_args(
             "lib/replicant/snapshotter/incremental.ex",
             :run_keyless_table,
             :keyless_scan
           ) ==
             ["table.qualified", "table.col_quoted", "table.row_filter"]
  end

  test "initial reader receives the probed connection version" do
    ast = "lib/replicant/connection.ex" |> File.read!() |> Code.string_to_quoted!()

    {_, starts} =
      Macro.prewalk(ast, [], fn
        {{:., _, [{:__aliases__, _, [:Replicant, :Snapshotter]}, :start]}, _, [args]} = node,
        acc ->
          {node, [Macro.to_string(args) | acc]}

        node, acc ->
          {node, acc}
      end)

    assert [args] = starts
    assert args =~ "server_version_num: state.server_version_num"
  end

  defp call_args(path, function, call) do
    ast = path |> File.read!() |> Code.string_to_quoted!()

    {_, bodies} =
      Macro.prewalk(ast, [], fn
        {kind, _, [{^function, _, _}, body]} = node, acc when kind in [:def, :defp] ->
          {node, [Keyword.get(body, :do) | acc]}

        node, acc ->
          {node, acc}
      end)

    {_, calls} =
      Macro.prewalk(bodies, [], fn
        {{:., _, [{:__aliases__, _, [:QueryBuilder]}, ^call]}, _, args} = node, acc ->
          {node, [Enum.map(args, &Macro.to_string/1) | acc]}

        node, acc ->
          {node, acc}
      end)

    assert [args] = calls
    args
  end
end

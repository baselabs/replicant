defmodule Replicant.DecoderParityTest do
  @moduledoc """
  Compares pgoutput and pglogical delivery on the same supported server. The
  fixture covers cast types, NULL values, unchanged TOAST and replica identities.
  """

  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag :pglogical
  @moduletag timeout: 180_000

  alias Replicant.Test.{DecoderParity, ParitySink, PG16}

  import Replicant.Test.DecoderParity, only: [conn_opts: 1]

  defp major(url) do
    {:ok, pid} = Postgrex.start_link(conn_opts(url) ++ [sync_connect: true, backoff_type: :stop])

    [[v]] =
      Postgrex.query!(pid, "SELECT current_setting('server_version_num')::int", []).rows

    GenServer.stop(pid)
    v
  end

  defp leg(url, decoder) do
    uniq = System.unique_integer([:positive])

    {:ok, ctrl} =
      Postgrex.start_link(conn_opts(url) ++ [pool_size: 2, backoff_type: :stop])

    slot = "rep_parity_#{decoder}_#{uniq}"
    publication = "parity_pub_#{uniq}"

    case decoder do
      :pgoutput -> DecoderParity.setup!(ctrl, decoder, publication: publication)
      :pglogical -> DecoderParity.setup!(ctrl, decoder)
    end

    drop_slot(ctrl, slot)

    extra = leg_extra(decoder, publication)

    on_exit(fn ->
      Replicant.stop(slot)
      {:ok, c} = Postgrex.start_link(conn_opts(url))

      if decoder == :pgoutput do
        Postgrex.query!(c, "DROP PUBLICATION IF EXISTS #{publication}", [])
        Postgrex.query!(c, "DROP PUBLICATION IF EXISTS #{publication}_nothing", [])
      end

      drop_slot(c, slot)
      GenServer.stop(c)
    end)

    mark = ParitySink.mark()

    {:ok, _} =
      Replicant.start_link(
        [
          connection: conn_opts(url),
          slot_name: slot,
          sink: ParitySink,
          go_forward_only: true
        ] ++ extra
      )

    # A FRESH slot only sees commits AFTER its creation point, and `start_link/1`
    # returns before the connect chain has created it — waits here, or the fixture
    # would race into the never-decoded window (probed live; see ADR-0009 testing).
    PG16.wait_until(fn ->
      rows =
        Postgrex.query!(
          ctrl,
          "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
          [slot]
        ).rows

      rows != [] and rows != [[nil]]
    end)

    # settle: writes racing a fresh slot's decode-start window can be skipped
    # server-side (OBSERVED live: the first of three staggered inserts lost) — the
    # boundary is PostgreSQL's fresh-slot semantics, not a decoder behavior.
    :timer.sleep(500)

    DecoderParity.apply_fixture!(ctrl)

    PG16.wait_until(fn ->
      txns = ParitySink.since(mark)
      txns != [] and hd(txns).changes != []
    end)

    Process.sleep(500)
    txns = ParitySink.since(mark)
    Replicant.stop(slot)
    PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 200)
    drop_slot(ctrl, slot)
    GenServer.stop(ctrl)

    DecoderParity.normalize(txns)
  end

  defp leg_extra(:pgoutput, publication) do
    [decoder: :pgoutput, publication: [publication, publication <> "_nothing"]]
  end

  defp leg_extra(:pglogical, _publication) do
    [decoder: :pglogical, replication_sets: ["default", "default_insert_only"]]
  end

  defp drop_slot(ctrl, slot) do
    Postgrex.query!(
      ctrl,
      "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )

    Postgrex.query!(
      ctrl,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  rescue
    _ -> :ok
  end

  setup do
    start_supervised!(ParitySink)
    :ok
  end

  test "the fixture delivers byte-identically across pgoutput and pglogical" do
    url = System.fetch_env!("REPLICANT_TEST_URL")
    version = major(url)

    pglogical = leg(url, :pglogical)
    pgoutput_core = leg(url, :pgoutput)

    assert_core_counts(pglogical, 1, "pglogical")
    assert_core_counts(pgoutput_core, 3, "pgoutput")

    # The pglogical fixture puts RI FULL in the insert-only set. Compare its
    # INSERT and all changes from the other tables at the full field projection.
    for {table, op} <- [
          {"parity_all", nil},
          {"parity_idx", nil},
          {"parity_nothing", nil},
          {"parity_full", :insert}
        ] do
      assert table_changes(pgoutput_core, table, op) == table_changes(pglogical, table, op),
             diff_legs(pgoutput_core, pglogical, table)
    end

    major = div(version, 10_000)

    if expected = System.get_env("EXPECTED_PG_MAJOR") do
      assert Integer.to_string(major) == expected
    end

    IO.puts("PGLOGICAL-PARITY-RECEIPT pg=#{major}")
  end

  defp assert_core_counts(txns, full_count, label) do
    assert_change_counts(
      txns,
      %{
        "parity_all" => 3,
        "parity_idx" => 3,
        "parity_full" => full_count,
        "parity_nothing" => 1
      },
      label
    )
  end

  # A readable failure message: the per-table projections of both legs.
  defp diff_legs(a, b, table) do
    ca = table_changes(a, table)
    cb = table_changes(b, table)

    "decoder legs disagree on " <>
      table <>
      ":\nleft:  " <>
      inspect(ca, limit: :infinity, pretty: true) <>
      "\nright: " <> inspect(cb, limit: :infinity, pretty: true)
  end

  # The FULL per-change projection of one table's delivery: every field the
  # normalized %Transaction{} carries per change (op, schema, table, record,
  # old_record, the sorted unchanged list, and the column metadata). The marquee
  # compares THIS — a narrowed field subset would let a divergence in any dropped
  # field pass silently (review finding: the old projection omitted schema and
  # column metadata entirely).
  defp table_changes(txns, table, op \\ nil) do
    txns
    |> Enum.flat_map(& &1.changes)
    |> Enum.filter(&(&1.table == table and (op == nil or &1.op == op)))
    |> Enum.map(fn ch ->
      %{
        op: ch.op,
        schema: ch.schema,
        table: ch.table,
        record: ch.record,
        old_record: ch.old_record,
        unchanged: Enum.sort(ch.unchanged || []),
        columns: ch.columns
      }
    end)
  end

  # Exact per-table change counts — the marquee's non-vacuity floor: a leg that
  # delivered NOTHING for a compared table cannot pass (a bare != [] assert guards
  # only one table per leg; fresh-review F6).
  defp assert_change_counts(txns, expected, label) do
    actual =
      Enum.map(expected, fn {table, _} -> {table, length(table_changes(txns, table))} end)

    assert actual == Enum.to_list(expected),
           "#{label} delivered wrong change counts: #{inspect(actual)} (expected #{inspect(Enum.to_list(expected))})"
  end
end

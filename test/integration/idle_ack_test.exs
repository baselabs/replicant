defmodule Replicant.IdleAckTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.Test.{LedgerSink, PG16}

  # Spec A1 §3.1 live marquee: an IDLE keepalive on a busy-but-FILTERED publication
  # advances the slot to `wal_end` so WAL is not pinned indefinitely by a quiet
  # (published-change-free) source. The busy-but-filtered load is 400 INSERTs into an
  # UNPUBLISHED `idle_noise` table plus a `pg_switch_wal()` — real WAL the walsender
  # streams as keepalives (no published change), which without the idle-ack would leave
  # `confirmed_flush_lsn` pinned at the last published commit. With the idle-ack the slot
  # advances over that filtered WAL, and a LATER published txn still lands exactly-once
  # (the `_replicant_calls` ledger, not the PK-upsert, proves applied-once).

  setup do
    unless PG16.enabled?() do
      :ok
    end

    # Named `Replicant.Test.LedgerConn` because `LedgerSink` writes its rows/checkpoint/
    # ledger through THAT named connection (test/support/ledger_sink.ex `@conn`); the
    # test's own polls share the same pool (pool_size: 5 keeps polls off the sink's calls).
    {:ok, ctrl} =
      PG16.named_conn(Replicant.Test.LedgerConn, pool_size: 5)

    slot = "rep_idle_#{System.unique_integer([:positive])}"

    reset_schema(ctrl)
    drop_slot(ctrl, slot)

    on_exit(fn ->
      Replicant.stop(slot)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 200)
      {:ok, c} = Postgrex.start_link(PG16.pg_opts())
      drop_slot(c, slot)
    end)

    %{ctrl: ctrl, slot: slot}
  end

  @tag timeout: 120_000
  test "a busy-but-filtered publication advances the slot; a later published txn is delivered exactly-once",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      start_pipeline(slot)

      insert(ctrl, 1, "a")
      PG16.wait_until(fn -> count(ctrl, "sink_orders") == 1 end)

      cf0 = confirmed_flush(ctrl, slot)

      for i <- 1..400,
          do: Postgrex.query!(ctrl, "INSERT INTO idle_noise (v) VALUES ($1)", ["n#{i}"])

      Postgrex.query!(ctrl, "SELECT pg_switch_wal()", [])

      PG16.wait_until(fn -> lsn_gt(confirmed_flush(ctrl, slot), cf0) end, 400)

      assert lsn_gt(confirmed_flush(ctrl, slot), cf0),
             "confirmed_flush did not advance over filtered WAL — idle-ack did not fire"

      insert(ctrl, 2, "b")
      PG16.wait_until(fn -> count(ctrl, "sink_orders") == 2 end, 400)
      assert rows(ctrl, "SELECT id, note FROM sink_orders ORDER BY id") == [[1, "a"], [2, "b"]]
      assert applied_counts(ctrl) |> Map.values() |> Enum.all?(&(&1 == 1))
    end
  end

  @tag timeout: 120_000
  test "reconnect during an idle-advanced window: re-idle-acks and a later published txn is exactly-once",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      start_pipeline(slot)
      insert(ctrl, 1, "a")
      PG16.wait_until(fn -> count(ctrl, "sink_orders") == 1 end)

      # Idle-advance the slot over filtered WAL.
      cf0 = confirmed_flush(ctrl, slot)

      for i <- 1..300,
          do: Postgrex.query!(ctrl, "INSERT INTO idle_noise (v) VALUES ($1)", ["n#{i}"])

      Postgrex.query!(ctrl, "SELECT pg_switch_wal()", [])
      PG16.wait_until(fn -> confirmed_flush(ctrl, slot) > cf0 end, 400)

      # Force a reconnect (handle_connect reseeds received_lsn := checkpoint, in_txn := false).
      [{conn, _}] = Registry.lookup(Replicant.Registry, {slot, :connection})
      ref = Process.monitor(conn)
      Process.exit(conn, :kill)
      assert_receive {:DOWN, ^ref, :process, ^conn, _}, 5000

      # After reconnect: more filtered noise still advances the slot (idle-ack recovers),
      # and a published txn is delivered exactly-once.
      cf1 = confirmed_flush(ctrl, slot)

      for i <- 301..600,
          do: Postgrex.query!(ctrl, "INSERT INTO idle_noise (v) VALUES ($1)", ["n#{i}"])

      Postgrex.query!(ctrl, "SELECT pg_switch_wal()", [])
      PG16.wait_until(fn -> confirmed_flush(ctrl, slot) > cf1 end, 400)

      insert(ctrl, 2, "b")
      PG16.wait_until(fn -> count(ctrl, "sink_orders") == 2 end, 400)
      assert rows(ctrl, "SELECT id, note FROM sink_orders ORDER BY id") == [[1, "a"], [2, "b"]]
      assert applied_counts(ctrl) |> Map.values() |> Enum.all?(&(&1 == 1))
    end
  end

  defp reset_schema(c) do
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS orders_pub", [])

    Postgrex.query!(
      c,
      "DROP TABLE IF EXISTS orders, sink_orders, idle_noise, _replicant_checkpoint, _replicant_calls",
      []
    )

    Postgrex.query!(c, "CREATE TABLE orders (id int PRIMARY KEY, note text)", [])
    Postgrex.query!(c, "CREATE TABLE sink_orders (id int PRIMARY KEY, note text)", [])
    Postgrex.query!(c, "CREATE TABLE idle_noise (id bigserial PRIMARY KEY, v text)", [])
    Postgrex.query!(c, "CREATE TABLE _replicant_checkpoint (id int PRIMARY KEY, lsn bigint)", [])

    Postgrex.query!(
      c,
      "CREATE TABLE _replicant_calls (seq bigserial PRIMARY KEY, lsn bigint, outcome text)",
      []
    )

    Postgrex.query!(c, "CREATE PUBLICATION orders_pub FOR TABLE orders", [])
  end

  defp start_pipeline(slot) do
    :telemetry.attach(
      {__MODULE__, {:active, slot}},
      [:replicant, :connection, :slot_active],
      fn _e, _m, _meta, pid -> send(pid, {:slot_active, slot}) end,
      self()
    )

    {:ok, _pid} =
      Replicant.start_link(
        connection: PG16.pg_opts(),
        slot_name: slot,
        publication: "orders_pub",
        sink: LedgerSink,
        go_forward_only: true
      )

    assert_receive {:slot_active, ^slot}, 15_000
    :telemetry.detach({__MODULE__, {:active, slot}})
  end

  defp confirmed_flush(c, slot) do
    case Postgrex.query!(
           c,
           "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
           [slot]
         ).rows do
      [[lsn]] when is_binary(lsn) -> Replicant.lsn_from_string(lsn)
      _ -> 0
    end
  end

  defp lsn_gt(a, b), do: a > b

  defp insert(c, id, note),
    do: Postgrex.query!(c, "INSERT INTO orders (id, note) VALUES ($1, $2)", [id, note])

  defp count(c, table), do: rows(c, "SELECT count(*) FROM #{table}") |> hd() |> hd()
  defp rows(c, sql), do: Postgrex.query!(c, sql, []).rows

  defp applied_counts(c) do
    rows(c, "SELECT lsn, count(*) FROM _replicant_calls WHERE outcome = 'applied' GROUP BY lsn")
    |> Map.new(fn [lsn, n] -> {lsn, n} end)
  end

  defp drop_slot(c, slot), do: drop_slot(c, slot, 20)

  defp drop_slot(c, slot, 0) do
    Postgrex.query!(
      c,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  end

  defp drop_slot(c, slot, tries) do
    Postgrex.query!(
      c,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  rescue
    _ ->
      Process.sleep(50)
      drop_slot(c, slot, tries - 1)
  end
end

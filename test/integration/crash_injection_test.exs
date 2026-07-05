defmodule Replicant.CrashInjectionTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.Test.{LedgerSink, PG16}

  setup do
    unless PG16.enabled?() do
      :ok
    end

    # `LedgerConn` is shared by the sink (one in-flight `Postgrex.transaction` per
    # applied txn, held for the SlowLedgerSink's 25ms) AND the test's own polling
    # queries. `pool_size: 5` (vs the default 1) keeps the slow-sink burst from
    # starving the test's `count`/audit polls of a connection (spike test).
    {:ok, ctrl} =
      Postgrex.start_link(PG16.pg_opts() ++ [name: Replicant.Test.LedgerConn, pool_size: 5])

    slot = "rep_ci_#{System.unique_integer([:positive])}"

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

  test "baseline: every committed transaction lands exactly once, checkpoint = last LSN", %{
    ctrl: ctrl,
    slot: slot
  } do
    start_pipeline(slot)
    insert(ctrl, 1, "a")
    insert(ctrl, 2, "b")
    insert(ctrl, 3, "c")

    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 3 end)

    assert rows(ctrl, "SELECT id, note FROM sink_orders ORDER BY id") == [
             [1, "a"],
             [2, "b"],
             [3, "c"]
           ]

    assert applied_counts(ctrl) |> Map.values() |> Enum.all?(&(&1 == 1))
  end

  test "crash-and-resume: killing the Connection mid-stream loses nothing", %{
    ctrl: ctrl,
    slot: slot
  } do
    start_pipeline(slot)
    insert(ctrl, 1, "a")
    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 1 end)

    conn = connection_pid(slot)
    ref = Process.monitor(conn)
    Process.exit(conn, :kill)
    assert_receive {:DOWN, ^ref, :process, ^conn, _}, 5000

    insert(ctrl, 2, "b")
    insert(ctrl, 3, "c")
    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 3 end)

    assert rows(ctrl, "SELECT id, note FROM sink_orders ORDER BY id") == [
             [1, "a"],
             [2, "b"],
             [3, "c"]
           ]
  end

  # RISK #1 (finalized against the live stream): the plan's premise — "a crash after
  # sink-commit before ack re-delivers" — does NOT hold for a plain `LedgerSink`. The
  # durable checkpoint IS the Connection's resume LSN, and `START_REPLICATION SLOT ...
  # LOGICAL <checkpoint>` skips every transaction at/below it, so PG never re-delivers
  # an already-checkpointed transaction on a clean crash+restart (proven live). The
  # sink's idempotency dedup (spec §6) is therefore only reachable via the spec §14.15
  # checkpoint-read-fail-open path: `FailOpenLedgerSink.checkpoint/0` reports `nil`, so
  # after the kill-before-ack the restart resumes from `0/0` (PG re-delivers txn 1) AND
  # the Assembler pre-skip is disabled (the re-delivery reaches the sink), whose DURABLE
  # `_replicant_checkpoint` watermark records it `skipped` and applies it zero more times.
  # The kill hook is `[:replicant, :sink, :committed]` — the real post-durable-commit,
  # pre-ack event: it fires from WITHIN the AssemblerServer AFTER the sink durably
  # commits and BEFORE that process sends `{:sink_committed, lsn}` to the Connection, so
  # the `Process.exit(conn, :kill)` (same sender) is enqueued ahead of the ack and wins.
  test "re-delivery dedup: a crash after sink-commit before ack re-delivers → skipped, applied once",
       %{ctrl: ctrl, slot: slot} do
    :telemetry.attach(
      {__MODULE__, :kill_before_ack},
      [:replicant, :sink, :committed],
      fn _e, _m, _meta, target_slot ->
        :telemetry.detach({__MODULE__, :kill_before_ack})

        case Registry.lookup(Replicant.Registry, {target_slot, :connection}) do
          [{conn, _}] -> Process.exit(conn, :kill)
          [] -> :ok
        end
      end,
      slot
    )

    start_pipeline(slot, sink: Replicant.Test.FailOpenLedgerSink)
    insert(ctrl, 1, "a")

    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 1 end)
    PG16.wait_until(fn -> skipped_count(ctrl) >= 1 end, 400)

    assert rows(ctrl, "SELECT id, note FROM sink_orders ORDER BY id") == [[1, "a"]]
    assert applied_counts(ctrl) |> Map.values() |> Enum.all?(&(&1 == 1))
  end

  # RISK #2 (finalized against the live stream): a 200-txn burst at a 25ms/txn sink
  # peaks the AssemblerServer mailbox at ~549–579 messages (200 txns × ~2.9 pgoutput
  # messages each), then drains MONOTONICALLY to completion — bounded by the FINITE
  # burst, never growing unbounded; all 200 apply exactly once. The bound is the
  # burst-implied ceiling (~580) + margin. `max_messages` was left at its `{:stream,
  # sql, [], state}` default: it bounds Postgrex's per-batch SOCKET read (messages
  # accumulated before `handle_data/2`), NOT the downstream AssemblerServer mailbox,
  # so tuning it would not lower this peak — no `lib/` change was warranted.
  @tag :spike
  test "backpressure: a slow sink under a burst keeps the assembler mailbox bounded", %{
    ctrl: ctrl,
    slot: slot
  } do
    start_pipeline(slot, sink: Replicant.Test.SlowLedgerSink)
    for i <- 1..200, do: insert(ctrl, i, "n#{i}")

    asm = assembler_pid(slot)

    max_len =
      Enum.reduce(1..40, 0, fn _, acc ->
        {:message_queue_len, len} = Process.info(asm, :message_queue_len)
        Process.sleep(50)
        max(acc, len)
      end)

    assert max_len < 800,
           "assembler mailbox grew unbounded (#{max_len}); a healthy 200-txn burst " <>
             "peaks at ~580 and drains — this exceeds the burst-implied ceiling, " <>
             "so the per-transaction synchronous drain (§4 backpressure) regressed"

    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 200 end, 1200)
  end

  defp start_pipeline(slot, opts \\ []) do
    sink = Keyword.get(opts, :sink, LedgerSink)

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
        sink: sink,
        go_forward_only: true
      )

    assert_receive {:slot_active, ^slot}, 15_000
    :telemetry.detach({__MODULE__, {:active, slot}})
  end

  defp reset_schema(c) do
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS orders_pub", [])

    Postgrex.query!(
      c,
      "DROP TABLE IF EXISTS orders, sink_orders, _replicant_checkpoint, _replicant_calls",
      []
    )

    Postgrex.query!(c, "CREATE TABLE orders (id int PRIMARY KEY, note text)", [])
    Postgrex.query!(c, "CREATE TABLE sink_orders (id int PRIMARY KEY, note text)", [])
    Postgrex.query!(c, "CREATE TABLE _replicant_checkpoint (id int PRIMARY KEY, lsn bigint)", [])

    Postgrex.query!(
      c,
      "CREATE TABLE _replicant_calls (seq bigserial PRIMARY KEY, lsn bigint, outcome text)",
      []
    )

    Postgrex.query!(c, "CREATE PUBLICATION orders_pub FOR TABLE orders", [])
  end

  defp drop_slot(c, slot) do
    Postgrex.query!(
      c,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  end

  defp insert(c, id, note),
    do: Postgrex.query!(c, "INSERT INTO orders (id, note) VALUES ($1, $2)", [id, note])

  defp count(c, table), do: rows(c, "SELECT count(*) FROM #{table}") |> hd() |> hd()
  defp rows(c, sql), do: Postgrex.query!(c, sql, []).rows

  defp applied_counts(c) do
    rows(c, "SELECT lsn, count(*) FROM _replicant_calls WHERE outcome = 'applied' GROUP BY lsn")
    |> Map.new(fn [lsn, n] -> {lsn, n} end)
  end

  defp skipped_count(c),
    do: rows(c, "SELECT count(*) FROM _replicant_calls WHERE outcome = 'skipped'") |> hd() |> hd()

  defp connection_pid(slot), do: lookup_pid({slot, :connection})
  defp assembler_pid(slot), do: lookup_pid({slot, :assembler})

  defp lookup_pid(key) do
    PG16.wait_until(fn -> match?([{_, _}], Registry.lookup(Replicant.Registry, key)) end, 200)
    [{pid, _}] = Registry.lookup(Replicant.Registry, key)
    pid
  end
end

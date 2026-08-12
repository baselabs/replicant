defmodule Replicant.BatchDeliveryTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.Test.PG16

  setup do
    {:ok, ctrl} =
      PG16.named_conn(Replicant.Test.BDCtrlConn, pool_size: 3)

    {:ok, _} = PG16.named_conn(Replicant.Test.BatchDeliveryConn, pool_size: 2)

    slot = "rep_bd_#{System.unique_integer([:positive])}"
    reset_schema(ctrl)
    drop_slot(ctrl, slot)

    on_exit(fn ->
      Replicant.stop(slot)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 400)
      {:ok, c} = Postgrex.start_link(PG16.pg_opts())
      drop_slot(c, slot)
    end)

    %{ctrl: ctrl, slot: slot}
  end

  test "MARQUEE: a mid-batch delivery fault rolls the whole batch back; resume re-delivers EFFECT-ONCE (dup=0), never loss",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      # max_transactions: 3 → batch 1 = {1,2,3}, batch 2 = {4,5,6}. Large delay so COUNT is the trigger.
      start_pipeline(slot, max_transactions: 3, max_delay_ms: 60_000)

      Enum.each([1, 2, 3], &insert(ctrl, &1))
      PG16.wait_until(fn -> cp_lsn(ctrl) not in [nil, 0] end)
      cp_after_batch1 = cp_lsn(ctrl)
      PG16.wait_until(fn -> MapSet.subset?(MapSet.new([1, 2, 3]), order_ids(ctrl)) end)

      # Fail the NEXT batch's atomic checkpoint write → the whole handle_batch transaction rolls back.
      Postgrex.query!(
        ctrl,
        "ALTER TABLE bd_sink_cp ADD CONSTRAINT tmp_block CHECK (lsn < 0) NOT VALID",
        []
      )

      Enum.each([4, 5, 6], &insert(ctrl, &1))
      # Batch 2 {4,5,6} delivery FAILS atomically → NONE of 4,5,6 land; pipeline halts.
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 800)
      assert cp_lsn(ctrl) == cp_after_batch1
      # Atomic rollback: 4,5,6 are ABSENT (unlike lib-batch where per-txn data was durable).
      refute MapSet.member?(order_ids(ctrl), 4)

      # Clear the fault, restart: resume from batch 1's checkpoint → re-deliver ONLY {4,5,6}
      # (1,2,3 <= checkpoint pre-skipped). The transactional idempotent sink applies each ONCE.
      Postgrex.query!(ctrl, "ALTER TABLE bd_sink_cp DROP CONSTRAINT tmp_block", [])
      start_pipeline(slot, max_transactions: 3, max_delay_ms: 60_000)
      PG16.wait_until(fn -> MapSet.subset?(MapSet.new([1, 2, 3, 4, 5, 6]), order_ids(ctrl)) end)

      # NEVER LOSS: all six present.
      assert MapSet.subset?(MapSet.new([1, 2, 3, 4, 5, 6]), order_ids(ctrl))
      # WELL-FORMED MIRROR: every id appears exactly once in bd_sink_orders. This proves no LOSS
      # and a well-formed state mirror, but is NOT the dup=0 signal — bd_sink_orders.id is a PK and
      # apply_change UPSERTs, so this count is invariant at 1 under a re-delivery (cannot go red).
      assert Enum.all?(1..6, fn id -> order_count(ctrl, id) == 1 end)

      # EFFECT-ONCE, load-bearing (not masked by the PK+UPSERT on bd_sink_orders): bd_sink_calls has
      # NO PK and records one row per COMMITTED delivery, INSIDE the atomic handle_batch transaction.
      # Trace: batch1 {1,2,3} commits → 3 calls; batch2 {4,5,6} first attempt INSERTs 3 calls then the
      # CHECK on bd_sink_cp raises → the whole txn (incl. those 3 calls) rolls back → net 0; halt.
      # Resume re-delivers ONLY {4,5,6} ({1,2,3} pre-skipped by commit_lsn ≤ checkpoint) → 3 calls.
      # Total = 6. A duplicated/re-committed batch would push this > 6, so the assertion CAN go red.
      assert calls_count(ctrl) == 6
      # The checkpoint advanced exactly once for the re-delivered batch.
      assert cp_lsn(ctrl) > cp_after_batch1
    end
  end

  test "a partial batch flushes on the max_delay_ms timer (low-traffic stream advances the checkpoint)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      start_pipeline(slot, max_transactions: 1000, max_delay_ms: 400)
      insert(ctrl, 1)
      PG16.wait_until(fn -> MapSet.member?(order_ids(ctrl), 1) end)
      PG16.wait_until(fn -> cp_lsn(ctrl) not in [nil, 0] end)
      assert cp_lsn(ctrl) > 0
    end
  end

  test "sustained batched delivery flushes multiple batches and keeps streaming (no self-trip)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      start_pipeline(slot, max_transactions: 3, max_delay_ms: 60_000)
      Enum.each(1..9, &insert(ctrl, &1))
      PG16.wait_until(fn -> MapSet.subset?(MapSet.new(1..9), order_ids(ctrl)) end)
      assert Registry.lookup(Replicant.Registry, {slot, :pipeline}) != []
      assert Enum.all?(1..9, fn id -> order_count(ctrl, id) == 1 end)

      # EFFECT-ONCE, load-bearing: three clean batches {1,2,3},{4,5,6},{7,8,9} commit once each with
      # no fault and no re-delivery → 9 committed deliveries in bd_sink_calls. Any self-trip re-flush
      # would push this > 9.
      assert calls_count(ctrl) == 9
    end
  end

  defp start_pipeline(slot, batch) do
    ref = make_ref()
    test_pid = self()

    :telemetry.attach(
      {__MODULE__, ref},
      [:replicant, :connection, :slot_active],
      fn _e, _m, _meta, _ -> send(test_pid, {:slot_active, ref}) end,
      nil
    )

    {:ok, _} =
      Replicant.start_link(
        connection: PG16.pg_opts(),
        slot_name: slot,
        publication: "bd_pub",
        sink: Replicant.Test.LedgerBatchSink,
        batch_delivery: batch,
        # LedgerBatchSink is a :state_mirror sink; on a fresh slot its checkpoint is empty, so the
        # Plan-2 start guard requires go_forward_only (matches crash_injection_test.exs:529). On the
        # post-fault restart the checkpoint is non-empty (batch 1 durable) — the flag is then a no-op.
        go_forward_only: true
      )

    receive do
      {:slot_active, ^ref} -> :ok
    after
      15_000 -> ExUnit.Assertions.flunk("pipeline never reached slot_active for #{slot}")
    end

    :telemetry.detach({__MODULE__, ref})
    PG16.wait_until(fn -> connection_pid(slot) != nil end)
  end

  defp reset_schema(c) do
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS bd_pub", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS bd_orders", [])
    Postgrex.query!(c, "CREATE TABLE bd_orders (id int PRIMARY KEY, note text)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS bd_sink_orders", [])
    Postgrex.query!(c, "CREATE TABLE bd_sink_orders (id int PRIMARY KEY, note text)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS bd_sink_cp", [])
    Postgrex.query!(c, "CREATE TABLE bd_sink_cp (id int PRIMARY KEY, lsn bigint)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS bd_sink_calls", [])
    Postgrex.query!(c, "CREATE TABLE bd_sink_calls (lsn bigint)", [])
    Postgrex.query!(c, "CREATE PUBLICATION bd_pub FOR TABLE bd_orders", [])
  end

  defp insert(c, id),
    do: Postgrex.query!(c, "INSERT INTO bd_orders (id, note) VALUES ($1, $2)", [id, "n#{id}"])

  defp order_ids(c) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(c, "SELECT id FROM bd_sink_orders", [])
    rows |> Enum.map(fn [id] -> id end) |> MapSet.new()
  end

  defp order_count(c, id) do
    %Postgrex.Result{rows: [[n]]} =
      Postgrex.query!(c, "SELECT count(*) FROM bd_sink_orders WHERE id = $1", [id])

    n
  end

  defp calls_count(c) do
    %Postgrex.Result{rows: [[n]]} = Postgrex.query!(c, "SELECT count(*) FROM bd_sink_calls", [])
    n
  end

  defp cp_lsn(c) do
    case Postgrex.query(c, "SELECT lsn FROM bd_sink_cp WHERE id = 1", []) do
      {:ok, %Postgrex.Result{rows: [[lsn]]}} -> lsn
      {:ok, %Postgrex.Result{rows: []}} -> nil
      {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} -> nil
    end
  end

  defp connection_pid(slot) do
    case Registry.lookup(Replicant.Registry, {slot, :connection}) do
      [{pid, _}] -> pid
      [] -> nil
    end
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

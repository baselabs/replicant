defmodule Replicant.BatchingTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.Test.PG16

  @cp_table "replicant_checkpoints_batch"

  setup do
    {:ok, ctrl} =
      Postgrex.start_link(PG16.pg_opts() ++ [name: Replicant.Test.BatchCtrlConn, pool_size: 3])

    {:ok, _} =
      Postgrex.start_link(PG16.pg_opts() ++ [name: Replicant.Test.SinkLedgerConn, pool_size: 2])

    slot = "rep_batch_#{System.unique_integer([:positive])}"
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

  test "MARQUEE: a mid-batch flush fault re-delivers the WHOLE batch (dup > 1, ≤ max_transactions), never loss, earlier batch pre-skipped",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      # max_transactions: 3 → batch 1 = {1,2,3}, batch 2 = {4,5,6}. Large delay/lag so the COUNT
      # cap is the only trigger.
      start_pipeline(slot, max_transactions: 3, max_delay_ms: 60_000)

      Enum.each([1, 2, 3], &insert(ctrl, &1))
      # Batch 1 flushes at count 3 → the store checkpoint advances.
      PG16.wait_until(fn -> store_lsn(ctrl, slot) not in [nil, 0] end)
      lsn_after_batch1 = store_lsn(ctrl, slot)
      PG16.wait_until(fn -> MapSet.subset?(MapSet.new([1, 2, 3]), ledger_ids(ctrl)) end)

      # Force the NEXT batch's flush write to fail (CHECK the >= 0 commit_lsn always violates).
      Postgrex.query!(
        ctrl,
        "ALTER TABLE #{@cp_table} ADD CONSTRAINT tmp_block CHECK (commit_lsn < 0) NOT VALID",
        []
      )

      Enum.each([4, 5, 6], &insert(ctrl, &1))
      # Batch 2 {4,5,6} applies (data durable per-txn) then its flush write fails → halt.
      PG16.wait_until(fn -> MapSet.subset?(MapSet.new([4, 5, 6]), ledger_ids(ctrl)) end)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 800)
      # The checkpoint did NOT advance past batch 1 (batch 2's write failed).
      assert store_lsn(ctrl, slot) == lsn_after_batch1

      # Clear the fault and restart: resume from batch 1's checkpoint → re-deliver ONLY {4,5,6}
      # (batch 1 {1,2,3} <= checkpoint is pre-skipped), sink re-appends each → dup 2.
      Postgrex.query!(ctrl, "ALTER TABLE #{@cp_table} DROP CONSTRAINT tmp_block", [])
      start_pipeline(slot, max_transactions: 3, max_delay_ms: 60_000)
      PG16.wait_until(fn -> id_count(ctrl, 6) >= 2 end)

      # NEVER LOSS: all six present.
      assert MapSet.subset?(MapSet.new([1, 2, 3, 4, 5, 6]), ledger_ids(ctrl))
      # The WHOLE un-checkpointed batch re-delivered (dup window = one batch, > 1 txn) ...
      assert id_count(ctrl, 4) == 2
      assert id_count(ctrl, 5) == 2
      assert id_count(ctrl, 6) == 2
      # ... bounded to max_transactions (exactly 3 duplicated ids, not more) ...
      assert Enum.count([1, 2, 3, 4, 5, 6], fn id -> id_count(ctrl, id) == 2 end) == 3
      # ... and the earlier, checkpointed batch was pre-skipped (no dup).
      assert id_count(ctrl, 1) == 1
      assert id_count(ctrl, 2) == 1
      assert id_count(ctrl, 3) == 1
    end
  end

  test "a partial batch flushes on the max_delay_ms timer (low-traffic stream advances the checkpoint)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      # max_transactions high (won't trigger), short delay → the TIMER is the trigger.
      start_pipeline(slot, max_transactions: 1000, max_delay_ms: 400)

      insert(ctrl, 1)
      PG16.wait_until(fn -> MapSet.member?(ledger_ids(ctrl), 1) end)
      # A single txn is under the count cap; the delay timer flushes it → checkpoint advances.
      PG16.wait_until(fn -> store_lsn(ctrl, slot) not in [nil, 0] end)
      assert store_lsn(ctrl, slot) > 0
    end
  end

  test "sustained batched streaming flushes multiple batches and keeps streaming (no self-trip halt)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      start_pipeline(slot, max_transactions: 3, max_delay_ms: 60_000)

      Enum.each(1..9, &insert(ctrl, &1))
      # Three full batches ({1,2,3},{4,5,6},{7,8,9}) flush; the pipeline stays alive (never
      # :sink_too_slow — the LSN-span cap keeps batching's lag contribution below the ceiling).
      PG16.wait_until(fn -> MapSet.subset?(MapSet.new(1..9), ledger_ids(ctrl)) end)
      PG16.wait_until(fn -> store_lsn(ctrl, slot) not in [nil, 0] end)
      assert Registry.lookup(Replicant.Registry, {slot, :pipeline}) != []
      # Every id delivered exactly once (no crash, no re-delivery on a clean run).
      assert Enum.all?(1..9, fn id -> id_count(ctrl, id) == 1 end)
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
        publication: "batch_ci_pub",
        sink: Replicant.Test.CheckpointRecordingSink,
        checkpoint_store: [connection: PG16.pg_opts(), table: @cp_table, batch: batch]
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
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS batch_ci_pub", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS batch_orders", [])
    Postgrex.query!(c, "CREATE TABLE batch_orders (id int PRIMARY KEY)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS sink_ledger", [])
    Postgrex.query!(c, "CREATE TABLE sink_ledger (commit_lsn bigint, id int)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS #{@cp_table}", [])
    Postgrex.query!(c, "CREATE PUBLICATION batch_ci_pub FOR TABLE batch_orders", [])
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

  defp insert(c, id), do: Postgrex.query!(c, "INSERT INTO batch_orders (id) VALUES ($1)", [id])

  defp ledger_ids(c) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(c, "SELECT DISTINCT id FROM sink_ledger", [])
    rows |> Enum.map(fn [id] -> id end) |> MapSet.new()
  end

  defp id_count(c, id) do
    %Postgrex.Result{rows: [[n]]} =
      Postgrex.query!(c, "SELECT count(*) FROM sink_ledger WHERE id = $1", [id])

    n
  end

  defp store_lsn(c, slot) do
    case Postgrex.query(c, "SELECT commit_lsn FROM #{@cp_table} WHERE slot_name = $1", [slot]) do
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
end

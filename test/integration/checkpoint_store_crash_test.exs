defmodule Replicant.CheckpointStoreCrashTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.Test.PG16

  @cp_table "replicant_checkpoints_ci"

  setup do
    {:ok, ctrl} =
      PG16.named_conn(Replicant.Test.CpCtrlConn, pool_size: 3)

    # The sink's own (non-transactional) connection — survives pipeline crashes.
    {:ok, _} =
      PG16.named_conn(Replicant.Test.SinkLedgerConn, pool_size: 2)

    slot = "rep_cpci_#{System.unique_integer([:positive])}"
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

  test "crash-and-resume: killing the Connection mid-stream loses nothing (never loss)", %{
    ctrl: ctrl,
    slot: slot
  } do
    if PG16.enabled?() do
      start_pipeline(slot)

      insert(ctrl, 1)
      insert(ctrl, 2)
      PG16.wait_until(fn -> ledger_ids(ctrl) |> MapSet.equal?(MapSet.new([1, 2])) end)

      conn = connection_pid(slot)
      ref = Process.monitor(conn)
      Process.exit(conn, :kill)
      assert_receive {:DOWN, ^ref, :process, ^conn, _}, 5000

      insert(ctrl, 3)
      insert(ctrl, 4)
      insert(ctrl, 5)
      PG16.wait_until(fn -> MapSet.subset?(MapSet.new([1, 2, 3, 4, 5]), ledger_ids(ctrl)) end)

      # NEVER LOSS: every committed id present at least once across the crash boundary.
      # (The DUP BOUND is proven deterministically by the write-fault test below, which
      # actually enters the persisted-but-not-checkpointed window; a post-drain kill here
      # would not.)
      assert MapSet.subset?(MapSet.new([1, 2, 3, 4, 5]), ledger_ids(ctrl))
    end
  end

  test "persisted-but-not-checkpointed txn re-delivers on restart: dup bounded to one, never loss",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      start_pipeline(slot)
      insert(ctrl, 1)
      PG16.wait_until(fn -> MapSet.member?(ledger_ids(ctrl), 1) end)
      PG16.wait_until(fn -> store_lsn(ctrl, slot) not in [nil, 0] end)
      lsn_after_1 = store_lsn(ctrl, slot)

      # Deterministically enter the crash window (spec §12.1): force the NEXT checkpoint
      # write to fail with a CHECK the real (>= 0) commit_lsn always violates. The sink
      # still INSERTs the txn (autocommit) → persisted-but-NOT-checkpointed, then the
      # pipeline halts :checkpoint_store_failed and tears down (a `:temporary` child).
      # `NOT VALID`: txn 1's checkpoint row (commit_lsn >= 0) already violates the CHECK,
      # so a validating ADD CONSTRAINT would fail at ALTER time; NOT VALID skips the
      # existing-row scan while STILL enforcing the CHECK on the next write — the store's
      # upsert on txn 2 is an `ON CONFLICT DO UPDATE`, and CHECK is enforced on UPDATE.
      Postgrex.query!(
        ctrl,
        "ALTER TABLE #{@cp_table} ADD CONSTRAINT tmp_block CHECK (commit_lsn < 0) NOT VALID",
        []
      )

      insert(ctrl, 2)
      PG16.wait_until(fn -> MapSet.member?(ledger_ids(ctrl), 2) end)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 400)
      # The checkpoint did NOT advance to txn 2 (the write failed).
      assert store_lsn(ctrl, slot) == lsn_after_1

      # Clear the fault and restart: resume from the store checkpoint (txn 1) → re-deliver
      # ONLY txn 2 (txn 1 <= checkpoint is pre-skipped) → sink re-INSERTs id 2 (one dup),
      # and the checkpoint now advances.
      Postgrex.query!(ctrl, "ALTER TABLE #{@cp_table} DROP CONSTRAINT tmp_block", [])
      start_pipeline(slot)
      PG16.wait_until(fn -> id_count(ctrl, 2) >= 2 end)

      # NEVER LOSS
      assert MapSet.subset?(MapSet.new([1, 2]), ledger_ids(ctrl))
      # no spurious dup of an acked txn
      assert id_count(ctrl, 1) == 1
      # exactly one crash-window dup
      assert id_count(ctrl, 2) == 2
    end
  end

  test "the checkpoint store advances to the last committed LSN", %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      start_pipeline(slot)
      insert(ctrl, 10)
      insert(ctrl, 11)
      PG16.wait_until(fn -> MapSet.subset?(MapSet.new([10, 11]), ledger_ids(ctrl)) end)
      PG16.wait_until(fn -> store_lsn(ctrl, slot) not in [nil, 0] end)
      assert store_lsn(ctrl, slot) > 0
    end
  end

  test "a DELETE is delivered to the lib-mode sink and advances the checkpoint (op-agnostic, not INSERT-only)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      start_pipeline(slot)
      insert(ctrl, 42)
      PG16.wait_until(fn -> MapSet.member?(ledger_ids(ctrl), 42) end)
      PG16.wait_until(fn -> store_lsn(ctrl, slot) not in [nil, 0] end)
      lsn_after_insert = store_lsn(ctrl, slot)

      # DELETE id 42. The sink records the id from `old_record` (a DELETE carries no
      # `record`; the PK is present under DEFAULT replica identity), so the delete lands in
      # the ledger too — proving the marquee's dup-never-loss invariant is observed for the
      # DELETE op-class, not INSERT only, and that the op-agnostic checkpoint path (keyed on
      # commit_lsn, no per-op branch) advances past the DELETE's LSN.
      Postgrex.query!(ctrl, "DELETE FROM cp_orders WHERE id = $1", [42])
      PG16.wait_until(fn -> id_count(ctrl, 42) >= 2 end)
      PG16.wait_until(fn -> store_lsn(ctrl, slot) > lsn_after_insert end)

      # Observed exactly once as INSERT and once as DELETE (never-loss for the DELETE), and
      # the durable checkpoint advanced past the DELETE.
      assert id_count(ctrl, 42) == 2
      assert store_lsn(ctrl, slot) > lsn_after_insert
    end
  end

  # Start a lib-mode pipeline and BLOCK until the replication slot is active (the
  # `[:replicant, :connection, :slot_active]` event fires when the Connection has created
  # OR resumed the slot and begun streaming). Waiting on `connection_pid != nil` alone is
  # a RACE: the Connection registers its Registry name in `init`, BEFORE it connects and
  # creates the slot, so an insert fired in that window commits WAL AHEAD of the slot's
  # start LSN and is NEVER streamed (verified — the plan's `connection_pid != nil` wait
  # timed out on delivery). The event fires on both the create-slot AND resume-on-present-
  # slot paths (`lib/replicant/connection.ex` handle_result :create_slot / begin_present_slot),
  # so this gate holds for the write-fault test's post-halt restart too.
  defp start_pipeline(slot) do
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
        publication: "cp_ci_pub",
        sink: Replicant.Test.CheckpointRecordingSink,
        checkpoint_store: [connection: PG16.pg_opts(), table: @cp_table]
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
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS cp_ci_pub", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS cp_orders", [])
    Postgrex.query!(c, "CREATE TABLE cp_orders (id int PRIMARY KEY)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS sink_ledger", [])
    Postgrex.query!(c, "CREATE TABLE sink_ledger (commit_lsn bigint, id int)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS #{@cp_table}", [])
    Postgrex.query!(c, "CREATE PUBLICATION cp_ci_pub FOR TABLE cp_orders", [])
  end

  # Drop the slot, tolerating the transient "replication slot is active" window: after a
  # `Process.exit(conn, :kill)` the PG-side walsender releases the slot slightly AFTER the
  # BEAM pipeline tears down, so an immediate `pg_drop_replication_slot` can raise
  # `... is active for PID ...`. Retry on any error for ~1s, then a final raising attempt so
  # a genuine teardown fault (not the active-slot race) still surfaces. On a non-existent
  # slot the `WHERE` matches no rows → a no-op success (the `setup` pre-drop).
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

  defp insert(c, id), do: Postgrex.query!(c, "INSERT INTO cp_orders (id) VALUES ($1)", [id])

  defp ledger_ids(c) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(c, "SELECT DISTINCT id FROM sink_ledger", [])
    rows |> Enum.map(fn [id] -> id end) |> MapSet.new()
  end

  defp id_count(c, id) do
    %Postgrex.Result{rows: [[n]]} =
      Postgrex.query!(c, "SELECT count(*) FROM sink_ledger WHERE id = $1", [id])

    n
  end

  # The lib-owned checkpoint table is created (`CREATE TABLE IF NOT EXISTS`) by the
  # CheckpointStore's `ensure/1`, which runs on the first store READ OR write. In the
  # lib-mode pipeline that first access is the Connection's connect-time read
  # (`handle_connect → read_checkpoint → CheckpointStore.read → ensure`), which happens
  # BEFORE `slot_active` fires. Since every `store_lsn` call here runs after
  # `start_pipeline` (which blocks on `slot_active`), the table already exists — so the
  # `:undefined_table` branch below is a DEFENSIVE guard this suite does not actually hit
  # (kept because it is harmless and returns nil rather than raising in a `wait_until`).
  # An undefined-table error (Postgres 42P01) is the only tolerated fault; anything else
  # surfaces.
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

defmodule Replicant.CheckpointStoreSnapshotTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.Test.PG16

  @cp_table "replicant_checkpoints_snap"

  setup do
    {:ok, ctrl} =
      PG16.named_conn(Replicant.Test.CpSnapCtrl, pool_size: 3)

    # The sink's own (non-transactional) connection — survives pipeline teardown.
    {:ok, _} =
      PG16.named_conn(Replicant.Test.SinkLedgerConn, pool_size: 2)

    slot = "rep_cpsnap_#{System.unique_integer([:positive])}"
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

  test "snapshot: true backfills a populated source, then the store holds the handoff LSN and streaming resumes gap-free",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      # Pre-populate the source so the backfill has rows.
      Enum.each(1..3, fn id ->
        Postgrex.query!(ctrl, "INSERT INTO cp_snap_orders (id) VALUES ($1)", [id])
      end)

      start_snapshot_pipeline(slot)

      # Backfill lands the 3 pre-existing rows (delivered via the sink's handle_snapshot/2).
      PG16.wait_until(fn -> MapSet.subset?(MapSet.new([1, 2, 3]), ledger_ids(ctrl)) end)
      # The store holds the handoff LSN (not nil/0).
      PG16.wait_until(fn -> store_lsn(ctrl, slot) not in [nil, 0] end)

      # A NEW post-snapshot insert streams through gap-free.
      Postgrex.query!(ctrl, "INSERT INTO cp_snap_orders (id) VALUES (4)", [])
      PG16.wait_until(fn -> MapSet.subset?(MapSet.new([1, 2, 3, 4]), ledger_ids(ctrl)) end)
      assert MapSet.subset?(MapSet.new([1, 2, 3, 4]), ledger_ids(ctrl))
    end
  end

  test "snapshot handoff WRITE-FAULT halts fail-closed (store stays nil), then recovery never loses rows",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      # Pre-populate the source so the backfill has rows to deliver.
      Enum.each(1..3, fn id ->
        Postgrex.query!(ctrl, "INSERT INTO cp_snap_orders (id) VALUES ($1)", [id])
      end)

      # Pre-create @cp_table with the EXACT shape CheckpointStore expects, then inject a
      # CHECK the real handoff LSN (>= 0) always violates. The table is EMPTY at
      # pre-create, so a plain (validating) ADD CONSTRAINT succeeds immediately (no rows
      # to scan) and is enforced on the handoff INSERT. CheckpointStore's connect-time
      # READ runs CREATE TABLE IF NOT EXISTS (no-op — the table exists), the shape-probe
      # passes on the bigint commit_lsn, and the read returns :empty, so the snapshot
      # proceeds. The handoff WRITE then faults on the CHECK → the flagged
      # write_snapshot_handoff → {:error, _} → Supervisor.halt +
      # {:disconnect, :checkpoint_store_failed} branch (connection.ex).
      Postgrex.query!(
        ctrl,
        "CREATE TABLE #{@cp_table} " <>
          "(slot_name text PRIMARY KEY, commit_lsn bigint NOT NULL, " <>
          "updated_at timestamptz NOT NULL DEFAULT now())",
        []
      )

      Postgrex.query!(
        ctrl,
        "ALTER TABLE #{@cp_table} ADD CONSTRAINT snap_block CHECK (commit_lsn < 0)",
        []
      )

      {:ok, _} =
        Replicant.start_link(
          connection: PG16.pg_opts(),
          slot_name: slot,
          publication: "cp_snap_pub",
          sink: Replicant.Test.CheckpointRecordingSink,
          snapshot: true,
          checkpoint_store: [connection: PG16.pg_opts(), table: @cp_table]
        )

      # The backfill still delivers the rows to the sink (handle_snapshot/2 runs before
      # the handoff write), then the handoff write FAULTS on the CHECK. CORE ASSERTION:
      # the pipeline TEARS DOWN fail-closed and the store checkpoint NEVER landed.
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 800)
      assert Registry.lookup(Replicant.Registry, {slot, :pipeline}) == []
      # The handoff write faulted → the store row was never written.
      assert store_lsn(ctrl, slot) in [nil, 0]

      # RECOVERY — the spec-documented operator remedy (snapshot-design spec §8, scenario
      # "Crash mid-COPY → fail-closed → operator retry"): the durable EXPORT_SNAPSHOT slot
      # from the first attempt PERSISTS (the library NEVER auto-drops — spec §8/§14.19),
      # and the store checkpoint stayed nil. So a bare restart would (correctly) halt
      # `:snapshot_incomplete` again — a present slot + empty checkpoint under
      # `snapshot: true` is "a snapshot began but never handed off", which the library
      # refuses to auto-recover (it cannot distinguish a stale slot from a concurrent/
      # foreign one). The operator drops the orphaned slot, THEN restarts; the redo re-runs
      # the WHOLE snapshot from a fresh EXPORT_SNAPSHOT (a coarse dup — acceptable; assert
      # only never-loss + store-advances, never exact counts on the redo path).
      Postgrex.query!(ctrl, "ALTER TABLE #{@cp_table} DROP CONSTRAINT snap_block", [])
      drop_slot(ctrl, slot)
      start_snapshot_pipeline(slot)

      PG16.wait_until(fn -> MapSet.subset?(MapSet.new([1, 2, 3]), ledger_ids(ctrl)) end)
      PG16.wait_until(fn -> store_lsn(ctrl, slot) not in [nil, 0] end)

      # NEVER LOSS + store advances.
      assert MapSet.subset?(MapSet.new([1, 2, 3]), ledger_ids(ctrl))
      assert store_lsn(ctrl, slot) > 0
    end
  end

  # Start a lib-mode `snapshot: true` pipeline and BLOCK until the slot is active. The
  # `[:replicant, :connection, :slot_active]` event fires once the snapshot handoff has
  # landed and streaming begins (connection.ex handle_info({:snapshot_done, _}) success
  # branch), OR on a post-halt restart resume of a present slot. Gating on this event —
  # not `connection_pid != nil` — avoids the race the sibling Task 11 marquee documented:
  # the Connection registers its Registry name in `init` BEFORE it connects/streams, so a
  # `connection_pid`-only wait can fire an insert into WAL AHEAD of the slot's start LSN.
  defp start_snapshot_pipeline(slot) do
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
        publication: "cp_snap_pub",
        sink: Replicant.Test.CheckpointRecordingSink,
        snapshot: true,
        checkpoint_store: [connection: PG16.pg_opts(), table: @cp_table]
      )

    receive do
      {:slot_active, ^ref} -> :ok
    after
      20_000 -> ExUnit.Assertions.flunk("pipeline never reached slot_active for #{slot}")
    end

    :telemetry.detach({__MODULE__, ref})
  end

  defp reset_schema(c) do
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS cp_snap_pub", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS cp_snap_orders", [])
    Postgrex.query!(c, "CREATE TABLE cp_snap_orders (id int PRIMARY KEY)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS sink_ledger", [])
    Postgrex.query!(c, "CREATE TABLE sink_ledger (commit_lsn bigint, id int)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS #{@cp_table}", [])
    Postgrex.query!(c, "CREATE PUBLICATION cp_snap_pub FOR TABLE cp_snap_orders", [])
  end

  # Drop the slot, tolerating the transient "replication slot is active" window after a
  # pipeline teardown (the PG-side walsender releases the slot slightly AFTER the BEAM
  # pipeline tears down). Retry on any error for ~1s, then a final raising attempt so a
  # genuine teardown fault still surfaces. On a non-existent slot the WHERE matches no
  # rows → a no-op success (the setup pre-drop).
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

  defp ledger_ids(c) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(c, "SELECT DISTINCT id FROM sink_ledger", [])
    rows |> Enum.map(fn [id] -> id end) |> MapSet.new()
  end

  # The lib-owned checkpoint table is created (`CREATE TABLE IF NOT EXISTS`) by the
  # CheckpointStore's `ensure/1`, which runs on the first store READ OR write. In the
  # lib-mode pipeline that first access is the Connection's connect-time read
  # (`handle_connect → read_checkpoint → CheckpointStore.read → ensure`), which happens
  # BEFORE `slot_active` fires. Since every `store_lsn` call here runs after
  # `start_snapshot_pipeline` (which blocks on `slot_active`), the table already exists —
  # so the `:undefined_table` branch below is a DEFENSIVE guard this suite does not
  # actually hit (kept because it is harmless and returns nil rather than raising in a
  # `wait_until`). An undefined-table error (Postgres 42P01) is the only tolerated fault;
  # anything else surfaces.
  defp store_lsn(c, slot) do
    case Postgrex.query(c, "SELECT commit_lsn FROM #{@cp_table} WHERE slot_name = $1", [slot]) do
      {:ok, %Postgrex.Result{rows: [[lsn]]}} -> lsn
      {:ok, %Postgrex.Result{rows: []}} -> nil
      {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} -> nil
    end
  end
end

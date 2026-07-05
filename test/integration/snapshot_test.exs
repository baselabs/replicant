defmodule Replicant.SnapshotTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.Test.{PG16, SnapshotSink}

  setup do
    unless PG16.enabled?(), do: :ok

    {:ok, ctrl} =
      Postgrex.start_link(PG16.pg_opts() ++ [name: Replicant.Test.LedgerConn, pool_size: 5])

    slot = "rep_snap_#{System.unique_integer([:positive])}"
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

  # Seeds MORE than the Snapshotter's @batch (1000) so the cursor spans multiple batches
  # — this is the only test that exercises the multi-batch first_for_table? one-shot + the
  # count accumulation across cursor pages (2500 rows → 3 batches of 1000/1000/500).
  @seed_rows 2500
  test "gap-free/dup-free bootstrap: pre-existing rows seed the mirror; a post-consistent-point commit arrives once via the stream",
       %{ctrl: ctrl, slot: slot} do
    # Bulk-seed via generate_series (fast — one round-trip, not @seed_rows inserts).
    Postgrex.query!(
      ctrl,
      "INSERT INTO orders (id, note) SELECT g, 'seed' || g FROM generate_series(1, #{@seed_rows}) g",
      []
    )

    assert count(ctrl, "orders") == @seed_rows
    assert count(ctrl, "sink_orders") == 0

    # A commit that lands AFTER the export's consistent_point but before/around streaming.
    # Attach to [:snapshot, :started] so the extra row races the snapshot window.
    :telemetry.attach(
      {__MODULE__, :started, slot},
      [:replicant, :snapshot, :started],
      fn _e, _m, _meta, pid -> send(pid, :snapshot_started) end,
      self()
    )

    # Assert the snapshot telemetry (spec §9): :completed carries a duration measurement
    # and change_count = the @seed_rows seeded rows (value-free — counts + LSN only).
    :telemetry.attach(
      {__MODULE__, :completed, slot},
      [:replicant, :snapshot, :completed],
      fn _e, meas, meta, pid -> send(pid, {:snapshot_completed, meas, meta}) end,
      self()
    )

    start_snapshot_pipeline(slot)
    assert_receive :snapshot_started, 15_000
    :telemetry.detach({__MODULE__, :started, slot})
    boundary = @seed_rows + 1
    insert(ctrl, boundary, "streamed-after")

    # loss=0: all seeded rows + the boundary row present in the mirror after handoff + stream.
    PG16.wait_until(fn -> count(ctrl, "sink_orders") == boundary end, 2000)
    assert rows(ctrl, "SELECT id FROM sink_orders ORDER BY id") == Enum.map(1..boundary, &[&1])

    # dup=0 (STREAM side only): the ledger proves each STREAMED commit applied exactly once —
    # the boundary row arrived via the STREAM (a real txn with a commit LSN), not the snapshot.
    # Snapshot rows flow through handle_snapshot (never _replicant_calls), so snapshot-side
    # dup-safety rests on the PK upsert + first_for_table? TRUNCATE — proven by the
    # `== 1..boundary` row-set gate above, not this ledger.
    assert applied_counts(ctrl) |> Map.values() |> Enum.all?(&(&1 == 1))

    # The :completed telemetry proves change_count accumulated across ALL cursor batches
    # (@seed_rows spans multiple @batch pages, spec §9).
    assert_received {:snapshot_completed, %{duration: dur},
                     %{commit_lsn: cp, change_count: @seed_rows}}

    assert is_integer(dur) and dur >= 0 and is_integer(cp) and cp > 0
    :telemetry.detach({__MODULE__, :completed, slot})
  end

  test "empty publication table: a zero-row table still resets the mirror (redo-safety)",
       %{ctrl: ctrl, slot: slot} do
    # Pre-seed a STALE row directly into the mirror (as a crashed prior attempt would).
    Postgrex.query!(ctrl, "INSERT INTO sink_orders (id, note) VALUES (999, 'stale')", [])
    assert count(ctrl, "orders") == 0
    assert count(ctrl, "sink_orders") == 1

    start_snapshot_pipeline(slot)

    # The zero-row source table triggers one first_for_table? reset → the stale row is gone.
    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 0 end, 400)
    assert count(ctrl, "sink_orders") == 0
  end

  test "crash mid-COPY halts fail-closed :snapshot_incomplete; after an operator slot-drop the retry lands exactly once",
       %{ctrl: ctrl, slot: slot} do
    for id <- 1..50, do: insert(ctrl, id, "seed#{id}")

    parent = self()

    # First attempt: kill the Connection right after the snapshot starts (mid-COPY).
    :telemetry.attach(
      {__MODULE__, :kill, slot},
      [:replicant, :snapshot, :started],
      fn _e, _m, _meta, {pid, target} ->
        case Registry.lookup(Replicant.Registry, {target, :connection}) do
          [{conn, _}] -> Process.exit(conn, :kill)
          [] -> :ok
        end

        send(pid, :killed)
      end,
      {self(), slot}
    )

    # Prove the RESTART after the kill halts fail-closed for the RIGHT reason: slot PRESENT
    # (EXPORT_SNAPSHOT created) + checkpoint EMPTY (snapshot never completed) →
    # begin_present_slot → :snapshot_incomplete (spec §8). Registry-empty alone doesn't
    # prove WHICH halt — this reason assertion does.
    :telemetry.attach(
      {__MODULE__, :incomplete, slot},
      [:replicant, :snapshot, :failed],
      fn _e, _m, meta, _cfg ->
        if meta[:reason] == :snapshot_incomplete, do: send(parent, :snapshot_incomplete)
      end,
      nil
    )

    start_snapshot_pipeline(slot)
    assert_receive :killed, 15_000
    :telemetry.detach({__MODULE__, :kill, slot})

    # The restart re-enters the connect matrix (slot present + empty checkpoint) and halts
    # :snapshot_incomplete — never auto-drops. This is THE fail-closed correctness gate.
    assert_receive :snapshot_incomplete, 15_000
    :telemetry.detach({__MODULE__, :incomplete, slot})

    # The pipeline tears down (fail-closed). Operator remediation: drop the incomplete slot.
    PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 400)
    {:ok, c} = Postgrex.start_link(PG16.pg_opts())
    drop_slot(c, slot)

    # Retry from scratch. first_for_table? clears any partial, so the mirror is exact.
    start_snapshot_pipeline(slot)
    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 50 end, 800)
    assert rows(ctrl, "SELECT id FROM sink_orders ORDER BY id") == Enum.map(1..50, &[&1])
    assert applied_counts(ctrl) |> Map.values() |> Enum.all?(&(&1 == 1))
  end

  test "orphan safety: the snapshotter dies with the pipeline (no post-teardown sink mutation)",
       %{ctrl: ctrl, slot: slot} do
    Postgrex.query!(
      ctrl,
      "INSERT INTO orders (id, note) SELECT g, 'seed' || g FROM generate_series(1, 100) g",
      []
    )

    parent = self()
    # Both handlers run IN the Snapshotter process, so self() there is the snapshotter pid.
    # [:snapshot, :started] fires during transaction setup; [:test, :slow_snapshot_sleeping]
    # fires from INSIDE the sleeping sink — i.e. genuinely mid-snapshot.
    :telemetry.attach(
      {__MODULE__, :snap_pid, slot},
      [:replicant, :snapshot, :started],
      fn _e, _m, _meta, _cfg -> send(parent, {:snapshotter_pid, self()}) end,
      nil
    )

    :telemetry.attach(
      {__MODULE__, :sleeping, slot},
      [:replicant, :test, :slow_snapshot_sleeping],
      fn _e, _m, _meta, _cfg -> send(parent, {:snapshot_sleeping, self()}) end,
      nil
    )

    start_snapshot_pipeline(slot, sink: Replicant.Test.SlowSnapshotSink)
    assert_receive {:snapshotter_pid, snap_pid}, 15_000
    :telemetry.detach({__MODULE__, :snap_pid, slot})

    # Wait until the snapshotter is ACTUALLY mid-snapshot (inside the sleeping sink) —
    # not merely started. Killing during transaction setup would break the export and
    # the snapshotter would self-exit regardless of link/monitor, masking the bug.
    assert_receive {:snapshot_sleeping, ^snap_pid}, 15_000
    :telemetry.detach({__MODULE__, :sleeping, slot})
    assert Process.alive?(snap_pid)

    # Tear down the pipeline mid-snapshot (the sink is sleeping 5s). With spawn_link the
    # Connection↔Snapshotter link kills the orphan; with the old spawn_monitor it survives
    # (the sleeping snapshotter is unlinked from the Connection, so the kill can't reach it).
    conn = connection_pid(slot)
    Process.exit(conn, :kill)

    # This poll window (~1s) is well under the 5s sink sleep. With spawn_link the kill
    # propagates near-instantly and the orphan dies inside the window; with the old
    # spawn_monitor the orphan keeps sleeping and is still alive when the window ends.
    # Poll (never flunk) so the descriptive refute below is what fires on the bug.
    poll_dead(snap_pid, 40)

    refute Process.alive?(snap_pid),
           "the orphan snapshotter must be torn down with the pipeline (spawn_link), not survive it"
  end

  defp poll_dead(_pid, 0), do: :ok

  defp poll_dead(pid, tries) do
    if Process.alive?(pid) do
      Process.sleep(25)
      poll_dead(pid, tries - 1)
    else
      :ok
    end
  end

  defp start_snapshot_pipeline(slot, opts \\ []) do
    sink = Keyword.get(opts, :sink, SnapshotSink)

    {:ok, _pid} =
      Replicant.start_link(
        connection: PG16.pg_opts(),
        slot_name: slot,
        publication: "orders_pub",
        sink: sink,
        snapshot: true
      )
  end

  defp connection_pid(slot) do
    PG16.wait_until(
      fn -> match?([{_, _}], Registry.lookup(Replicant.Registry, {slot, :connection})) end,
      200
    )

    [{pid, _}] = Registry.lookup(Replicant.Registry, {slot, :connection})
    pid
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

  defp insert(c, id, note),
    do: Postgrex.query!(c, "INSERT INTO orders (id, note) VALUES ($1, $2)", [id, note])

  defp count(c, table), do: rows(c, "SELECT count(*) FROM #{table}") |> hd() |> hd()
  defp rows(c, sql), do: Postgrex.query!(c, sql, []).rows

  defp applied_counts(c) do
    rows(c, "SELECT lsn, count(*) FROM _replicant_calls WHERE outcome = 'applied' GROUP BY lsn")
    |> Map.new(fn [lsn, n] -> {lsn, n} end)
  end
end

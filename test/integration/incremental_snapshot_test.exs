defmodule Replicant.IncrementalSnapshotTest do
  @moduledoc """
  Integration marquee I for the incremental snapshot (Task 11): resume effect-once,
  closure/keepalive ordering, idle-source instant closure, and (carry-forward Test 4)
  reconnect-during-active-backfill = exactly-one-reader.

  These are LIVE crash-injection marquees on PG16 — the drop-set/closure/epoch
  correctness of Tasks 6-9 is proven end-to-end here. Each test's core convergence
  gate is `final mirror == final source (fresh SELECT) row-for-row`: a REAL
  end-to-end check that RED's if any drop-set/closure/epoch bug lets a stale chunk
  row survive over a newer streamed update. The concurrent writer is what makes that
  gate RED-able (without collisions the drop-set is untested).

  Substrate: `Replicant.Test.IncrementalSnapshotSink` (Task 10) — a `:state_mirror`
  ETS sink whose `mirror/0` is the drop-immune final-state proof and whose append-only
  `ledger/0` is the effect-once (dup/order) proof. Every marquee table has a single PK
  column named `id` (the sink keys the mirror by `record["id"]`).
  """
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.Test.{IncrementalSnapshotSink, PG16}

  setup do
    unless PG16.enabled?(), do: :ok

    {:ok, ctrl} =
      Postgrex.start_link(PG16.pg_opts() ++ [name: Replicant.Test.IncCtrlConn, pool_size: 5])

    # The sink Agent OWNS the ETS mirror/ledger and must OUTLIVE the pipeline under test
    # (Task 11/12 kill+restart the pipeline mid-backfill and assert state persisted). It is
    # supervised by the ExUnit test process (torn down between tests), never a pipeline-
    # internal process — so a `:one_for_all` restart / killed Connection cannot take it down.
    start_supervised!(IncrementalSnapshotSink)
    IncrementalSnapshotSink.reset()

    slot = "rep_inc_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Replicant.stop(slot)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 400)
      {:ok, c} = Postgrex.start_link(PG16.pg_opts())
      drop_slot(c, slot)
    end)

    %{ctrl: ctrl, slot: slot}
  end

  # ---------------------------------------------------------------------------
  # Test 1 — resume marquee (RED-able), sink-owned effect-once.
  # ---------------------------------------------------------------------------
  @tag timeout: 120_000
  test "resume marquee: fail-closed halt mid-backfill, then RESUME (not restart) to effect-once convergence",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      setup_int_table(ctrl, "inc_orders", "inc_pub", 5_000)

      start_incremental(slot, "inc_pub", chunk_rows: 200, max_pending_chunks: 2)

      # A concurrent writer for the whole backfill (started AFTER the pipeline is streaming,
      # per spec "start pipeline; concurrently run a writer" — a high-rate writer racing the
      # fresh-slot CREATE_REPLICATION_SLOT destabilizes slot creation): UPDATE random existing
      # rows to a MONOTONIC note (so a stale chunk row is distinguishable from the fresh
      # streamed value) + INSERT ids 5001..5200. This is what makes the drop-set RED-able.
      {writer, go} = start_writer(fn w, n -> write_orders(w, n, "inc_orders", 5_000) end)

      # Arm the fault after ~10 chunk ledger entries → the NEXT chunk halts fail-closed.
      wait_chunks(10, 4_000)
      failed = attach_snapshot_failed(slot)
      IncrementalSnapshotSink.set_fail_next_chunk(true)

      # Halt occurred: the snapshot :failed telemetry fired AND the pipeline tore down.
      assert_receive {:snapshot_failed, _reason}, 15_000
      detach(failed)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 800)

      # Progress token is non-nil and NOT complete at the halt (backfill is genuinely partial).
      assert {:ok, token} = IncrementalSnapshotSink.snapshot_progress()
      assert token != nil
      refute backfill_complete?()

      pre_restart_len = length(IncrementalSnapshotSink.ledger())

      # Restart against the SAME durable sink state.
      start_incremental(slot, "inc_pub", chunk_rows: 200, max_pending_chunks: 2)

      # Backfill COMPLETES after the restart.
      wait_backfill_complete(8_000)

      stop_writer(writer, go)

      # RESUMED, never RESTARTED: no `first_for_table?: true` chunk after the restart point.
      post = Enum.drop(IncrementalSnapshotSink.ledger(), pre_restart_len)

      refute Enum.any?(post, fn
               {:chunk, _t, _pks, _prog, true, _complete?} -> true
               _ -> false
             end),
             "a first_for_table?: true chunk after the restart means a RESTART, not a RESUME"

      # Effect-once on chunks: sink-owned atomic progress ⇒ ZERO chunk-PK duplicates in the ledger.
      assert chunk_pk_duplicates() == [],
             "sink-owned atomic progress must deliver every chunk PK at most once"

      # END-TO-END convergence: final mirror == final source row-for-row (the closure tripwire).
      wait_converged(ctrl, "inc_orders")
    end
  end

  # ---------------------------------------------------------------------------
  # Test 1b — uuid-PK canonical drop-set (plan review F1).
  # ---------------------------------------------------------------------------
  @tag timeout: 120_000
  test "uuid-PK resume: native-vs-cast PK canonicalization keeps the drop-filter sound (convergence)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      setup_uuid_table(ctrl, "inc_uorders", "incu_pub", 5_000)

      start_incremental(slot, "incu_pub", chunk_rows: 200, max_pending_chunks: 2)

      # Writer UPDATEs EXISTING uuid rows during the backfill (a native-vs-cast PK mismatch
      # would make the drop-filter vacuous → a stale chunk row survives over the newer
      # streamed update → this convergence gate RED's).
      {writer, go} = start_writer(fn w, n -> update_random_uuid(w, n, "inc_uorders") end)

      wait_backfill_complete(15_000)
      stop_writer(writer, go)

      assert chunk_pk_duplicates() == []
      wait_converged(ctrl, "inc_uorders")
    end
  end

  # ---------------------------------------------------------------------------
  # Test 2 — closure/keepalive ordering (the P7 protocol probe as a standing gate).
  # ---------------------------------------------------------------------------
  @tag timeout: 180_000
  test "closure ordering: 20k ~1KB rows + a concurrent writer converge (no chunk applied before a <=HW txn)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      setup_wide_table(ctrl, "inc_kaorder", "inck_pub", 20_000)

      start_incremental(slot, "inck_pub", chunk_rows: 500, max_pending_chunks: 4)

      {writer, go} = start_writer(fn w, n -> update_random_wide(w, n, "inc_kaorder", 20_000) end)

      wait_backfill_complete(30_000)
      stop_writer(writer, go)

      # Any keepalive-closure unsoundness (a chunk applied before an undelivered <=HW txn)
      # surfaces here as a stale row that diverges from the final source.
      wait_converged(ctrl, "inc_kaorder")
    end
  end

  # ---------------------------------------------------------------------------
  # Test 3 — idle-source instant closure (wall-clock bound). D3 idle-closure invariant.
  # ---------------------------------------------------------------------------
  @tag timeout: 120_000
  test "idle closure: a fully idle source backfills 20 chunks well within 60s (closure not keepalive-paced)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      setup_int_table(ctrl, "inc_idle", "inci_pub", 2_000)

      t0 = System.monotonic_time(:millisecond)
      # chunk_rows: 100 over 2,000 rows = 20 chunks, NO concurrent writes.
      start_incremental(slot, "inci_pub", chunk_rows: 100, max_pending_chunks: 2)
      wait_backfill_complete(2_400)
      elapsed = System.monotonic_time(:millisecond) - t0

      # After the first frontier signal (<=1 keepalive interval on a fresh idle stream),
      # LW == HW == frontier ⇒ every chunk closes instantly. A per-chunk keepalive-paced
      # closure would cost ~20 * 30s. Bound = 60s (tolerates ONE initial keepalive interval).
      assert elapsed < 60_000,
             "idle backfill took #{elapsed}ms (>= 60s) — closure is waiting per-chunk keepalives"

      wait_converged(ctrl, "inc_idle")
    end
  end

  # ---------------------------------------------------------------------------
  # Test 4 (carry-forward) — reconnect during active backfill = EXACTLY ONE reader + effect-once.
  # Proves the `retire_reader` fix (spec §8, [[replicant-otp-async-lifetime-hygiene]]).
  # ---------------------------------------------------------------------------
  @tag timeout: 120_000
  test "reconnect during backfill: exactly one live reader + effect-once convergence (retire_reader proof)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      setup_int_table(ctrl, "inc_recon", "incr_pub", 8_000)

      # Observe the resume-into-backfill on reconnect (begin_present_slot {:in_flight, sp}).
      resumed = attach_snapshot_resumed(slot)

      start_incremental(slot, "incr_pub", chunk_rows: 200, max_pending_chunks: 2)

      # Concurrent writer (after slot_active) makes the drop-set RED-able across the reconnect.
      {writer, go} = start_writer(fn w, n -> write_orders(w, n, "inc_recon", 8_000) end)

      # Sample the LIVE reader count for the whole backfill; the retire_reader bug (old
      # reader survives the reconnect) shows up as TWO concurrent readers here.
      {sampler, sample_go} = start_reader_sampler()

      # Reconnect mid-backfill: terminate the slot's walsender backend so `auto_reconnect`
      # re-runs the connect chain → begin_present_slot({:in_flight, sp}) → retire_reader +
      # respawn from durable progress.
      wait_chunks(8, 6_000)
      terminate_walsender(ctrl, slot)

      # The reconnect re-entered the resume-into-backfill path (not a plain resume/fresh run).
      assert_receive {:snapshot_resumed, _}, 15_000
      detach(resumed)

      wait_backfill_complete(20_000)
      stop_writer(writer, go)

      max_readers = stop_reader_sampler(sampler, sample_go)

      # (Observability) EXACTLY ONE live reader across the reconnect: never 2 (retire_reader
      # bug), and we DID observe a reader (non-vacuous sampler).
      assert max_readers == 1,
             "expected exactly one live incremental reader across the reconnect, saw a max of #{max_readers}"

      # (b) No chunk-PK delivered twice beyond the single allowable in-flight chunk — the
      # retired reader's old-epoch chunk was discarded by the window reset (retire_reader fix).
      assert chunk_pk_duplicates() == [],
             "the retired reader's old-epoch chunk must be discarded (no double delivery)"

      # (a) The backfill COMPLETED and converges effect-once despite the reconnect.
      wait_converged(ctrl, "inc_recon")
    end
  end

  # ===========================================================================
  # convergence gate
  # ===========================================================================

  # Wait for the mirror to CONVERGE to the final source, then assert row-for-row. The
  # source is frozen (the writer is stopped before this is called), so it is read ONCE;
  # after an idle source the last pending chunks close on the keepalive frontier (D3),
  # so a fixed sleep is wrong — poll to convergence (bounded), then let the assert show
  # the precise residual diff if it never converges.
  defp wait_converged(ctrl, table) do
    src = source_notes(ctrl, table)
    poll_converge(src, 120)

    mirror = IncrementalSnapshotSink.mirror()

    # final mirror == final source, row-for-row: (1) no extra/dual/missing mirror entries
    # (map_size), and (2) every source id's payload matches (value + completeness). A stale
    # chunk row that survives over a newer streamed update breaks BOTH.
    assert map_size(mirror) == map_size(src),
           "row-for-row: mirror has #{map_size(mirror)} entries for #{map_size(src)} source rows " <>
             "(a dual/stale survivor inflates this; a loss shrinks it)"

    assert mirror_notes(mirror) == src,
           "row-for-row: a mirror row's payload diverges from the final source"
  end

  # Poll (<= tries * 250ms) until the mirror matches the frozen source exactly.
  defp poll_converge(_src, 0), do: :ok

  defp poll_converge(src, tries) do
    mirror = IncrementalSnapshotSink.mirror()

    if map_size(mirror) == map_size(src) and mirror_notes(mirror) == src do
      :ok
    else
      Process.sleep(250)
      poll_converge(src, tries - 1)
    end
  end

  defp source_notes(ctrl, table) do
    Postgrex.query!(ctrl, "SELECT id, note FROM #{table}", []).rows
    |> Map.new(fn [id, note] -> {canon(id), note} end)
  end

  defp mirror_notes(mirror) do
    Map.new(mirror, fn {{_tbl, _pk}, rec} -> {canon(rec["id"]), rec["note"]} end)
  end

  # Canonical id: a 16-byte binary uuid and a dashed uuid string both normalize to 32
  # lowercase hex chars; ints pass through. So a row keyed the same physical id from either
  # delivery path collapses to one key (the mirror-is-a-faithful-mirror premise).
  defp canon(<<_::128>> = bin), do: Base.encode16(bin, case: :lower)
  defp canon(s) when is_binary(s), do: s |> String.replace("-", "") |> String.downcase()
  defp canon(other), do: other

  # Chunk PKs delivered more than once across the whole append-only ledger (canonicalized).
  defp chunk_pk_duplicates do
    IncrementalSnapshotSink.ledger()
    |> Enum.flat_map(fn
      {:chunk, _t, pks, _prog, _first?, _complete?} -> Enum.map(pks, &canon/1)
      _ -> []
    end)
    |> Enum.frequencies()
    |> Enum.filter(fn {_pk, n} -> n > 1 end)
  end

  # ===========================================================================
  # concurrent writer
  # ===========================================================================

  # A background writer running `body.(conn, n)` in a tight loop (n monotonic) until stopped.
  defp start_writer(body) do
    go = :atomics.new(1, [])
    :atomics.put(go, 1, 1)

    task =
      Task.async(fn ->
        {:ok, w} = Postgrex.start_link(PG16.pg_opts())
        drive(w, go, body, 1)
        GenServer.stop(w)
      end)

    {task, go}
  end

  defp stop_writer(task, go) do
    :atomics.put(go, 1, 0)
    Task.await(task, 30_000)
  end

  defp drive(w, go, body, n) do
    if :atomics.get(go, 1) == 1 do
      body.(w, n)
      drive(w, go, body, n + 1)
    else
      :ok
    end
  end

  # UPDATE a random existing id to a monotonic note; also INSERT the extension band once.
  defp write_orders(w, n, table, base) do
    Postgrex.query!(w, "UPDATE #{table} SET note=$2 WHERE id=$1", [:rand.uniform(base), "v#{n}"])

    if n <= 200 do
      Postgrex.query!(
        w,
        "INSERT INTO #{table} (id, note) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING",
        [base + n, "ins#{n}"]
      )
    end
  end

  defp update_random_uuid(w, n, table) do
    Postgrex.query!(
      w,
      "UPDATE #{table} SET note=$1 WHERE id = (SELECT id FROM #{table} ORDER BY random() LIMIT 1)",
      ["v#{n}"]
    )
  end

  defp update_random_wide(w, n, table, base) do
    Postgrex.query!(w, "UPDATE #{table} SET note=$2 WHERE id=$1", [:rand.uniform(base), "v#{n}"])
  end

  # ===========================================================================
  # live reader-count observability (Test 4)
  # ===========================================================================

  # Count live incremental chunk readers by their spawn_link initial call.
  defp live_readers do
    Enum.count(Process.list(), fn pid ->
      Process.info(pid, :initial_call) ==
        {:initial_call, {Replicant.Snapshotter.Incremental, :run, 1}}
    end)
  end

  defp start_reader_sampler do
    go = :atomics.new(1, [])
    :atomics.put(go, 1, 1)
    parent = self()

    task =
      Task.async(fn -> sample_readers(go, 0, parent) end)

    {task, go}
  end

  defp sample_readers(go, max, parent) do
    if :atomics.get(go, 1) == 1 do
      Process.sleep(10)
      sample_readers(go, max(max, live_readers()), parent)
    else
      max
    end
  end

  defp stop_reader_sampler(task, go) do
    :atomics.put(go, 1, 0)
    Task.await(task, 30_000)
  end

  # ===========================================================================
  # pipeline boot / telemetry
  # ===========================================================================

  # Boot the pipeline and block until it is READY to stream. A FRESH incremental slot emits
  # `[:connection, :slot_active]`; a RESUME (present slot + in-flight progress — the restart
  # in Test 1) emits `[:snapshot, :resumed]` and NOT slot_active — so wait for EITHER (the
  # documented readiness gate; never poll `connection_pid != nil`, the racy pattern).
  defp start_incremental(slot, pub, snap_opts) do
    ref = make_ref()
    test_pid = self()
    active = {__MODULE__, :active, ref}
    resumed = {__MODULE__, :resumed_ready, ref}

    :telemetry.attach(
      active,
      [:replicant, :connection, :slot_active],
      fn _e, _m, _meta, _ -> send(test_pid, {:ready, ref}) end,
      nil
    )

    :telemetry.attach(
      resumed,
      [:replicant, :snapshot, :resumed],
      fn _e, _m, _meta, _ -> send(test_pid, {:ready, ref}) end,
      nil
    )

    {:ok, _} =
      Replicant.start_link(
        connection: PG16.pg_opts(),
        slot_name: slot,
        publication: pub,
        sink: IncrementalSnapshotSink,
        snapshot: [mode: :incremental] ++ snap_opts
      )

    receive do
      {:ready, ^ref} -> :ok
    after
      15_000 ->
        ExUnit.Assertions.flunk("pipeline never became ready (slot_active/resumed) for #{slot}")
    end

    :telemetry.detach(active)
    :telemetry.detach(resumed)
  end

  defp attach_snapshot_failed(slot) do
    ref = {__MODULE__, :failed, make_ref()}
    test_pid = self()

    :telemetry.attach(
      ref,
      [:replicant, :snapshot, :failed],
      fn _e, _m, meta, _ -> send(test_pid, {:snapshot_failed, meta[:reason]}) end,
      nil
    )

    _ = slot
    ref
  end

  defp attach_snapshot_resumed(slot) do
    ref = {__MODULE__, :resumed, make_ref()}
    test_pid = self()

    :telemetry.attach(
      ref,
      [:replicant, :snapshot, :resumed],
      fn _e, _m, meta, _ -> send(test_pid, {:snapshot_resumed, meta[:slot_name]}) end,
      nil
    )

    _ = slot
    ref
  end

  defp detach(ref), do: :telemetry.detach(ref)

  # ===========================================================================
  # ledger polling helpers
  # ===========================================================================

  defp wait_backfill_complete(tries), do: PG16.wait_until(&backfill_complete?/0, tries)

  defp backfill_complete? do
    Enum.any?(IncrementalSnapshotSink.ledger(), fn
      {:chunk, _t, _pks, _prog, _first?, true} -> true
      _ -> false
    end)
  end

  defp wait_chunks(n, tries), do: PG16.wait_until(fn -> chunk_count() >= n end, tries)

  defp chunk_count do
    Enum.count(IncrementalSnapshotSink.ledger(), fn
      {:chunk, _t, _pks, _prog, _first?, _complete?} -> true
      _ -> false
    end)
  end

  # ===========================================================================
  # source-table setup
  # ===========================================================================

  defp setup_int_table(c, table, pub, rows) do
    reset_pub_table(c, table, pub, "#{table} (id int PRIMARY KEY, note text)")

    Postgrex.query!(
      c,
      "INSERT INTO #{table} (id, note) SELECT g, 'seed'||g FROM generate_series(1,#{rows}) g",
      []
    )
  end

  defp setup_uuid_table(c, table, pub, rows) do
    reset_pub_table(
      c,
      table,
      pub,
      "#{table} (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), note text)"
    )

    Postgrex.query!(
      c,
      "INSERT INTO #{table} (note) SELECT 'seed'||g FROM generate_series(1,#{rows}) g",
      []
    )
  end

  # ~1 KB rows (a multi-second decode at 20k rows).
  defp setup_wide_table(c, table, pub, rows) do
    reset_pub_table(c, table, pub, "#{table} (id int PRIMARY KEY, note text)")

    Postgrex.query!(
      c,
      "INSERT INTO #{table} (id, note) SELECT g, repeat('x', 1024) FROM generate_series(1,#{rows}) g",
      []
    )
  end

  defp reset_pub_table(c, table, pub, ddl) do
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS #{pub}", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS #{table}", [])
    Postgrex.query!(c, "CREATE TABLE #{ddl}", [])
    Postgrex.query!(c, "CREATE PUBLICATION #{pub} FOR TABLE #{table}", [])
  end

  # ===========================================================================
  # replication-slot helpers
  # ===========================================================================

  # Terminate the walsender backend holding the slot so the replication connection drops
  # and `auto_reconnect` re-runs the connect chain (spec §8 reconnect).
  defp terminate_walsender(c, slot) do
    PG16.wait_until(fn -> active_pid(c, slot) != nil end, 400)
    pid = active_pid(c, slot)
    Postgrex.query!(c, "SELECT pg_terminate_backend($1)", [pid])
  end

  defp active_pid(c, slot) do
    case Postgrex.query!(
           c,
           "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1",
           [slot]
         ).rows do
      [[pid]] when is_integer(pid) -> pid
      _ -> nil
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

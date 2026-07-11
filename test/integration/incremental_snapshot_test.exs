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
      # Drop the per-slot lib-mode store tables — the slot name recurs across `mix test` runs
      # (unique_integer resets per BEAM), so a leftover checkpoint here + the now-absent slot would
      # halt a later run :data_gap. No-op for sink-owned tests (the tables never existed).
      Postgrex.query!(c, "DROP TABLE IF EXISTS cp_#{slot}", [])
      Postgrex.query!(c, "DROP TABLE IF EXISTS prog_#{slot}", [])
      # Lib tests that pass no progress_table share the DEFAULT replicant_snapshot_progress table;
      # delete just this slot's row (guarded — the table may not exist for sink-owned tests).
      Postgrex.query(c, "DELETE FROM replicant_snapshot_progress WHERE slot_name = $1", [slot])
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

      # Halt occurred: the snapshot :failed telemetry fired AND the pipeline tore down. Generous
      # timeout: the fault lands on the NEXT chunk, but under full-suite load on the shared PG16 the
      # applier can be starved (the documented "TIMEOUT ≠ logic bug" trap) — 30s absorbs that jitter.
      assert_receive {:snapshot_failed, _reason}, 30_000
      detach(failed)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 800)

      # Progress token is non-nil and NOT complete at the halt (backfill is genuinely partial).
      assert {:ok, token} = IncrementalSnapshotSink.snapshot_progress()
      assert token != nil
      refute backfill_complete?()

      pre_restart_len = length(IncrementalSnapshotSink.ledger())

      # Restart against the SAME durable sink state (after the PG-side slot is released).
      wait_slot_released(ctrl, slot)
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

  # ---------------------------------------------------------------------------
  # Test 5 — lib+batch × incremental composition (live). This is the FIRST live exercise of the mode
  # combination the interim `:config_invalid` guard used to reject: a `checkpoint_store` with a
  # nested `batch` policy backfilling incrementally under a continuous concurrent writer, end-to-end
  # through the store (lib-mode progress) + batch flush + real Postgres reader. It proves the
  # composition BOOTS and CONVERGES effect-once — guarding the store-progress / batch-flush / drop-
  # set seam the cross-mode-composition blindspot warns about. NOTE: this is a convergence proof, not
  # the sharp drop-filter gate — the old taint→re-read path also converges (loss-free, just slower);
  # the mechanism (lib+batch DROP-FILTERS instead of tainting) is red-gated by the F-DROP UNIT test
  # (assembler_server_test.exs), which returns {:error, :table_discarded} without PK-retention.
  # ---------------------------------------------------------------------------
  @tag timeout: 120_000
  test "lib+batch × incremental: converges effect-once under a concurrent writer (live composition)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      setup_int_table(ctrl, "inc_libbatch", "inclb_pub", 5_000)

      cp_table = "cp_" <> String.replace(slot, "-", "_")

      start_incremental_lib_batch(slot, "inclb_pub", cp_table,
        chunk_rows: 200,
        max_pending_chunks: 4
      )

      # A CONTINUOUS writer to the SAME table being backfilled (each write is a lib+batch txn routed
      # through buffer_txn → last_buffered_changes → track_capped drop-filter). The final row-for-row
      # check RED's if any stale snapshot chunk row survives over a newer streamed update.
      {writer, go} = start_writer(fn w, n -> update_random_wide(w, n, "inc_libbatch", 5_000) end)

      wait_backfill_complete(60_000)
      stop_writer(writer, go)

      wait_converged(ctrl, "inc_libbatch")

      # Effect-once through the batched checkpoint path: no chunk-PK delivered twice.
      assert chunk_pk_duplicates() == [],
             "lib+batch backfill must deliver each chunk-PK once (no double-apply via re-read)"
    end
  end

  # ---------------------------------------------------------------------------
  # Test 6 — DELETE during backfill. A concurrent DELETE feeds the drop-set a change whose `record`
  # is nil (its PK lives in old_record); before the fix that raised BadMapError in track_capped,
  # OUTSIDE the value-free rescue → the AssemblerServer crashed (OTP dumping row bytes = Rule 1) and
  # :one_for_all restarted it, storming under a continuous delete writer so the backfill never
  # completed. Now deletes drop-track via old_record. Gates: the assembler PID is STABLE (no crash/
  # restart), the backfill completes, and it converges (deleted rows absent from BOTH source and
  # mirror — a resurrected ghost row would diverge).
  # ---------------------------------------------------------------------------
  @tag timeout: 120_000
  test "DELETE during backfill: no AssemblerServer crash (Rule 1) + converges with deletes",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      setup_int_table(ctrl, "inc_del", "incdel_pub", 5_000)
      start_incremental(slot, "incdel_pub", chunk_rows: 200, max_pending_chunks: 4)

      asm_pid = GenServer.whereis(Replicant.AssemblerServer.via(slot))
      assert is_pid(asm_pid)

      {writer, go} = start_writer(fn w, n -> update_and_delete(w, n, "inc_del", 5_000) end)

      wait_backfill_complete(60_000)
      stop_writer(writer, go)

      # Same PID ⇒ the assembler never crashed+restarted on a delete (the pre-fix BadMapError).
      assert GenServer.whereis(Replicant.AssemblerServer.via(slot)) == asm_pid,
             "AssemblerServer crashed+restarted during backfill (a DELETE hit pk_tuple(nil, …))"

      wait_converged(ctrl, "inc_del")
    end
  end

  # ---------------------------------------------------------------------------
  # Test 4 (§12.4) — PK-less whole-table fallback: contended → bounded-attempt halt; quiescent → done.
  # ---------------------------------------------------------------------------
  @tag timeout: 120_000
  test "PK-less contended: bounded attempts → halt :snapshot_table_contended (+ :chunk_retried fired)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      setup_nopk_table(ctrl, "inc_nopk_c", "incnopkc_pub", 1_000)

      failed = attach_snapshot_failed(slot)
      retried = attach_snapshot_retried(slot)

      start_incremental(slot, "incnopkc_pub", chunk_rows: 500, max_pending_chunks: 4)

      # A CONTINUOUS writer: the keyless path has NO drop-set, so ANY concurrent write taints the
      # whole read → redo. After @max_table_attempts (3) the reader halts :snapshot_table_contended
      # (spec §6.4), emitting :chunk_retried on each redo (spec §9 "no silent re-read loops").
      {writer, go} = start_writer(fn w, n -> update_random_nopk(w, n, "inc_nopk_c") end)

      assert_receive {:snapshot_failed, :snapshot_table_contended}, 60_000
      assert_receive {:snapshot_retried, "public.inc_nopk_c"}, 5_000

      stop_writer(writer, go)
      detach(failed)
      detach(retried)
    end
  end

  @tag timeout: 120_000
  test "PK-less quiescent: no writer → the whole-table read completes + converges (§12.4)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      setup_nopk_table(ctrl, "inc_nopk_q", "incnopkq_pub", 1_000)
      start_incremental(slot, "incnopkq_pub", chunk_rows: 500, max_pending_chunks: 4)
      wait_backfill_complete(30_000)
      wait_converged(ctrl, "inc_nopk_q")
    end
  end

  # ---------------------------------------------------------------------------
  # Test 8 (§12.7) — completion is delivered at-least-once until durable.
  # ---------------------------------------------------------------------------
  @tag timeout: 120_000
  test "completion at-least-once: a fault on the backfill_complete? call re-delivers on resume, then :complete durable",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      setup_int_table(ctrl, "inc_compl", "inccompl_pub", 400)

      failed = attach_snapshot_failed(slot)

      # Fault ONLY the dedicated completion call (backfill_complete?: true); data chunks land fine.
      IncrementalSnapshotSink.set_fail_completion(true)

      start_incremental(slot, "inccompl_pub", chunk_rows: 200, max_pending_chunks: 4)

      # Data chunks applied, then the completion call faults → halt.
      assert_receive {:snapshot_failed, _reason}, 15_000
      detach(failed)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 800)

      # NOT complete yet — the completion never durably landed.
      refute backfill_complete?()
      assert {:ok, token} = IncrementalSnapshotSink.snapshot_progress()
      assert {:ok, sp} = Replicant.SnapshotProgress.decode(token)
      refute sp.complete?

      pre = length(IncrementalSnapshotSink.ledger())

      # Restart: the completion is RE-DELIVERED (at-least-once) and now succeeds.
      wait_slot_released(ctrl, slot)
      start_incremental(slot, "inccompl_pub", chunk_rows: 200, max_pending_chunks: 4)
      PG16.wait_until(&backfill_complete?/0, 10_000)

      post = Enum.drop(IncrementalSnapshotSink.ledger(), pre)
      # The completion entry re-delivered post-restart …
      assert Enum.any?(post, fn
               {:chunk, _t, _pks, _prog, _first, true} -> true
               _ -> false
             end)

      # … as a RESUME, not a restart (no first_for_table?: true after the halt).
      refute Enum.any?(post, fn
               {:chunk, _t, _pks, _prog, true, _complete?} -> true
               _ -> false
             end)

      # And ONLY NOW is :complete durable.
      assert {:ok, token2} = IncrementalSnapshotSink.snapshot_progress()
      assert {:ok, sp2} = Replicant.SnapshotProgress.decode(token2)
      assert sp2.complete?

      wait_converged(ctrl, "inc_compl")
    end
  end

  # ---------------------------------------------------------------------------
  # Test 6 (§12.6) — sink-owned batch_delivery × incremental composition (cross-mode blindspot).
  # ---------------------------------------------------------------------------
  @tag timeout: 120_000
  test "batch_delivery × incremental: converges + effect-once under a concurrent writer (§12.6)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      setup_int_table(ctrl, "inc_bd", "incbd_pub", 5_000)

      start_incremental_batch_delivery(slot, "incbd_pub", chunk_rows: 200, max_pending_chunks: 4)

      # A concurrent writer whose txns are delivered via handle_batch (batched). A buffered-but-
      # unflushed same-PK txn that flushes AFTER a colliding chunk must WIN (the drop-set consults the
      # receipt boundary — a superset of flush); a stale chunk row surviving over it RED's convergence.
      {writer, go} = start_writer(fn w, n -> write_orders(w, n, "inc_bd", 5_000) end)

      wait_backfill_complete(30_000)
      stop_writer(writer, go)

      # Effect-once on chunks (sink-owned atomic progress) + final row-for-row convergence.
      assert chunk_pk_duplicates() == []
      wait_converged(ctrl, "inc_bd")
    end
  end

  # ---------------------------------------------------------------------------
  # Test 7 (§12.5) — §4 backpressure: a slow sink halts :sink_too_slow mid-backfill; resume continues.
  # ---------------------------------------------------------------------------
  @tag timeout: 120_000
  test "§4 backpressure: a slow sink trips :sink_too_slow DURING an incremental backfill (§12.5 composition)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      setup_int_table(ctrl, "inc_slow", "incslow_pub", 3_000)

      too_slow = attach_sink_too_slow(slot)
      IncrementalSnapshotSink.set_txn_delay(40)

      # Tiny max_inflight_lag + a slow sink (40 ms/txn) + a heavy writer → the applier falls behind
      # and the in-flight lag trips the §4 bound → :sink_too_slow halt DURING an in-flight incremental
      # backfill (the §4 backpressure composes with the snapshot, not just v1 streaming — spec §4/§12.5).
      start_incremental_slow(slot, "incslow_pub", 8_192, chunk_rows: 200, max_pending_chunks: 2)
      {writer, go} = start_writer(fn w, n -> write_orders(w, n, "inc_slow", 3_000) end)

      assert_receive {:sink_too_slow, %{lag: lag}}, 30_000
      assert lag > 8_192
      detach(too_slow)
      stop_writer(writer, go)
      IncrementalSnapshotSink.set_txn_delay(0)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 800)

      # The halt was genuinely MID-backfill (not after completion). The resume-from-partial-progress
      # mechanism itself is the proven Test-1 gate (its sink-fault halt exercises the same resume path);
      # this test's unique coverage is the :sink_too_slow TRIGGER composing with an incremental backfill.
      refute backfill_complete?()
    end
  end

  # ---------------------------------------------------------------------------
  # Test 5 (§12.5) — LIB-mode incremental composition: progress lives in the checkpoint store.
  # ---------------------------------------------------------------------------
  # The lib-mode dup ≤ 1-chunk guarantee follows from checkpoint-AFTER-persist (persist_progress writes
  # the store token only AFTER the sink applied the chunk) — the SAME mechanism the stream-side lib
  # crash tests exercise deterministically (checkpoint_store_crash_test). A live progress-write fault
  # armed EXACTLY at an incremental chunk boundary races the fast local backfill and cannot be made
  # deterministic without a flaky gate, so this marquee proves the lib × incremental COMPOSITION
  # end-to-end (progress in the store, backfill completes + converges effect-once under a concurrent
  # writer) and leaves the dup ≤ 1 boundary case to that shared mechanism.
  @tag timeout: 120_000
  test "lib mode × incremental: store-owned progress completes + converges effect-once under a concurrent writer",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      setup_int_table(ctrl, "inc_lib", "inclib_pub", 5_000)
      cp = "cp_" <> String.replace(slot, "-", "_")
      prog = "prog_" <> String.replace(slot, "-", "_")
      # Drop any store tables from a prior BEAM run: the slot name recurs across `mix test` runs
      # (unique_integer resets), and a STALE checkpoint in cp_<slot> + the now-absent slot would halt
      # :data_gap. The store re-creates both tables on connect.
      Postgrex.query!(ctrl, "DROP TABLE IF EXISTS #{cp}", [])
      Postgrex.query!(ctrl, "DROP TABLE IF EXISTS #{prog}", [])

      start_incremental_lib(slot, "inclib_pub", cp, prog, chunk_rows: 200, max_pending_chunks: 4)

      # A concurrent writer during the LIB-mode backfill — the drop-set collision-corrects exactly as
      # in sink-owned mode, and progress rides the checkpoint store (not the sink). A stale chunk row
      # surviving over a newer streamed update RED's the convergence gate.
      {writer, go} = start_writer(fn w, n -> write_orders(w, n, "inc_lib", 5_000) end)

      wait_backfill_complete(30_000)
      stop_writer(writer, go)

      # Progress is durable IN THE STORE and decodes to :complete (lib mode carries no sink progress).
      assert [[token]] =
               Postgrex.query!(ctrl, "SELECT token FROM #{prog} WHERE slot_name = $1", [slot]).rows

      assert {:ok, sp} = Replicant.SnapshotProgress.decode(token)
      assert sp.complete?

      # Dup ≤ 1 chunk (lib guarantee); an uninterrupted backfill delivers each chunk-PK once.
      assert length(chunk_pk_duplicates()) <= 200

      # NEVER LOSS: final mirror == source, row-for-row.
      wait_converged(ctrl, "inc_lib")
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

  defp update_random_nopk(w, n, table) do
    Postgrex.query!(w, "UPDATE #{table} SET note=$1 WHERE id=$2", ["v#{n}", :rand.uniform(1_000)])
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

  # UPDATE a random row every iteration; DELETE a random row every 4th. A DELETE carries its PK only
  # in old_record (record: nil) — the crash trigger the drop-set fix closes.
  defp update_and_delete(w, n, table, base) do
    Postgrex.query!(w, "UPDATE #{table} SET note=$2 WHERE id=$1", [:rand.uniform(base), "v#{n}"])

    if rem(n, 4) == 0 do
      Postgrex.query!(w, "DELETE FROM #{table} WHERE id=$1", [:rand.uniform(base)])
    end
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
  # PLAIN LIB-mode incremental (§12.5 dup≤1): a checkpoint_store (NO batch) with a per-test unique
  # checkpoint + progress table, so progress is written per chunk (dup ≤ 1 chunk on a fault).
  defp start_incremental_lib(slot, pub, cp_table, prog_table, snap_opts) do
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
        checkpoint_store: [
          connection: PG16.pg_opts(),
          table: cp_table,
          progress_table: prog_table
        ],
        snapshot: [mode: :incremental] ++ snap_opts
      )

    receive do
      {:ready, ^ref} -> :ok
    after
      15_000 -> ExUnit.Assertions.flunk("lib pipeline never became ready for #{slot}")
    end

    :telemetry.detach(active)
    :telemetry.detach(resumed)
  end

  # SLOW-sink incremental (§12.5): the delay-configurable sink + an explicit max_inflight_lag so a
  # tiny bound trips the §4 :sink_too_slow halt. Same readiness gate as start_incremental/3.
  defp start_incremental_slow(slot, pub, max_inflight_lag, snap_opts) do
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
        sink: Replicant.Test.SlowIncrementalSnapshotSink,
        max_inflight_lag: max_inflight_lag,
        snapshot: [mode: :incremental] ++ snap_opts
      )

    receive do
      {:ready, ^ref} -> :ok
    after
      15_000 ->
        ExUnit.Assertions.flunk("slow pipeline never became ready for #{slot}")
    end

    :telemetry.detach(active)
    :telemetry.detach(resumed)
  end

  # Sink-owned batch_delivery (handle_batch/1) × incremental (§12.6): a top-level batch_delivery
  # policy + the batch-capable incremental sink. Same readiness gate as start_incremental/3.
  defp start_incremental_batch_delivery(slot, pub, snap_opts) do
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
        sink: Replicant.Test.BatchIncrementalSnapshotSink,
        batch_delivery: [max_transactions: 50, max_delay_ms: 5000],
        snapshot: [mode: :incremental] ++ snap_opts
      )

    receive do
      {:ready, ^ref} -> :ok
    after
      15_000 ->
        ExUnit.Assertions.flunk("batch_delivery pipeline never became ready for #{slot}")
    end

    :telemetry.detach(active)
    :telemetry.detach(resumed)
  end

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

  # Boot a LIB+BATCH incremental pipeline: a `checkpoint_store` (which makes it lib mode and owns
  # progress) with a NESTED `batch` policy. The store auto-creates its checkpoint + progress tables
  # on connect. Same readiness gate as start_incremental/3. This is the ONLY path that exercises
  # `buffer_txn`'s `last_buffered_changes` retention → track_capped drop-filter — lib-non-batch uses
  # the {:transaction} path, so `batch:` is essential to test the PK-retention code.
  defp start_incremental_lib_batch(slot, pub, cp_table, snap_opts) do
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
        checkpoint_store: [
          connection: PG16.pg_opts(),
          table: cp_table,
          batch: [max_transactions: 50, max_delay_ms: 200]
        ],
        snapshot: [mode: :incremental] ++ snap_opts
      )

    receive do
      {:ready, ^ref} -> :ok
    after
      15_000 ->
        ExUnit.Assertions.flunk("lib+batch pipeline never became ready for #{slot}")
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

  # After a fail-closed halt the ELIXIR pipeline is gone, but the PG-side walsender releases the slot
  # slightly LATER (the documented active-slot race, checkpoint_store_crash_test) — so a restart that
  # connects too soon hits "replication slot already active" and its readiness gate times out. Poll
  # `pg_replication_slots` until the slot is inactive (or gone) before restarting.
  defp wait_slot_released(ctrl, slot) do
    PG16.wait_until(
      fn ->
        case Postgrex.query!(
               ctrl,
               "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
               [slot]
             ).rows do
          [[false]] -> true
          [] -> true
          _active -> false
        end
      end,
      1_200
    )
  end

  defp attach_sink_too_slow(slot) do
    ref = {__MODULE__, :too_slow, make_ref()}
    test_pid = self()

    :telemetry.attach(
      ref,
      [:replicant, :connection, :disconnected],
      fn _e, meas, meta, _ ->
        if meta[:reason] == :sink_too_slow, do: send(test_pid, {:sink_too_slow, meas})
      end,
      nil
    )

    _ = slot
    ref
  end

  defp attach_snapshot_retried(slot) do
    ref = {__MODULE__, :retried, make_ref()}
    test_pid = self()

    :telemetry.attach(
      ref,
      [:replicant, :snapshot, :chunk_retried],
      fn _e, _m, meta, _ -> send(test_pid, {:snapshot_retried, meta[:table]}) end,
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

  # A PK-LESS table (no PRIMARY KEY) → the reader's PK discovery yields pk_raw [] → keyless whole-
  # table fallback (spec §6.4). REPLICA IDENTITY FULL is required for pgoutput to stream its
  # UPDATE/DELETE (a keyless table has no default identity). `id` is an ordinary (non-key) column
  # the mirror keys by; seed values are distinct so the row-for-row compare is well-defined.
  defp setup_nopk_table(c, table, pub, rows) do
    reset_pub_table(c, table, pub, "#{table} (id int, note text)")
    Postgrex.query!(c, "ALTER TABLE #{table} REPLICA IDENTITY FULL", [])

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

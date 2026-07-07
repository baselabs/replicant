defmodule Replicant.AssemblerServerTest do
  use ExUnit.Case, async: false

  alias Replicant.AssemblerServer
  alias Replicant.Decoder.Messages.{Begin, Commit, Insert, Relation}
  alias Replicant.Decoder.Messages.Relation.Column
  alias Replicant.Test.RecordingSink

  defp col(name, type, flags),
    do: %Column{name: name, type: type, flags: flags, type_modifier: -1}

  defp relation(cols),
    do: %Relation{
      id: 1,
      namespace: "public",
      name: "t",
      replica_identity: :default,
      columns: cols
    }

  defp start(slot, sink) do
    {:ok, pid} = AssemblerServer.start_link(slot_name: slot, sink: sink)
    pid
  end

  defp cast(pid, msg, bytes), do: GenServer.cast(pid, {:message, msg, bytes, self()})

  defmodule SkipSink do
    @behaviour Replicant.Sink
    @impl true
    def checkpoint, do: {:ok, 0x100}
    @impl true
    def handle_transaction(_txn), do: raise("must not be called for a skipped txn")
  end

  defmodule FailWriteSink do
    @behaviour Replicant.Sink
    @impl true
    def checkpoint, do: {:ok, nil}
    @impl true
    def handle_transaction(_txn), do: {:error, :store_unreachable}
  end

  # A misbehaving sink whose {:ok, lsn} return is HIGHER than the transaction's own
  # commit LSN. The ack must NOT trust it (that would advance the slot past
  # un-persisted WAL → loss); the connection is told the KNOWN txn.commit_lsn.
  defmodule WrongHighLsnSink do
    @behaviour Replicant.Sink
    @impl true
    def checkpoint, do: {:ok, nil}
    @impl true
    def handle_transaction(_txn), do: {:ok, 0x9999}
  end

  setup do
    {:ok, pid} = RecordingSink.start_link()

    on_exit(fn ->
      # Deterministic teardown: stopping the linked named Agent can race its own
      # link-driven death (TOCTOU on Process.alive?/1), so tolerate a dead pid.
      try do
        Agent.stop(pid)
      catch
        :exit, _ -> :ok
      end
    end)

    RecordingSink.reset()
    :ok
  end

  test "a full transaction applies the sink and notifies the connection with the commit LSN" do
    pid = start("srv_happy", RecordingSink)
    cast(pid, %Begin{final_lsn: 0x2A, commit_timestamp: ~U[2026-07-04 00:00:00Z], xid: 7}, 20)
    cast(pid, relation([col("id", "int4", [:key])]), 30)
    cast(pid, %Insert{relation_id: 1, tuple_data: {"1"}}, 15)

    cast(
      pid,
      %Commit{lsn: 0x2A, end_lsn: 0x2A, commit_timestamp: ~U[2026-07-04 00:00:00Z], flags: []},
      8
    )

    assert_receive {:sink_committed, 0x2A}, 1000
    assert [{0x2A, [change]}] = RecordingSink.seen()
    assert change.op == :insert
    assert change.record["id"] == 1
    refute :sys.get_state(pid).halted
  end

  test "the ack advances to txn.commit_lsn, NEVER a higher sink-returned LSN (no over-advance loss)" do
    pid = start("srv_wrong_lsn", WrongHighLsnSink)
    cast(pid, %Begin{final_lsn: 0x2A, commit_timestamp: ~U[2026-07-04 00:00:00Z], xid: 7}, 20)
    cast(pid, relation([col("id", "int4", [:key])]), 30)
    cast(pid, %Insert{relation_id: 1, tuple_data: {"1"}}, 15)

    cast(
      pid,
      %Commit{lsn: 0x2A, end_lsn: 0x2A, commit_timestamp: ~U[2026-07-04 00:00:00Z], flags: []},
      8
    )

    # The sink returned {:ok, 0x9999}, but only 0x2A was durably committed. Acking
    # 0x9999 would advance the slot past un-persisted WAL → loss.
    assert_receive {:sink_committed, 0x2A}, 1000
    refute_received {:sink_committed, 0x9999}
    refute :sys.get_state(pid).halted
  end

  test "a watermark-skipped transaction still advances the ack (commit_lsn <= checkpoint)" do
    pid = start("srv_skip", SkipSink)
    cast(pid, %Begin{final_lsn: 0x50, commit_timestamp: ~U[2026-07-04 00:00:00Z], xid: 7}, 20)
    cast(pid, relation([col("id", "int4", [:key])]), 30)
    cast(pid, %Insert{relation_id: 1, tuple_data: {"1"}}, 15)

    cast(
      pid,
      %Commit{lsn: 0x50, end_lsn: 0x50, commit_timestamp: ~U[2026-07-04 00:00:00Z], flags: []},
      8
    )

    # 0x50 <= checkpoint 0x100 → skipped, but the ack must still advance to 0x50.
    assert_receive {:sink_committed, 0x50}, 1000
    refute :sys.get_state(pid).halted
  end

  test "a sink WRITE fault halts fail-closed and sends no ack (spec §6 fail-closed)" do
    pid = start("srv_failwrite", FailWriteSink)
    cast(pid, %Begin{final_lsn: 0x2A, commit_timestamp: ~U[2026-07-04 00:00:00Z], xid: 7}, 20)
    cast(pid, relation([col("id", "int4", [:key])]), 30)
    cast(pid, %Insert{relation_id: 1, tuple_data: {"1"}}, 15)

    cast(
      pid,
      %Commit{lsn: 0x2A, end_lsn: 0x2A, commit_timestamp: ~U[2026-07-04 00:00:00Z], flags: []},
      8
    )

    # get_state flushes the mailbox → all four casts processed before we read.
    assert :sys.get_state(pid).halted
    refute_received {:sink_committed, _}
  end

  test "a destructive schema change (dropped column) halts fail-closed" do
    pid = start("srv_destructive", RecordingSink)
    cast(pid, relation([col("id", "int4", [:key]), col("name", "text", [])]), 30)
    cast(pid, relation([col("id", "int4", [:key])]), 30)

    assert :sys.get_state(pid).halted
    refute_received {:sink_committed, _}
  end

  test "post-halt WAL is dropped (no reprocessing during teardown)" do
    pid = start("srv_drop_after_halt", FailWriteSink)
    cast(pid, %Begin{final_lsn: 0x2A, commit_timestamp: ~U[2026-07-04 00:00:00Z], xid: 7}, 20)
    cast(pid, relation([col("id", "int4", [:key])]), 30)
    cast(pid, %Insert{relation_id: 1, tuple_data: {"1"}}, 15)

    cast(
      pid,
      %Commit{lsn: 0x2A, end_lsn: 0x2A, commit_timestamp: ~U[2026-07-04 00:00:00Z], flags: []},
      8
    )

    assert :sys.get_state(pid).halted

    # Further WAL after the halt is ignored (no crash, no ack).
    cast(pid, %Begin{final_lsn: 0x99, commit_timestamp: ~U[2026-07-04 00:00:00Z], xid: 8}, 20)
    assert :sys.get_state(pid).halted
    refute_received {:sink_committed, _}
  end

  test "sink-owned init builds a :sink_owned assembler" do
    {:ok, pid} =
      start_supervised({Replicant.AssemblerServer, slot_name: "s_owned", sink: RecordingSink})

    assert :sys.get_state(pid).asm.mode == :sink_owned
  end

  test "a {:seed_lib_checkpoint, lsn} cast sets the in-memory watermark" do
    writer = fn _ -> :ok end
    # Start with a sink-owned assembler, then inject a lib-mode one directly to
    # exercise the seed cast handler (bypass the store wiring for this unit test).
    {:ok, pid} =
      start_supervised({Replicant.AssemblerServer, slot_name: "s_seed", sink: RecordingSink})

    :sys.replace_state(pid, fn st ->
      %{st | asm: Replicant.Assembler.new(RecordingSink, mode: :lib, checkpoint_writer: writer)}
    end)

    GenServer.cast(AssemblerServer.via("s_seed"), {:seed_lib_checkpoint, 4242})
    # get_state flushes the mailbox → the cast is processed before we read.
    assert :sys.get_state(pid).asm.lib_checkpoint == 4242
  end

  describe "lib-mode write retry (write_with_retry/5)" do
    test "retries a TRANSIENT fault then succeeds (proves it does not give up on the first fault)" do
      calls = :counters.new(1, [])

      stub = fn ->
        n = :counters.get(calls, 1)
        :counters.add(calls, 1, 1)
        if n == 0, do: {:error, %Replicant.Error{reason: :checkpoint_store_failed}}, else: :ok
      end

      assert :ok = Replicant.AssemblerServer.write_with_retry(stub, "rep_wr", 3, 5, 0)
      assert :counters.get(calls, 1) == 2
    end

    test "a TRANSIENT fault that never clears halts after max_retries (returns {:error, reason})" do
      calls = :counters.new(1, [])

      stub = fn ->
        :counters.add(calls, 1, 1)
        {:error, %Replicant.Error{reason: :checkpoint_store_failed}}
      end

      assert {:error, :checkpoint_store_failed} =
               Replicant.AssemblerServer.write_with_retry(stub, "rep_wr2", 2, 5, 0)

      assert :counters.get(calls, 1) == 3
    end

    test "a PERMANENT fault (schema mismatch) returns immediately, 0 retries" do
      calls = :counters.new(1, [])

      stub = fn ->
        :counters.add(calls, 1, 1)
        {:error, %Replicant.Error{reason: :checkpoint_store_schema_mismatch}}
      end

      assert {:error, :checkpoint_store_schema_mismatch} =
               Replicant.AssemblerServer.write_with_retry(stub, "rep_wr3", 5, 5, 0)

      assert :counters.get(calls, 1) == 1
    end
  end

  describe "batched checkpointing (spec §7/§9)" do
    defmodule BatchOkSink do
      @behaviour Replicant.Sink
      @impl true
      def checkpoint, do: {:ok, nil}
      @impl true
      def handle_transaction(txn), do: {:ok, txn.commit_lsn}
    end

    # Inject a lib+batch assembler (a stub writer that acks to the test) into a running server,
    # bypassing the CheckpointStore wiring — the seed test (above) uses the same pattern.
    defp inject_batched(pid, slot, writer, policy) do
      :sys.replace_state(pid, fn st ->
        asm =
          Replicant.Assembler.new(BatchOkSink,
            mode: :lib,
            checkpoint_writer: writer,
            slot_name: slot,
            lib_checkpoint: 0,
            batch: policy
          )

        %{st | asm: asm}
      end)
    end

    # Cache the relation once (id 1), then per-txn Begin(lsn)/Insert/Commit(lsn).
    defp cache_relation(pid), do: cast(pid, relation([col("id", "int4", [:key])]), 30)

    defp drive_txn(pid, lsn) do
      cast(pid, %Begin{final_lsn: lsn, commit_timestamp: ~U[2026-07-04 00:00:00Z], xid: lsn}, 20)
      cast(pid, %Insert{relation_id: 1, tuple_data: {"1"}}, 15)

      cast(
        pid,
        %Commit{lsn: lsn, end_lsn: lsn, commit_timestamp: ~U[2026-07-04 00:00:00Z], flags: []},
        8
      )
    end

    test "buffers under the count cap (no ack) then flushes ONE ack at the cap" do
      pid = start("srv_batch_count", RecordingSink)
      test = self()

      inject_batched(pid, "srv_batch_count", fn lsn -> send(test, {:wrote, lsn}) && :ok end,
        max_transactions: 2,
        max_delay_ms: 60_000,
        max_span: 1_000_000
      )

      cache_relation(pid)
      drive_txn(pid, 100)
      refute_receive {:sink_committed, _}, 100
      drive_txn(pid, 200)
      assert_receive {:sink_committed, 200}, 1000
      assert_received {:wrote, 200}
      refute :sys.get_state(pid).halted
    end

    test "a partial batch flushes on the max_delay_ms timer (low-traffic safety valve)" do
      pid = start("srv_batch_timer", RecordingSink)
      test = self()

      inject_batched(pid, "srv_batch_timer", fn lsn -> send(test, {:wrote, lsn}) && :ok end,
        max_transactions: 100,
        max_delay_ms: 50,
        max_span: 1_000_000
      )

      cache_relation(pid)
      drive_txn(pid, 100)
      # Under the count cap → buffered; the timer flushes it within max_delay_ms.
      assert_receive {:sink_committed, 100}, 1000
      assert_received {:wrote, 100}
    end

    test "a stale :batch_timeout after a count-flush is inert and does not disrupt the next batch" do
      pid = start("srv_batch_stale", RecordingSink)

      inject_batched(pid, "srv_batch_stale", fn _lsn -> :ok end,
        max_transactions: 2,
        max_delay_ms: 60_000,
        max_span: 1_000_000
      )

      cache_relation(pid)
      drive_txn(pid, 100)
      drive_txn(pid, 200)
      assert_receive {:sink_committed, 200}, 1000

      # A leftover timer fires after the batch already flushed by the count cap. It is inert by
      # DEFENSE-IN-DEPTH: after the flush pending_lsn is nil, so handle_info's `batch_pending?` guard
      # short-circuits to a no-op; and even if it didn't, flush_batch's `:empty` clause is a no-op.
      # Each layer backstops the other, so this test red-gates the COMPOSITE invariant, NOT either
      # guard alone — removing ONLY `batch_pending?` still lands on `:empty` (green), removing ONLY
      # `:empty` never reaches flush_batch (green); only removing BOTH reddens it (a stale flush of
      # the empty batch). The `:empty` clause is red-gated individually by the assembler "no open
      # batch is :empty" test. Asserts: no spurious ack, and the NEXT batch still flushes (state not
      # corrupted).
      send(pid, :batch_timeout)
      refute_receive {:sink_committed, _}, 100

      drive_txn(pid, 300)
      drive_txn(pid, 400)
      assert_receive {:sink_committed, 400}, 1000
      refute :sys.get_state(pid).halted
    end

    test "a :batch_timeout AFTER a halt is a no-op even with a batch OPEN (halted guard — OTP async-lifetime)" do
      pid = start("srv_batch_halted", RecordingSink)
      test = self()

      inject_batched(pid, "srv_batch_halted", fn lsn -> send(test, {:wrote, lsn}) && :ok end,
        max_transactions: 5,
        max_delay_ms: 60_000,
        max_span: 1_000_000
      )

      cache_relation(pid)
      # OPEN a batch (pending_lsn set) → batch_pending? is TRUE, so ONLY the halted guard can
      # stop a flush. This test goes RED if handle_info(:batch_timeout, %{halted: true}) is removed
      # (the general clause would flush the pending batch and ack it).
      drive_txn(pid, 100)
      refute_receive {:sink_committed, _}, 100

      :sys.replace_state(pid, fn st -> %{st | halted: true} end)
      send(pid, :batch_timeout)
      refute_receive {:sink_committed, _}, 100
      refute_received {:wrote, _}
      assert :sys.get_state(pid).halted
    end

    test "a batch-flush WRITE fault halts fail-closed and sends NO ack (buffer discarded)" do
      pid = start("srv_batch_failwrite", RecordingSink)

      inject_batched(
        pid,
        "srv_batch_failwrite",
        fn _lsn -> {:error, :checkpoint_store_failed} end,
        max_transactions: 1,
        max_delay_ms: 60_000,
        max_span: 1_000_000
      )

      cache_relation(pid)
      drive_txn(pid, 100)
      # get_state flushes the mailbox → the flush + halt are processed before we read.
      assert :sys.get_state(pid).halted
      refute_received {:sink_committed, _}
    end

    # Runs in the AssemblerServer process (not the test), so it reports via Application env — safe
    # because assembler_server_test.exs is `async: false`. checkpoint/0 is the resume watermark.
    defmodule SrvBatchSink do
      def checkpoint, do: {:ok, nil}

      def handle_batch(txns) do
        send(
          Application.get_env(:replicant, :srv_batch_pid),
          {:batch, Enum.map(txns, & &1.commit_lsn)}
        )

        {:ok, List.last(txns).commit_lsn}
      end
    end

    test "a sink-owned batch pipeline delivers via handle_batch at the count cap, then {:reset_batch} discards an open batch" do
      Application.put_env(:replicant, :srv_batch_pid, self())
      on_exit(fn -> Application.delete_env(:replicant, :srv_batch_pid) end)

      {:ok, pid} =
        AssemblerServer.start_link(
          slot_name: "srv_bd",
          sink: SrvBatchSink,
          batch: [max_transactions: 2, max_delay_ms: 60_000, max_span: 1_000_000]
        )

      # cache_relation/1 (assembler_server_test.exs helper) caches relation id 1 FIRST — every
      # batched test in this block does so, else drive_txn's %Insert{relation_id: 1} halts
      # :config_invalid ("row for uncached relation", assembler.ex:358-360). drive_txn/2 + cast/3
      # are existing helpers (assembler_server_test.exs:26,268) casting {:message, ..., self()}.
      cache_relation(pid)
      # Seed stream_floor so the span cap does not fire.
      GenServer.cast(pid, {:stream_floor, 0})
      drive_txn(pid, 100)
      :sys.get_state(pid)
      refute_received {:batch, _}
      drive_txn(pid, 200)
      assert_receive {:batch, [100, 200]}

      # Open a new batch, then {:reset_batch} must discard it (no delivery on the next timer/flush).
      drive_txn(pid, 300)
      GenServer.cast(pid, {:reset_batch})
      state = :sys.get_state(pid)
      refute Replicant.Assembler.batch_pending?(state.asm)
      assert state.asm.batch_txns == []

      # {:reset_batch} must also CANCEL the armed flush timer (not just clear the batch) —
      # else a re-buffered batch rides the stale timer's window. Timer was armed by txn 300.
      assert state.batch_timer == nil
    end

    test "a reconnect re-seed discards the open batch — no stale accumulators across reconnect (CV1)" do
      pid = start("srv_batch_reseed", RecordingSink)
      test = self()

      inject_batched(pid, "srv_batch_reseed", fn lsn -> send(test, {:wrote, lsn}) && :ok end,
        max_transactions: 5,
        max_delay_ms: 60_000,
        max_span: 1_000_000
      )

      cache_relation(pid)
      drive_txn(pid, 100)

      # A batch is OPEN (buffered, un-checkpointed) and its flush timer is ARMED (max_delay_ms set).
      state_before = :sys.get_state(pid)
      assert Replicant.Assembler.batch_pending?(state_before.asm)
      assert is_reference(state_before.batch_timer)

      # A mid-stream Connection reconnect re-seeds the durable checkpoint. The stale in-memory batch
      # MUST be discarded: its txns re-stream from the durable checkpoint and re-buffer as a FRESH
      # batch, matching the crash/stop→restart dup model (bounds mid-reconnect dup to one batch;
      # stale accumulators would misalign flush boundaries). No spurious ack on the discard.
      GenServer.cast(pid, {:seed_lib_checkpoint, 500})
      state = :sys.get_state(pid)
      asm = state.asm
      refute Replicant.Assembler.batch_pending?(asm)
      assert asm.batch_count == 0
      assert asm.lib_checkpoint == 500
      # {:seed_lib_checkpoint} must ALSO cancel the now-stale flush timer (same OTP-async-lifetime
      # hygiene as {:reset_batch}) — else the orphaned timer fires against the re-buffered fresh
      # batch. Load-bearing: dropping cancel_batch_timer/1 from the handler reddens this.
      assert state.batch_timer == nil
      refute_received {:sink_committed, _}
      refute_received {:wrote, _}
    end
  end

  describe "spill (spec §5/§9)" do
    alias Replicant.Decoder.Messages.{Insert, StreamCommit, StreamStart}
    alias Replicant.Decoder.Messages.Relation
    alias Replicant.Decoder.Messages.Relation.Column

    test "the server builds a spill-capable assembler and signals {:spilled_bytes, total} to the connection after a spill" do
      base = Path.join(System.tmp_dir!(), "srv_spill_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf(base) end)

      {:ok, pid} =
        AssemblerServer.start_link(
          slot_name: "srv_sp",
          sink: Replicant.Test.RecordingSink,
          streaming: [max_concurrent_txns: 8, spill: [dir: base, max_spill_bytes: 1_000_000]],
          max_inflight_lag: 100
        )

      state0 = :sys.get_state(pid)
      assert state0.asm.spill == [dir: base, max_spill_bytes: 1_000_000]
      assert state0.asm.max_inflight_lag == 100

      rel = %Relation{
        id: 1,
        namespace: "public",
        name: "t",
        replica_identity: :default,
        columns: [%Column{name: "v", type: "int4", flags: [:key], type_modifier: nil}]
      }

      GenServer.cast(pid, {:message, rel, 10, self()})
      GenServer.cast(pid, {:message, %StreamStart{xid: 100, first_segment: true}, 4, self()})

      for v <- 1..3,
          do:
            GenServer.cast(
              pid,
              {:message, %Insert{xid: 100, relation_id: 1, tuple_data: {"#{v}"}}, 60, self()}
            )

      # The AssemblerServer signals the Connection via a PLAIN send → its handle_info substrate
      # (the same idiom the existing dispatch/3 uses for {:sink_committed, lsn}). Task 10's
      # Connection handles {:spilled_bytes, total} in handle_info. So assert the plain message,
      # NOT a {:"$gen_cast", ...} wrapper.
      assert_receive {:spilled_bytes, total} when total > 0
    end

    test "a sub-bound message does NOT signal {:spilled_bytes} (the spilled_total != before guard)" do
      base = Path.join(System.tmp_dir!(), "srv_nospill_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf(base) end)

      {:ok, pid} =
        AssemblerServer.start_link(
          slot_name: "srv_nosp",
          sink: Replicant.Test.RecordingSink,
          streaming: [max_concurrent_txns: 8, spill: [dir: base, max_spill_bytes: 1_000_000]],
          max_inflight_lag: 100
        )

      rel = %Relation{
        id: 1,
        namespace: "public",
        name: "t",
        replica_identity: :default,
        columns: [%Column{name: "v", type: "int4", flags: [:key], type_modifier: nil}]
      }

      GenServer.cast(pid, {:message, rel, 10, self()})
      GenServer.cast(pid, {:message, %StreamStart{xid: 100, first_segment: true}, 4, self()})
      # ONE small Insert, 50 bytes — well UNDER max_inflight_lag: 100, so resident_total never
      # crosses the bound and no spill fires. spilled_total stays 0, so `spilled_total != before`
      # is false → NO {:spilled_bytes} signal. Guards against a false-positive send.
      GenServer.cast(
        pid,
        {:message, %Insert{xid: 100, relation_id: 1, tuple_data: {"1"}}, 50, self()}
      )

      refute_receive {:spilled_bytes, _}, 100
      # No spill actually happened (sanity: the buffer stayed resident).
      assert :sys.get_state(pid).asm.spilled_total == 0
    end

    test "a halt discards open spill files (reset_streams cleanup on the halted assembler, spec §5)" do
      base = Path.join(System.tmp_dir!(), "srv_halt_spill_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf(base) end)

      {:ok, pid} =
        AssemblerServer.start_link(
          slot_name: "srv_halt_sp",
          sink: Replicant.Test.RecordingSink,
          streaming: [max_concurrent_txns: 8, spill: [dir: base, max_spill_bytes: 1_000_000]],
          max_inflight_lag: 100
        )

      rel = %Relation{
        id: 1,
        namespace: "public",
        name: "t",
        replica_identity: :default,
        columns: [%Column{name: "v", type: "int4", flags: [:key], type_modifier: nil}]
      }

      GenServer.cast(pid, {:message, rel, 10, self()})
      GenServer.cast(pid, {:message, %StreamStart{xid: 100, first_segment: true}, 4, self()})

      for v <- 1..3,
          do:
            GenServer.cast(
              pid,
              {:message, %Insert{xid: 100, relation_id: 1, tuple_data: {"#{v}"}}, 60, self()}
            )

      # Force the spill and capture the on-disk file path from xid 100's stream buffer.
      assert_receive {:spilled_bytes, total} when total > 0
      state = :sys.get_state(pid)
      spill_path = state.asm.stream_txns[100].spill.path
      assert File.exists?(spill_path)

      # Drive a HALT: a StreamCommit for an UNKNOWN top xid → {:halt, :config_invalid, asm} where the
      # halted asm still carries xid 100's open (spilled) buffer. The {:halt} dispatch runs
      # reset_streams |> reset_batch for the file-delete side effect. Supervisor.halt/2 spawns an
      # UNLINKED teardown that Registry.lookup-misses in the unit context (no registered pipeline) and
      # returns :ok — never errors/hangs — so the halt path completes cleanly here.
      GenServer.cast(
        pid,
        {:message,
         %StreamCommit{xid: 999, commit_lsn: 0x50, end_lsn: 0x50, commit_timestamp: nil}, 8,
         self()}
      )

      # get_state flushes the mailbox → the halt (and its spill-file discard) ran before we read.
      assert :sys.get_state(pid).halted
      refute File.exists?(spill_path)
    end
  end

  describe "streaming (spec §7/§9)" do
    alias Replicant.Decoder.Messages.StreamStart

    test "the server builds a stream-capable assembler and {:reset_streams} discards open streams" do
      {:ok, pid} =
        AssemblerServer.start_link(
          slot_name: "srv_stream",
          sink: Replicant.Test.RecordingSink,
          streaming: [max_concurrent_txns: 8]
        )

      assert :sys.get_state(pid).asm.max_concurrent_txns == 8

      GenServer.cast(pid, {:message, %StreamStart{xid: 100, first_segment: true}, 4, self()})
      :sys.get_state(pid)
      assert Map.has_key?(:sys.get_state(pid).asm.stream_txns, 100)

      GenServer.cast(pid, {:reset_streams})
      state = :sys.get_state(pid)
      assert state.asm.stream_txns == %{}
      assert state.asm.current_stream_xid == nil
    end
  end
end

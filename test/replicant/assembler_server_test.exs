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

    test "a spilled txn COMMITTING re-casts {:spilled_bytes} with the LOWER total (frees disk bytes; the §4 numerator un-strands after a large spilled txn commits)" do
      base = Path.join(System.tmp_dir!(), "srv_spdec_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf(base) end)

      defmodule ConsumingSink do
        def checkpoint, do: {:ok, nil}

        def handle_transaction(txn) do
          # Consume the lazy Reader WITHIN the call (Enum, not Enum.to_list) — delivery contract.
          Enum.each(txn.changes, fn _ -> :ok end)
          {:ok, txn.commit_lsn}
        end
      end

      {:ok, pid} =
        AssemblerServer.start_link(
          slot_name: "srv_spdec",
          sink: ConsumingSink,
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

      GenServer.cast(pid, {:message, %Replicant.Decoder.Messages.StreamStop{}, 4, self()})

      # The spill fires → an INCREASE cast.
      assert_receive {:spilled_bytes, up} when up > 0

      # The commit delivers the spilled txn and frees its disk bytes → spilled_total drops → a NET
      # re-cast with the LOWER total (RED before the CV3 fix cast the delta, not the net total —
      # the numerator would strand stale-high after the commit).
      GenServer.cast(
        pid,
        {:message, %StreamCommit{xid: 100, commit_lsn: 900, end_lsn: 901, commit_timestamp: nil},
         8, self()}
      )

      assert_receive {:spilled_bytes, down} when down < up
      assert :sys.get_state(pid).asm.spilled_total == down
    end

    test "a SINGLE row larger than max_inflight_lag spills — the bound-crossing change is appended BEFORE the spill trigger runs (CV2)" do
      base = Path.join(System.tmp_dir!(), "srv_bigrow_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf(base) end)

      {:ok, pid} =
        AssemblerServer.start_link(
          slot_name: "srv_bigrow",
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

      # ONE Insert whose WAL bytes (250) exceed max_inflight_lag (100): the change is itself larger
      # than the RAM bound, so it MUST spill. That requires the change to already be in the buffer
      # when the spill trigger (maybe_spill) runs — i.e. handle_message (append) BEFORE observe_bytes.
      GenServer.cast(
        pid,
        {:message, %Insert{xid: 100, relation_id: 1, tuple_data: {"x"}}, 250, self()}
      )

      buf = :sys.get_state(pid).asm.stream_txns[100]

      # RED with observe-before-append: the change isn't in buf.changes when maybe_spill flushes →
      # wrote 0, spilled_bytes stays 0 and the change stays resident (unaccounted, never spilled).
      assert buf.spilled_bytes > 0
      assert buf.changes == []
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

  defmodule ChunkLedgerSink do
    @behaviour Replicant.Sink
    def checkpoint, do: {:ok, nil}
    def handle_transaction(_txn), do: {:ok, 0}

    def handle_snapshot(changes, ctx) do
      send(Process.whereis(:asrv_chunk_test), {:snapshot_call, changes, ctx})
      :ok
    end

    def snapshot_progress, do: {:ok, nil}
  end

  defmodule FaultChunkSink do
    @behaviour Replicant.Sink
    def checkpoint, do: {:ok, nil}
    def handle_transaction(_txn), do: {:ok, 0}
    def handle_snapshot(_changes, _ctx), do: {:error, {:secret_value, "PII-LEAK"}}
    def snapshot_progress, do: {:ok, nil}
  end

  # handle_snapshot RAISES with a PII-bearing message — exercises apply_chunk's `rescue _ ->
  # :halt_sentinel` arm (the classic leak vector: a rescue that echoes the exception would leak
  # the message). The message MUST NOT reach the halt telemetry.
  defmodule RaiseChunkSink do
    @behaviour Replicant.Sink
    def checkpoint, do: {:ok, nil}
    def handle_transaction(_txn), do: {:ok, 0}
    def handle_snapshot(_changes, _ctx), do: raise("PII-RAISE-SECRET-abc123")
    def snapshot_progress, do: {:ok, nil}
  end

  # handle_snapshot THROWS a PII-bearing term — exercises apply_chunk's `catch _kind, _reason ->
  # :halt_sentinel` arm (throw/exit). The thrown term MUST NOT reach the halt telemetry.
  defmodule ThrowChunkSink do
    @behaviour Replicant.Sink
    def checkpoint, do: {:ok, nil}
    def handle_transaction(_txn), do: {:ok, 0}
    def handle_snapshot(_changes, _ctx), do: throw({:secret_value, "PII-THROW-xyz789"})
    def snapshot_progress, do: {:ok, nil}
  end

  # A sink-owned batch-delivery sink (handle_batch/1, no checkpoint_store) that ALSO backfills via
  # handle_snapshot/2 — the incremental × batch_delivery composition (spec §6/§7, Task 12). Reports
  # the flushed batch's LSNs AND every applied chunk's changes to the registered test process.
  defmodule BatchChunkSink do
    @behaviour Replicant.Sink
    def checkpoint, do: {:ok, nil}
    def snapshot_progress, do: {:ok, nil}
    def handle_transaction(_txn), do: {:ok, 0}

    def handle_batch(txns) do
      send(Process.whereis(:asrv_chunk_test), {:batch_call, Enum.map(txns, & &1.commit_lsn)})
      {:ok, List.last(txns).commit_lsn}
    end

    def handle_snapshot(changes, ctx) do
      send(Process.whereis(:asrv_chunk_test), {:snapshot_call, changes, ctx})
      :ok
    end
  end

  describe "incremental chunk path" do
    setup do
      Process.register(self(), :asrv_chunk_test)
      on_exit(fn -> nil end)
      :ok
    end

    defp start_incremental_server(sink, slot) do
      start_supervised!(
        {Replicant.AssemblerServer,
         slot_name: slot,
         sink: sink,
         snapshot_window: [chunk_rows: 10, max_pending_chunks: 2],
         max_inflight_lag: 1_000_000}
      )
    end

    defp chunk_msg(hw, ids, opts \\ []) do
      %{
        qualified: "public.orders",
        schema: "public",
        table: "orders",
        pk_raw: ["id"],
        pk_canon: Enum.map(ids, &[&1]),
        changes:
          Enum.map(ids, fn id ->
            %Replicant.Change{
              op: :snapshot,
              schema: "public",
              table: "orders",
              record: %{"id" => id}
            }
          end),
        hw: hw,
        first?: Keyword.get(opts, :first?, false),
        complete?: Keyword.get(opts, :complete?, false),
        progress: Keyword.get(opts, :progress, <<1>>),
        bound: nil
      }
    end

    # A PK-less chunk for "public.nopk": pk_raw == [] (no drop-set — convergence rests on redo).
    defp keyless_chunk_msg(hw, ids) do
      %{
        chunk_msg(hw, ids)
        | qualified: "public.nopk",
          schema: "public",
          table: "nopk",
          pk_raw: [],
          pk_canon: []
      }
    end

    # Fabricate a decoded Begin/Relation/Insert/Commit flow for `table`/`id` through the normal
    # message path (mirrors drive_txn/2 + relation/1; commit LSN 400 is below every test chunk HW,
    # so the explicit {:snapshot_frontier} cast closes the chunk).
    defp send_committed_txn(pid, table, id) do
      rel = %Relation{
        id: 1,
        namespace: "public",
        name: table,
        replica_identity: :default,
        columns: [col("id", "int4", [:key])]
      }

      cast(pid, %Begin{final_lsn: 400, commit_timestamp: ~U[2026-07-04 00:00:00Z], xid: 400}, 20)
      cast(pid, rel, 30)
      cast(pid, %Insert{relation_id: 1, tuple_data: {"#{id}"}}, 15)

      cast(
        pid,
        %Commit{lsn: 400, end_lsn: 400, commit_timestamp: ~U[2026-07-04 00:00:00Z], flags: []},
        8
      )
    end

    test "open_window -> deliver -> frontier closure applies the chunk with drop-filter + ctx keys" do
      pid = start_incremental_server(ChunkLedgerSink, "asrv_inc_1")
      assert {:ok, _} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.orders")
      assert :ok = Replicant.AssemblerServer.deliver_snapshot_chunk(pid, chunk_msg(500, [1, 2]))
      # not yet closed: no sink call
      refute_receive {:snapshot_call, _, _}, 100
      GenServer.cast(pid, {:snapshot_frontier, 0, 500})
      assert_receive {:snapshot_call, changes, ctx}, 1_000
      assert Enum.map(changes, & &1.record["id"]) == [1, 2]
      assert %{first_for_table?: false, backfill_complete?: false, progress: <<1>>} = ctx
      assert is_integer(ctx.snapshot_lsn)
    end

    test "TRIPWIRE: a txn applied after open_window drops the colliding chunk row" do
      pid = start_incremental_server(ChunkLedgerSink, "asrv_inc_2")
      assert {:ok, _} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.orders")
      # a committed txn for orders id=2 flows through the normal message path
      send_committed_txn(pid, "orders", 2)
      assert :ok = Replicant.AssemblerServer.deliver_snapshot_chunk(pid, chunk_msg(500, [1, 2]))
      GenServer.cast(pid, {:snapshot_frontier, 0, 500})
      assert_receive {:snapshot_call, changes, _ctx}, 1_000
      assert Enum.map(changes, & &1.record["id"]) == [1]
    end

    test "TRIPWIRE incremental×batch_delivery: a cap-tripping {:flush} txn drops the colliding chunk row" do
      # Sink-owned batch_delivery (batch:, no checkpoint_store) COMPOSED with an incremental window.
      # max_transactions: 1 makes the FIRST committed txn trip the count cap → the assembler returns
      # {:flush, :max_transactions, asm} DIRECTLY, skipping {:buffered}. The bug: dispatch({:flush})
      # tracked nothing, so the tripping txn's PK never entered the drop-set and a colliding chunk row
      # survived → stale-overwrite. The fix tracks at RECEIPT (buffered_changes = batch_txns head)
      # before do_flush resets the batch. RED without the fix: the chunk keeps [1, 2].
      pid =
        start_supervised!(
          {Replicant.AssemblerServer,
           slot_name: "asrv_inc_batch",
           sink: BatchChunkSink,
           batch: [max_transactions: 1, max_delay_ms: 60_000, max_span: 1_000_000],
           snapshot_window: [chunk_rows: 10, max_pending_chunks: 2],
           max_inflight_lag: 1_000_000}
        )

      assert {:ok, _} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.orders")

      # A committed txn for orders id=2 flows through the normal message path; at Commit the count cap
      # (1) trips → {:flush} → dispatch({:flush}). Receiving the flushed batch is a happens-after
      # barrier for the RECEIPT tracking, which (with the fix) strictly precedes do_flush→handle_batch.
      send_committed_txn(pid, "orders", 2)
      assert_receive {:batch_call, [400]}, 1_000

      assert :ok = Replicant.AssemblerServer.deliver_snapshot_chunk(pid, chunk_msg(500, [1, 2]))
      GenServer.cast(pid, {:snapshot_frontier, 0, 500})

      assert_receive {:snapshot_call, changes, _ctx}, 1_000

      # id=2 DROPPED because the {:flush} txn tracked it at receipt (RED → [1, 2] without the fix).
      assert Enum.map(changes, & &1.record["id"]) == [1]
    end

    test "TRIPWIRE value-free: a sink {:error, values} on a chunk halts WITHOUT the value leaking" do
      pid = start_incremental_server(FaultChunkSink, "asrv_inc_3")
      assert {:ok, _} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.orders")
      assert :ok = Replicant.AssemblerServer.deliver_snapshot_chunk(pid, chunk_msg(500, [1]))

      # Observe the halt via the telemetry channel (Supervisor.halt/2 discards its reason —
      # supervisor.ex:48) + the server's own halted flag; assert the event meta is the bare
      # allowlisted atom and the sink's PII payload appears nowhere in it.
      ref = make_ref()
      handler = fn event, _meas, meta, _cfg -> send(:asrv_chunk_test, {ref, event, meta}) end
      :telemetry.attach({__MODULE__, ref}, [:replicant, :snapshot, :failed], handler, nil)
      on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)

      GenServer.cast(pid, {:snapshot_frontier, 0, 500})
      assert_receive {^ref, [:replicant, :snapshot, :failed], meta}, 1_000
      assert meta == %{reason: :snapshot_failed}
      refute inspect(meta) =~ "PII-LEAK"
      assert :sys.get_state(pid).halted
    end

    test "backpressure: the 3rd deliver call blocks until a chunk applies (max_pending_chunks: 2)" do
      pid = start_incremental_server(ChunkLedgerSink, "asrv_inc_4")
      assert {:ok, _} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.orders")
      assert :ok = Replicant.AssemblerServer.deliver_snapshot_chunk(pid, chunk_msg(100, [1]))
      assert :ok = Replicant.AssemblerServer.deliver_snapshot_chunk(pid, chunk_msg(200, [2]))

      caller =
        Task.async(fn ->
          Replicant.AssemblerServer.deliver_snapshot_chunk(pid, chunk_msg(300, [3]))
        end)

      refute Task.yield(caller, 200)
      GenServer.cast(pid, {:snapshot_frontier, 0, 100})
      assert :ok = Task.await(caller, 1_000)
    end

    test "reconnect reset discards pending chunks and adopts the new epoch" do
      pid = start_incremental_server(ChunkLedgerSink, "asrv_inc_5")
      assert {:ok, _} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.orders")
      assert :ok = Replicant.AssemblerServer.deliver_snapshot_chunk(pid, chunk_msg(100, [1]))
      GenServer.cast(pid, {:reset_snapshot_window, 1})
      # a stale epoch-0 frontier cannot close anything; the chunk is gone anyway
      GenServer.cast(pid, {:snapshot_frontier, 0, 999_999})
      refute_receive {:snapshot_call, _, _}, 200
    end

    test "F-PACE: open_snapshot_window is NOT paced when the frontier gap from the floor is within max_inflight_lag/2" do
      # max_inflight_lag 1_000_000 → the gate is div(_, 2) = 500_000. Seed a large ABSOLUTE floor and
      # a frontier only 400_000 above it (WITHIN the gate), with last_applied still 0 (no commit yet).
      # The in-flight base MUST be max(last_applied, floor_lsn) = floor, so the estimate is
      # 400_000 < 500_000 → NOT paced → :ok. RED with the old `frontier - last_applied` base:
      # 1e9+400_000 − 0 ≫ 500_000 → paced → the call blocks forever (a fresh slot on an idle/low-write
      # DB could never open a window → backfill never starts).
      pid = start_incremental_server(ChunkLedgerSink, "asrv_pace_ok")
      floor = 1_000_000_000
      GenServer.cast(pid, {:snapshot_floor, floor})
      GenServer.cast(pid, {:snapshot_frontier, 0, floor + 400_000})
      :sys.get_state(pid)

      task =
        Task.async(fn -> Replicant.AssemblerServer.open_snapshot_window(pid, "public.orders") end)

      # Replies {:ok, epoch} within the window (GREEN). nil (still blocked) is the RED signal of the old base.
      assert {:ok, {:ok, _epoch}} = Task.yield(task, 1_000) || Task.shutdown(task)
    end

    test "F-PACE: open_snapshot_window IS still deferred when the frontier gap from the floor exceeds max_inflight_lag/2" do
      # Same absolute floor, but the frontier is 600_000 above it — OVER the 500_000 gate. The gate
      # still fires (stream drain has genuine priority) → the call is DEFERRED (no reply). This half
      # proves the fix did not defeat the pacing gate (the estimate is now floor-relative, not 0).
      pid = start_incremental_server(ChunkLedgerSink, "asrv_pace_defer")
      floor = 1_000_000_000
      GenServer.cast(pid, {:snapshot_floor, floor})
      GenServer.cast(pid, {:snapshot_frontier, 0, floor + 600_000})
      :sys.get_state(pid)

      task =
        Task.async(fn -> Replicant.AssemblerServer.open_snapshot_window(pid, "public.orders") end)

      refute Task.yield(task, 300)
      Task.shutdown(task, :brutal_kill)
    end

    test "F-TAINT lib+batch: a stream write to an UNTRACKED table leaves a cold backfill table's chunk intact (drop-filter no-op, not taint-all)" do
      pid = start_incremental_server(ChunkLedgerSink, "asrv_inc_taint")

      # Swap in a lib+batch assembler (mode: :lib + a batch policy) — start_incremental_server
      # otherwise builds a sink-owned one. Same inject pattern as the batched-checkpointing block; the
      # window lives on server STATE (untouched by the asm swap), and the sink stays ChunkLedgerSink so
      # handle_transaction (per-txn in lib+batch) AND handle_snapshot both resolve.
      :sys.replace_state(pid, fn st ->
        asm =
          Replicant.Assembler.new(ChunkLedgerSink,
            mode: :lib,
            checkpoint_writer: fn _lsn -> :ok end,
            slot_name: "asrv_inc_taint",
            lib_checkpoint: 0,
            batch: [max_transactions: 5, max_delay_ms: 60_000, max_span: 1_000_000],
            max_inflight_lag: 1_000_000
          )

        %{st | asm: asm}
      end)

      # Backfill public.cold: open its window + deliver a PENDING chunk (hw 500 > the hot txn's commit
      # LSN 400, so it stays pending until the explicit frontier advance closes it).
      assert {:ok, _} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.cold")

      cold_chunk = %{
        qualified: "public.cold",
        schema: "public",
        table: "cold",
        pk_raw: ["id"],
        pk_canon: [[1], [2]],
        changes:
          Enum.map([1, 2], fn id ->
            %Replicant.Change{
              op: :snapshot,
              schema: "public",
              table: "cold",
              record: %{"id" => id}
            }
          end),
        hw: 500,
        first?: false,
        complete?: false,
        progress: <<1>>,
        bound: nil
      }

      assert :ok = Replicant.AssemblerServer.deliver_snapshot_chunk(pid, cold_chunk)

      # A lib+batch stream txn writing ONLY public.hot (commit LSN 400): delivered per-txn, its
      # retained changes drop-filtered via track_capped. public.hot has NO open window → not in
      # w.tracking → a drop-filter NO-OP (no PK added, nothing tainted); public.cold's pending chunk
      # is untouched and survives. RED under the old taint-all path: public.cold would be tainted →
      # its chunk discarded and the snapshot_call never arrives.
      send_committed_txn(pid, "hot", 42)
      :sys.get_state(pid)

      # Close public.cold's chunk. If it survived, it applies now with BOTH rows.
      GenServer.cast(pid, {:snapshot_frontier, 0, 500})
      assert_receive {:snapshot_call, changes, ctx}, 1_000
      assert ctx.table == "public.cold"
      assert Enum.map(changes, & &1.record["id"]) == [1, 2]
    end

    test "F-DROP lib+batch: a stream write to a BACKFILLING table drop-filters the colliding chunk row (PK-retention, no taint)" do
      pid = start_incremental_server(ChunkLedgerSink, "asrv_inc_libbatch_drop")

      # Swap in a lib+batch assembler (mode: :lib + a batch policy) — same inject pattern as F-TAINT.
      :sys.replace_state(pid, fn st ->
        asm =
          Replicant.Assembler.new(ChunkLedgerSink,
            mode: :lib,
            checkpoint_writer: fn _lsn -> :ok end,
            slot_name: "asrv_inc_libbatch_drop",
            lib_checkpoint: 0,
            batch: [max_transactions: 5, max_delay_ms: 60_000, max_span: 1_000_000],
            max_inflight_lag: 1_000_000
          )

        %{st | asm: asm}
      end)

      assert {:ok, _} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.orders")

      # A lib+batch stream txn writes public.orders id=2 (delivered per-txn, checkpoint batched). With
      # PK-retention the buffered txn's changes are retained → track_window DROP-FILTERS: id=2 enters
      # the drop-set, exactly as lib-non-batch / sink-owned already do. RED before the refactor
      # (buffered_changes == :unavailable → track_window taints the affected table): public.orders is
      # tainted → the chunk deliver below returns {:error, :table_discarded}, never applies [1].
      send_committed_txn(pid, "orders", 2)
      :sys.get_state(pid)

      assert :ok = Replicant.AssemblerServer.deliver_snapshot_chunk(pid, chunk_msg(500, [1, 2]))
      GenServer.cast(pid, {:snapshot_frontier, 0, 500})

      # id=2 DROPPED (drop-filtered), id=1 survives — no taint, no re-read.
      assert_receive {:snapshot_call, changes, _ctx}, 1_000
      assert Enum.map(changes, & &1.record["id"]) == [1]
    end

    test "TRIPWIRE value-free: a sink handle_snapshot that RAISES halts WITHOUT the message leaking (rescue arm)" do
      pid = start_incremental_server(RaiseChunkSink, "asrv_inc_raise")
      assert {:ok, _} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.orders")
      assert :ok = Replicant.AssemblerServer.deliver_snapshot_chunk(pid, chunk_msg(500, [1]))

      ref = make_ref()
      handler = fn event, _meas, meta, _cfg -> send(:asrv_chunk_test, {ref, event, meta}) end
      :telemetry.attach({__MODULE__, ref}, [:replicant, :snapshot, :failed], handler, nil)
      on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)

      GenServer.cast(pid, {:snapshot_frontier, 0, 500})
      assert_receive {^ref, [:replicant, :snapshot, :failed], meta}, 1_000
      assert meta == %{reason: :snapshot_failed}
      refute inspect(meta) =~ "PII-RAISE-SECRET"
      assert :sys.get_state(pid).halted
    end

    test "TRIPWIRE value-free: a sink handle_snapshot that THROWS halts WITHOUT the term leaking (catch arm)" do
      pid = start_incremental_server(ThrowChunkSink, "asrv_inc_throw")
      assert {:ok, _} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.orders")
      assert :ok = Replicant.AssemblerServer.deliver_snapshot_chunk(pid, chunk_msg(500, [1]))

      ref = make_ref()
      handler = fn event, _meas, meta, _cfg -> send(:asrv_chunk_test, {ref, event, meta}) end
      :telemetry.attach({__MODULE__, ref}, [:replicant, :snapshot, :failed], handler, nil)
      on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)

      GenServer.cast(pid, {:snapshot_frontier, 0, 500})
      assert_receive {^ref, [:replicant, :snapshot, :failed], meta}, 1_000
      assert meta == %{reason: :snapshot_failed}
      refute inspect(meta) =~ "PII-THROW"
      assert :sys.get_state(pid).halted
    end

    test "ctx carries the non-zero snapshot floor and the first?/complete? chunk flags" do
      pid = start_incremental_server(ChunkLedgerSink, "asrv_inc_ctx")
      floor = 0xABCDEF
      GenServer.cast(pid, {:snapshot_floor, floor})
      assert {:ok, _} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.orders")

      assert :ok =
               Replicant.AssemblerServer.deliver_snapshot_chunk(
                 pid,
                 chunk_msg(500, [1], first?: true, complete?: true)
               )

      GenServer.cast(pid, {:snapshot_frontier, 0, 500})
      assert_receive {:snapshot_call, _changes, ctx}, 1_000
      assert ctx.snapshot_lsn == floor
      assert ctx.first_for_table? == true
      assert ctx.backfill_complete? == true
    end

    test "a tracked write to a PK-less table discards it → the reader's next deliver returns {:error, :table_discarded}" do
      pid = start_incremental_server(ChunkLedgerSink, "asrv_kl_disc")
      assert {:ok, _} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.nopk")

      # The first keyless chunk binds pk_raw == [] onto the tracking entry (stays pending, hw 500).
      assert :ok =
               Replicant.AssemblerServer.deliver_snapshot_chunk(
                 pid,
                 keyless_chunk_msg(500, [1, 2])
               )

      # A committed txn WRITES public.nopk → track_capped taints it (keyless contention).
      send_committed_txn(pid, "nopk", 9)
      :sys.get_state(pid)

      # RED without the fix: track_window ignored the discard, so this deliver buffers and returns :ok.
      assert {:error, :table_discarded} =
               Replicant.AssemblerServer.deliver_snapshot_chunk(
                 pid,
                 keyless_chunk_msg(600, [3, 4])
               )
    end

    test "a PK-less chunk whose table saw a write is DISCARDED at closure, never applied empty" do
      pid = start_incremental_server(ChunkLedgerSink, "asrv_kl_apply")
      assert {:ok, _} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.nopk")

      # A placeholder write BEFORE the first keyless chunk (tracked as {:record, _}, pk_raw nil —
      # NOT yet a taint). Its txn commits at 400 → the frontier advances to 400.
      send_committed_txn(pid, "nopk", 1)
      :sys.get_state(pid)

      # Deliver the keyless chunk: add_chunk binds pk_raw == [] and collapses the placeholder → the
      # tracking set is non-empty, so the chunk is STALE.
      assert :ok =
               Replicant.AssemblerServer.deliver_snapshot_chunk(
                 pid,
                 keyless_chunk_msg(500, [1, 2, 3])
               )

      GenServer.cast(pid, {:snapshot_frontier, 0, 500})

      # RED without the fix: pop_ready drop-filters the keyless chunk to empty and APPLIES it — a
      # {:snapshot_call, [], _} arrives (the batch silently lost). The fix DISCARDS it: no sink call.
      refute_receive {:snapshot_call, _, _}, 300
      # …and the table is now flagged, so the reader re-reads on its next deliver.
      assert {:error, :table_discarded} =
               Replicant.AssemblerServer.deliver_snapshot_chunk(pid, keyless_chunk_msg(600, [4]))
    end

    test "finish_snapshot_table replies :ok immediately when the table has no pending chunks" do
      pid = start_incremental_server(ChunkLedgerSink, "asrv_kl_barrier_ok")
      assert {:ok, epoch} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.nopk")
      assert :ok = Replicant.AssemblerServer.finish_snapshot_table(pid, "public.nopk", epoch)
    end

    test "finish_snapshot_table DEFERS while a chunk is pending, then replies :ok when it applies (barrier)" do
      pid = start_incremental_server(ChunkLedgerSink, "asrv_kl_barrier_defer")
      assert {:ok, epoch} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.nopk")

      assert :ok =
               Replicant.AssemblerServer.deliver_snapshot_chunk(
                 pid,
                 keyless_chunk_msg(500, [1, 2])
               )

      task =
        Task.async(fn ->
          Replicant.AssemblerServer.finish_snapshot_table(pid, "public.nopk", epoch)
        end)

      # Still pending (frontier 0 < hw 500) → the barrier BLOCKS.
      refute Task.yield(task, 200)

      # Close it → the uncontended chunk applies WHOLE → the barrier releases :ok.
      GenServer.cast(pid, {:snapshot_frontier, 0, 500})
      assert {:ok, :ok} = Task.yield(task, 1_000) || Task.shutdown(task)
      assert_received {:snapshot_call, changes, _ctx}
      assert length(changes) == 2
    end

    test "finish_snapshot_table replies {:error, :table_discarded} when a write discards the table mid-barrier" do
      pid = start_incremental_server(ChunkLedgerSink, "asrv_kl_barrier_disc")
      assert {:ok, epoch} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.nopk")

      assert :ok =
               Replicant.AssemblerServer.deliver_snapshot_chunk(
                 pid,
                 keyless_chunk_msg(500, [1, 2])
               )

      task =
        Task.async(fn ->
          Replicant.AssemblerServer.finish_snapshot_table(pid, "public.nopk", epoch)
        end)

      refute Task.yield(task, 150)

      # A concurrent write taints the still-pending table → the barrier releases {:error, :table_discarded}.
      send_committed_txn(pid, "nopk", 9)
      assert {:ok, {:error, :table_discarded}} = Task.yield(task, 1_000) || Task.shutdown(task)
    end

    test "finish_snapshot_table replies {:error, :window_reset} when a reconnect reset bumped the epoch since open (stale generation — never a spurious :ok)" do
      pid = start_incremental_server(ChunkLedgerSink, "asrv_kl_barrier_stale")
      assert {:ok, epoch} = Replicant.AssemblerServer.open_snapshot_window(pid, "public.nopk")

      # A reconnect re-seats the window under a NEW epoch, clearing tracking/pending/discarded — the
      # reader's provisional batches are WIPED, not applied. The barrier then sees a table that is
      # neither pending nor discarded, so WITHOUT the epoch guard it replies a spurious :ok and marks
      # a never-delivered table done = DATA LOSS. The reader's captured `epoch` is now stale.
      GenServer.cast(pid, {:reset_snapshot_window, epoch + 1})
      :sys.get_state(pid)

      # RED without the stale-generation check: the barrier ignores the epoch and replies :ok.
      assert {:error, :window_reset} =
               Replicant.AssemblerServer.finish_snapshot_table(pid, "public.nopk", epoch)
    end
  end
end

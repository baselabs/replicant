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

      # A leftover timer fires after the batch already flushed by the count cap. Here pending_lsn
      # is nil, so handle_info's `batch_pending?` guard short-circuits to the no-op branch and never
      # calls flush_batch — the stale timer is inert: no spurious ack, and the NEXT batch still
      # flushes correctly (state not corrupted). This test red-gates the `batch_pending?` guard
      # (removing it routes the stale timer into a flush); flush_batch's `:empty` clause is the
      # second layer, red-gated separately by the assembler "no open batch is :empty" test.
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
  end
end

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
end

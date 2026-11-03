defmodule Replicant.BackpressureRegressionTest do
  use ExUnit.Case, async: true

  alias Replicant.{Assembler, AssemblerServer}

  alias Replicant.Decoder.Messages.{
    Begin,
    Commit,
    Insert,
    Message,
    Relation,
    StreamAbort,
    StreamStart,
    StreamStop
  }

  alias Replicant.Decoder.Messages.Relation.Column

  defmodule Sink do
    @behaviour Replicant.Sink
    def checkpoint, do: {:ok, nil}
    def handle_transaction(txn), do: {:ok, txn.commit_lsn}
    def handle_batch(txns), do: {:ok, List.last(txns).commit_lsn}
  end

  defp relation do
    %Relation{
      id: 1,
      namespace: "public",
      name: "t",
      replica_identity: :default,
      columns: [%Column{name: "id", type: "int4", flags: [:key], type_modifier: -1}]
    }
  end

  defp handle(asm, msg, bytes) do
    {:ok, asm} = Assembler.handle_message(asm, msg)
    Assembler.observe_bytes(asm, bytes)
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "pressure_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)

    asm =
      Assembler.new(Sink,
        slot_name: "pressure",
        max_inflight_lag: 1000,
        spill: [dir: dir, max_spill_bytes: 1_000_000]
      )

    {:ok, asm} = Assembler.handle_message(asm, relation())
    %{asm: asm, dir: dir}
  end

  test "partial abort releases row bytes while top-level message bytes remain", %{asm: asm} do
    asm = handle(asm, %StreamStart{xid: 1, first_segment: true}, 0)
    asm = handle(asm, %Insert{xid: 2, relation_id: 1, tuple_data: {"2"}}, 800)

    asm =
      handle(
        asm,
        %Message{xid: 1, transactional?: true, prefix: "test", content: "retained"},
        200
      )

    {:ok, asm} = Assembler.handle_message(asm, %StreamAbort{xid: 1, subxid: 2})
    assert [%Message{xid: 1}] = asm.stream_txns[1].messages
    assert Assembler.buffered_bytes(asm) == 200
    asm = handle(asm, %Insert{xid: 1, relation_id: 1, tuple_data: {"1"}}, 800)
    assert asm.stream_txns[1].spill == nil
    Assembler.reset_streams(asm)
  end

  test "message-heavy stream cannot prevent spilling a different stream", %{asm: asm} do
    asm = handle(asm, %StreamStart{xid: 1, first_segment: true}, 0)

    asm =
      handle(
        asm,
        %Message{
          xid: 1,
          transactional?: true,
          prefix: "test",
          content: String.duplicate("x", 650)
        },
        700
      )

    {:ok, asm} = Assembler.handle_message(asm, %StreamStop{})
    asm = handle(asm, %StreamStart{xid: 2, first_segment: true}, 0)
    asm = handle(asm, %Insert{xid: 2, relation_id: 1, tuple_data: {"2"}}, 400)
    assert Assembler.buffered_bytes(asm) <= 1000
    assert asm.stream_txns[1].spill == nil
    assert asm.stream_txns[2].spill != nil
    Assembler.reset_streams(asm)
  end

  test "spill repeats until all reclaimable tails fit", %{asm: asm} do
    asm = %{asm | max_inflight_lag: 2000}
    asm = handle(asm, %StreamStart{xid: 1, first_segment: true}, 0)

    asm =
      handle(asm, %Message{xid: 1, transactional?: true, prefix: "test", content: "keep"}, 600)

    asm = handle(asm, %Insert{xid: 1, relation_id: 1, tuple_data: {"1"}}, 400)
    {:ok, asm} = Assembler.handle_message(asm, %StreamStop{})
    asm = handle(asm, %StreamStart{xid: 2, first_segment: true}, 0)
    asm = handle(asm, %Insert{xid: 2, relation_id: 1, tuple_data: {"2"}}, 350)
    {:ok, asm} = Assembler.Streaming.maybe_spill(%{asm | max_inflight_lag: 900})
    assert Assembler.buffered_bytes(asm) == 600
    assert asm.stream_txns[1].spill != nil
    assert asm.stream_txns[2].spill != nil
    Assembler.reset_streams(asm)
  end

  test "buffer-triggered flush never re-tracks an earlier receipt in a new window" do
    slot = "pressure_#{System.unique_integer([:positive])}"

    pid =
      start_supervised!(
        {AssemblerServer,
         slot_name: slot,
         sink: Sink,
         max_inflight_lag: 1000,
         batch: [max_transactions: 100, max_delay_ms: 60_000, max_span: 1_000_000],
         snapshot_window: [chunk_rows: 10, max_pending_chunks: 2]}
      )

    for msg <- [
          %Begin{final_lsn: 100, xid: 1},
          relation(),
          %Insert{relation_id: 1, tuple_data: {"1"}},
          %Commit{lsn: 100}
        ] do
      GenServer.cast(pid, {:message, msg, 10, self()})
    end

    asm = :sys.get_state(pid).asm
    assert asm.batch_bytes == Enum.reduce(asm.batch_txns, 0, &(:erlang.external_size(&1) + &2))
    assert asm.batch_bytes > 200
    assert Assembler.reset_batch(asm).batch_bytes == 0
    assert {:ok, 0} = AssemblerServer.open_snapshot_window(pid, "public.t")
    GenServer.cast(pid, {:message, %Begin{final_lsn: 200, xid: 2}, 800, self()})
    assert_receive {:sink_committed, 100}
    state = :sys.get_state(pid)
    refute state.halted
    assert state.asm.batch_bytes == 0
    assert state.asm.txn.byte_size == 800
    assert MapSet.size(state.window.tracking["public.t"].pks) == 0

    for msg <- [%Insert{relation_id: 1, tuple_data: {"2"}}, %Commit{lsn: 200}] do
      GenServer.cast(pid, {:message, msg, 10, self()})
    end

    state = :sys.get_state(pid)
    assert state.asm.batch_bytes > 0
    assert MapSet.size(state.window.tracking["public.t"].pks) == 1
  end

  test "non-streamed transaction retains spill-enabled compatibility allowance", %{dir: dir} do
    slot = "pressure_#{System.unique_integer([:positive])}"

    pid =
      start_supervised!(
        {AssemblerServer,
         slot_name: slot,
         sink: Sink,
         max_inflight_lag: 100,
         streaming: [spill: [dir: dir, max_spill_bytes: 1000]]}
      )

    for {msg, bytes} <- [
          {%Begin{final_lsn: 100, xid: 1}, 10},
          {relation(), 10},
          {%Insert{relation_id: 1, tuple_data: {"1"}}, 200},
          {%Commit{lsn: 100}, 10}
        ] do
      GenServer.cast(pid, {:message, msg, bytes, self()})
    end

    assert_receive {:sink_committed, 100}
    refute :sys.get_state(pid).halted
  end

  test "open streamed transaction does not starve an idle snapshot window" do
    slot = "pressure_#{System.unique_integer([:positive])}"

    pid =
      start_supervised!(
        {AssemblerServer,
         slot_name: slot,
         sink: Sink,
         max_inflight_lag: 1000,
         snapshot_window: [chunk_rows: 10, max_pending_chunks: 2]}
      )

    for {msg, bytes} <- [
          {%StreamStart{xid: 1, first_segment: true}, 0},
          {relation(), 0},
          {%Insert{xid: 1, relation_id: 1, tuple_data: {"1"}}, 600},
          {%StreamStop{}, 0}
        ] do
      GenServer.cast(pid, {:message, msg, bytes, self()})
    end

    assert {:ok, 0} = GenServer.call(pid, {:open_snapshot_window, "public.t"}, 1000)
  end
end

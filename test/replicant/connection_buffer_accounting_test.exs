defmodule Replicant.ConnectionBufferAccountingTest do
  use ExUnit.Case, async: false

  alias Replicant.Connection
  alias Replicant.Decoder.Wal2json

  test "payload bytes pause intake until the assembler drains without advancing the checkpoint" do
    slot = "buffer_pause_#{System.unique_integer([:positive])}"
    {:ok, _} = Registry.register(Replicant.Registry, {slot, :assembler}, nil)
    payload = <<?B, 2_000::64, 0::64, 7::32>>

    state = %Connection{
      slot_name: slot,
      checkpoint_lsn: 1_000,
      max_inflight_lag: byte_size(payload)
    }

    assert {:pause, state} = Connection.handle_data(frame(payload), state)
    assert_receive {:"$gen_cast", {:message, _, _, _, {epoch, bytes, messages}}}
    assert state.flow.paused
    assert state.checkpoint_lsn == 1_000

    assert {:noreply, unchanged} =
             Connection.handle_info({:assembler_processed, make_ref(), bytes, messages}, state)

    assert unchanged == state

    {_timer, token} = state.flow.timer

    assert {:noreply, [ack], state} =
             Connection.handle_info({:flow_feedback, epoch, token}, state)

    assert <<?r, 1_000::64, 1_000::64, 1_000::64, _::64, 0>> = IO.iodata_to_binary(ack)

    assert {:resume, state} =
             Connection.handle_info({:assembler_processed, epoch, bytes, messages}, state)

    assert state.flow.timer == nil
    refute state.flow.paused
    assert state.checkpoint_lsn == 1_000
    assert {:noreply, ^state} = Connection.handle_info({:flow_feedback, epoch, token}, state)
  end

  test "message count bounds a queue of tiny messages" do
    slot = "buffer_count_#{System.unique_integer([:positive])}"
    {:ok, _} = Registry.register(Replicant.Registry, {slot, :assembler}, nil)
    state = %Connection{slot_name: slot}
    payload = <<?B, 2_000::64, 0::64, 7::32>>

    state =
      Enum.reduce(1..511, state, fn _, state ->
        assert {:noreply, state} = Connection.handle_data(frame(payload), state)
        state
      end)

    assert {:pause, state} = Connection.handle_data(frame(payload), state)
    assert Replicant.FlowControl.queued_messages(state.flow) == 512
    assert {:noreply, state} = Connection.handle_disconnect(state)
    refute state.flow.paused
    assert Replicant.FlowControl.queued_messages(state.flow) == 0
  end

  test "a WAL segment gap is not buffered transaction data" do
    slot = "buffer_gap_#{System.unique_integer([:positive])}"
    {:ok, _} = Registry.register(Replicant.Registry, {slot, :assembler}, nil)

    state = %Connection{
      slot_name: slot,
      checkpoint_lsn: 1_000,
      checkpoint_state: :present,
      max_inflight_lag: 1_024
    }

    commit_lsn = 128 * 1_024 * 1_024
    begin_payload = <<?B, commit_lsn::64, 0::64, 7::32>>
    commit_payload = <<?C, 0::8, commit_lsn::64, commit_lsn + 1::64, 0::64>>

    assert {:noreply, state} =
             Connection.handle_data(
               <<?w, 1_000::64, 1_000::64, 0::64, begin_payload::binary>>,
               state
             )

    assert {:noreply, state} =
             Connection.handle_data(
               <<?w, commit_lsn::64, commit_lsn::64, 0::64, commit_payload::binary>>,
               state
             )

    assert state.checkpoint_lsn == 1_000
    assert_receive {:"$gen_cast", {:message, %Replicant.Decoder.Messages.Begin{}, _, _, _}}
    assert_receive {:"$gen_cast", {:message, %Replicant.Decoder.Messages.Commit{}, _, _, _}}
  end

  test "a pending sink commit keeps keepalive acknowledgement at the durable checkpoint" do
    slot = "buffer_pending_#{System.unique_integer([:positive])}"
    {:ok, _} = Registry.register(Replicant.Registry, {slot, :assembler}, nil)

    state = %Connection{
      slot_name: slot,
      checkpoint_lsn: 1_000,
      checkpoint_state: :present,
      in_txn: true,
      last_commit_lsn: 1_000,
      stream_floor_lsn: 1_000,
      max_inflight_lag: 1_024
    }

    commit_lsn = 128 * 1_024 * 1_024
    commit_payload = <<?C, 0::8, commit_lsn::64, commit_lsn + 1::64, 0::64>>

    assert {:noreply, state} =
             Connection.handle_data(
               <<?w, commit_lsn::64, commit_lsn::64, 0::64, commit_payload::binary>>,
               state
             )

    assert {:noreply, [ack], _state} =
             Connection.handle_data(<<?k, commit_lsn + 1::64, 0::64, 1::8>>, state)

    assert <<?r, 1_000::64, 1_000::64, 1_000::64, _clock::64, 0>> =
             IO.iodata_to_binary(ack)
  end

  test "a wal2json payload counts bytes once and credits both ordered messages" do
    slot = "buffer_plugin_#{System.unique_integer([:positive])}"
    {:ok, _} = Registry.register(Replicant.Registry, {slot, :assembler}, nil)

    payload =
      "test/fixtures/wal2json_pg12.jsons"
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Enum.find(&(Jason.decode!(&1)["action"] == "I"))

    bytes = byte_size(payload)
    state = %Connection{slot_name: slot, max_inflight_lag: bytes}
    decoder = %{state.decoder | kind: :wal2json, cache: Wal2json.init_cache([])}
    assert {:pause, state} = Connection.handle_data(frame(payload), %{state | decoder: decoder})

    assert_receive {:"$gen_cast",
                    {:message, %Replicant.Decoder.Messages.Relation{}, ^bytes, _,
                     {epoch, ^bytes, 1}}}

    assert_receive {:"$gen_cast",
                    {:message, %Replicant.Decoder.Messages.Insert{}, 0, _, {^epoch, ^bytes, 2}}}

    assert Replicant.FlowControl.queued_bytes(state.flow) == bytes
    assert Replicant.FlowControl.queued_messages(state.flow) == 2

    assert {:resume, state} =
             Connection.handle_info({:assembler_processed, epoch, bytes, 2}, state)

    assert Replicant.FlowControl.queued_bytes(state.flow) == 0
    assert Replicant.FlowControl.queued_messages(state.flow) == 0
  end

  defp frame(payload), do: <<?w, 1_000::64, 1_000::64, 0::64, payload::binary>>
end

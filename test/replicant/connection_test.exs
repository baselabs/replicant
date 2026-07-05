defmodule Replicant.ConnectionTest do
  use ExUnit.Case, async: false

  alias Replicant.Connection
  alias Replicant.Decoder.Messages.Begin

  defmodule StubSink do
    @behaviour Replicant.Sink
    @impl true
    def checkpoint, do: {:ok, 0x100}
    @impl true
    def handle_transaction(_txn), do: {:ok, 0}
  end

  defp state(overrides) do
    base = %Connection{
      slot_name: "conn_test",
      publication: "orders_pub",
      sink: StubSink,
      go_forward_only: false,
      checkpoint_lsn: 0,
      step: :streaming
    }

    struct(base, overrides)
  end

  # ---- the marquee exact-once seam ----

  describe "encode_status_update/1 + keepalive ack (the exact-once seam)" do
    test "encode_status_update reports the given LSN in all three positions, reply=0" do
      <<?r, write::64, flush::64, apply::64, _clock::64, reply::8>> =
        Connection.encode_status_update(0x16E3778)

      assert write == 0x16E3778
      assert flush == 0x16E3778
      assert apply == 0x16E3778
      assert reply == 0
    end

    test "a reply-requested keepalive acks the durable CHECKPOINT, never the received wal_end (fixes walex's wal_end+1)" do
      received_wal_end = 0x9999
      checkpoint = 0x1000
      keepalive = <<?k, received_wal_end::64, 0::64, 1::8>>

      {:noreply, [ack], _state} =
        Connection.handle_data(keepalive, state(checkpoint_lsn: checkpoint))

      <<?r, _write::64, flush::64, _apply::64, _clock::64, 0>> = IO.iodata_to_binary(ack)

      assert flush == checkpoint
      refute flush == received_wal_end
      refute flush == received_wal_end + 1
    end

    test "a keepalive that does NOT request a reply sends no ack" do
      keepalive = <<?k, 0x9999::64, 0::64, 0::8>>

      assert {:noreply, returned} =
               Connection.handle_data(keepalive, state(checkpoint_lsn: 0x1000))

      assert returned == state(checkpoint_lsn: 0x1000)
    end
  end

  # ---- async ack (monotonic) ----

  describe "handle_info({:sink_committed, lsn}) — async ack" do
    test "advances the checkpoint and acks the new flush position" do
      {:noreply, [ack], new_state} =
        Connection.handle_info({:sink_committed, 0x200}, state(checkpoint_lsn: 0x100))

      assert new_state.checkpoint_lsn == 0x200
      <<?r, _w::64, flush::64, _a::64, _c::64, 0>> = IO.iodata_to_binary(ack)
      assert flush == 0x200
    end

    test "never regresses the checkpoint on a stale/lower LSN" do
      {:noreply, [ack], new_state} =
        Connection.handle_info({:sink_committed, 0x50}, state(checkpoint_lsn: 0x100))

      assert new_state.checkpoint_lsn == 0x100
      <<?r, _w::64, flush::64, _a::64, _c::64, 0>> = IO.iodata_to_binary(ack)
      assert flush == 0x100
    end
  end

  # ---- XLogData decode-and-forward ----

  describe "handle_data(XLogData) — decode + forward" do
    test "decodes the payload and casts the decoded message to the AssemblerServer" do
      {:ok, _} = Registry.register(Replicant.Registry, {"conn_xlog", :assembler}, nil)
      begin_payload = <<"B", 0::64, 0::64, 7::32>>
      xlog = <<?w, 0::64, 0::64, 0::64, begin_payload::binary>>

      assert {:noreply, _state} = Connection.handle_data(xlog, state(slot_name: "conn_xlog"))
      assert_receive {:"$gen_cast", {:message, %Begin{xid: 7}, bytes, from}}
      assert bytes == byte_size(begin_payload)
      assert from == self()
    end

    test "a malformed/unknown payload halts fail-closed and forwards nothing (value-free)" do
      {:ok, _} = Registry.register(Replicant.Registry, {"conn_bad", :assembler}, nil)
      xlog = <<?w, 0::64, 0::64, 0::64, "Zbad">>

      assert {:disconnect, :decode_failure} =
               Connection.handle_data(xlog, state(slot_name: "conn_bad"))

      refute_received {:"$gen_cast", _}
    end
  end

  # ---- slot invalidation (fail-closed halt) ----

  describe "classify_slot_status/1 (PG16 wal_status + conflicting)" do
    test "an absent slot classifies :absent (first run → create)" do
      assert Connection.classify_slot_status([]) == :absent
    end

    test "a reserved, non-conflicting slot is :ok" do
      assert Connection.classify_slot_status([["reserved", false]]) == :ok
    end

    test "wal_status 'lost' is an invalidation (WAL removed → data gap)" do
      assert Connection.classify_slot_status([["lost", false]]) == {:invalidated, :wal_lost}
    end

    test "conflicting = true is an invalidation (standby recovery conflict)" do
      assert Connection.classify_slot_status([["reserved", true]]) == {:invalidated, :conflict}
    end
  end

  describe "handle_result(:invalidation_check)" do
    test "an invalidated slot halts fail-closed and never recreates the slot" do
      :telemetry.attach(
        {__MODULE__, :inval},
        [:replicant, :connection, :slot_invalidated],
        fn _e, _m, meta, pid -> send(pid, {:slot_invalidated, meta}) end,
        self()
      )

      result = [%Postgrex.Result{rows: [["lost", false]]}]

      assert {:disconnect, :slot_invalidated} =
               Connection.handle_result(result, state(step: :invalidation_check))

      assert_received {:slot_invalidated, %{reason: :wal_lost}}
      :telemetry.detach({__MODULE__, :inval})
    end

    test "an absent slot advances to create_slot" do
      result = [%Postgrex.Result{rows: []}]

      assert {:query, sql, new_state} =
               Connection.handle_result(result, state(step: :invalidation_check))

      assert sql =~ "CREATE_REPLICATION_SLOT conn_test"
      assert new_state.step == :create_slot
    end

    test "a valid slot streams from the durable checkpoint LSN" do
      result = [%Postgrex.Result{rows: [["reserved", false]]}]
      st = state(step: :invalidation_check, checkpoint_lsn: 0x16E3778)
      assert {:stream, sql, [], new_state} = Connection.handle_result(result, st)
      assert sql =~ "START_REPLICATION SLOT conn_test"
      assert sql =~ "0/16E3778"
      assert new_state.step == :streaming
    end
  end

  # ---- connect chain ----

  describe "handle_connect/1 + recovery telemetry" do
    test "reads the sink checkpoint and queries recovery status" do
      {:query, sql, new_state} =
        Connection.handle_connect(state(checkpoint_lsn: 0, step: :disconnected))

      assert sql == Replicant.QueryBuilder.is_in_recovery()
      assert new_state.checkpoint_lsn == 0x100
      assert new_state.step == :recovery_check
    end

    test "recovery_check emits [:connection, :connected] with the source kind" do
      :telemetry.attach(
        {__MODULE__, :conn},
        [:replicant, :connection, :connected],
        fn _e, _m, meta, pid -> send(pid, {:connected, meta}) end,
        self()
      )

      result = [%Postgrex.Result{rows: [[true]]}]

      assert {:query, _sql, new_state} =
               Connection.handle_result(result, state(step: :recovery_check))

      assert new_state.step == :invalidation_check
      assert_received {:connected, %{kind: :standby}}
      :telemetry.detach({__MODULE__, :conn})
    end
  end
end

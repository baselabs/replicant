defmodule Replicant.Test.FailIfCompleteSink do
  def handle_snapshot_complete(_lsn), do: raise("must not be called in lib mode")
end

defmodule Replicant.Test.OkCompleteSink do
  def handle_snapshot_complete(lsn), do: {:ok, lsn}
end

defmodule Replicant.ConnectionTest do
  use ExUnit.Case, async: false

  alias Replicant.Connection
  alias Replicant.Decoder.Messages.Begin
  alias Replicant.Decoder.Messages.{Commit, StreamAbort, StreamCommit, StreamStart}

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
      snapshot: false,
      connection: [hostname: "h", port: 5599, username: "postgres", database: "postgres"],
      checkpoint_lsn: 0,
      checkpoint_state: :empty,
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

    test "IDLE + reply-requested: advances to wal_end and updates checkpoint_lsn (A1)" do
      wal_end = 0x9999

      st =
        state(
          checkpoint_lsn: 0x1000,
          received_lsn: 0x1000,
          in_txn: false,
          last_commit_lsn: 0x1000
        )

      keepalive = <<?k, wal_end::64, 0::64, 1::8>>

      {:noreply, [ack], new_state} = Connection.handle_data(keepalive, st)
      <<?r, _write::64, flush::64, _apply::64, _clock::64, 0>> = IO.iodata_to_binary(ack)

      assert flush == wal_end
      assert new_state.checkpoint_lsn == wal_end
    end

    test "an idle-advance emits :checkpoint,:advanced tagged kind: :idle (A2 disambiguation)" do
      ref = make_ref()

      :telemetry.attach(
        {__MODULE__, ref},
        [:replicant, :checkpoint, :advanced],
        fn _e, _m, meta, pid -> send(pid, {:advanced, meta}) end,
        self()
      )

      on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)

      st =
        state(
          checkpoint_lsn: 0x1000,
          received_lsn: 0x1000,
          in_txn: false,
          last_commit_lsn: 0x1000
        )

      Connection.handle_data(<<?k, 0x9999::64, 0::64, 1::8>>, st)
      assert_received {:advanced, %{commit_lsn: 0x9999, kind: :idle}}
    end

    test "IDLE + no reply requested: VOLUNTEERS a status update acking wal_end (A1)" do
      wal_end = 0x9999

      st =
        state(
          checkpoint_lsn: 0x1000,
          received_lsn: 0x1000,
          in_txn: false,
          last_commit_lsn: 0x1000
        )

      keepalive = <<?k, wal_end::64, 0::64, 0::8>>

      {:noreply, [ack], new_state} = Connection.handle_data(keepalive, st)
      <<?r, _::64, flush::64, _::64, _::64, 0>> = IO.iodata_to_binary(ack)
      assert flush == wal_end
      assert new_state.checkpoint_lsn == wal_end
    end

    test "NOT idle (open transaction) + reply-requested: acks the durable checkpoint, never wal_end" do
      wal_end = 0x9999

      st =
        state(checkpoint_lsn: 0x1000, received_lsn: 0x1000, in_txn: true, last_commit_lsn: 0x1000)

      keepalive = <<?k, wal_end::64, 0::64, 1::8>>

      {:noreply, [ack], new_state} = Connection.handle_data(keepalive, st)
      <<?r, _::64, flush::64, _::64, _::64, 0>> = IO.iodata_to_binary(ack)
      assert flush == 0x1000
      assert new_state.checkpoint_lsn == 0x1000
    end

    test "NOT idle (an open STREAMED txn) + reply-requested: acks checkpoint even though in_txn is false (CV1/CV2)" do
      # in_txn is false (no non-streamed txn), but a concurrent streamed xid is still open — the
      # single-boolean bug would idle-ack past its un-durable data. open_streams must block it.
      st =
        state(
          checkpoint_lsn: 0x1000,
          received_lsn: 0x1000,
          in_txn: false,
          last_commit_lsn: 0x1000,
          open_streams: MapSet.new([200])
        )

      {:noreply, [ack], new_state} = Connection.handle_data(<<?k, 0x9999::64, 0::64, 1::8>>, st)
      <<?r, _::64, flush::64, _::64, _::64, 0>> = IO.iodata_to_binary(ack)
      assert flush == 0x1000
      assert new_state.checkpoint_lsn == 0x1000
    end

    test "NOT idle (unflushed batch: checkpoint < last_commit_lsn) + reply-requested: acks checkpoint" do
      wal_end = 0x9999

      st =
        state(
          checkpoint_lsn: 0x1000,
          received_lsn: 0x1000,
          in_txn: false,
          last_commit_lsn: 0x2000
        )

      keepalive = <<?k, wal_end::64, 0::64, 1::8>>

      {:noreply, [ack], new_state} = Connection.handle_data(keepalive, st)
      <<?r, _::64, flush::64, _::64, _::64, 0>> = IO.iodata_to_binary(ack)
      assert flush == 0x1000
      assert new_state.checkpoint_lsn == 0x1000
    end

    test "NOT idle + no reply requested: sends nothing" do
      st =
        state(checkpoint_lsn: 0x1000, received_lsn: 0x1000, in_txn: true, last_commit_lsn: 0x1000)

      keepalive = <<?k, 0x9999::64, 0::64, 0::8>>
      assert {:noreply, ^st} = Connection.handle_data(keepalive, st)
    end

    test "IDLE but wal_end <= checkpoint + no reply: no redundant send" do
      st =
        state(
          checkpoint_lsn: 0x9999,
          received_lsn: 0x9999,
          in_txn: false,
          last_commit_lsn: 0x9999
        )

      keepalive = <<?k, 0x1000::64, 0::64, 0::8>>
      assert {:noreply, ^st} = Connection.handle_data(keepalive, st)
    end

    test "ordering invariant (A1 §3.2): a Begin forwarded via forward_message blocks the next keepalive's idle-ack" do
      {:ok, _} = Registry.register(Replicant.Registry, {"conn_order", :assembler}, nil)
      begin_payload = <<"B", 0::64, 0::64, 7::32>>
      xlog = <<?w, 0::64, 0x2000::64, 0::64, begin_payload::binary>>

      st =
        state(
          slot_name: "conn_order",
          checkpoint_lsn: 0x1000,
          received_lsn: 0x1000,
          in_txn: false,
          last_commit_lsn: 0x1000,
          max_inflight_lag: 100_000_000
        )

      {:noreply, after_begin} = Connection.handle_data(xlog, st)
      assert after_begin.in_txn == true

      {:noreply, [ack], _} = Connection.handle_data(<<?k, 0x9999::64, 0::64, 1::8>>, after_begin)
      <<?r, _::64, flush::64, _::64, _::64, 0>> = IO.iodata_to_binary(ack)
      assert flush == 0x1000
    end

    test "advancing checkpoint on idle-ack keeps the §4 lag honest over a gap > max_inflight_lag (A1 §3.3)" do
      {:ok, _} = Registry.register(Replicant.Registry, {"conn_lag", :assembler}, nil)
      bound = 100
      wal_end = 1_000 + 10 * bound

      st =
        state(
          slot_name: "conn_lag",
          checkpoint_lsn: 1_000,
          received_lsn: 1_000,
          in_txn: false,
          last_commit_lsn: 1_000,
          stream_floor_lsn: 1_000,
          max_inflight_lag: bound
        )

      {:noreply, [_ack], advanced} = Connection.handle_data(<<?k, wal_end::64, 0::64, 1::8>>, st)
      assert advanced.checkpoint_lsn == wal_end

      begin_payload = <<"B", 0::64, 0::64, 7::32>>
      frame = <<?w, 0::64, wal_end + 10::64, 0::64, begin_payload::binary>>
      assert {:noreply, _} = Connection.handle_data(frame, advanced)
      assert_receive {:"$gen_cast", {:message, %Begin{xid: 7}, _b, _f}}
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

    test "the FIRST frame of a stream never false-halts — lag is measured from the stream floor, not 0" do
      {:ok, _} = Registry.register(Replicant.Registry, {"conn_first", :assembler}, nil)
      begin_payload = <<"B", 0::64, 0::64, 7::32>>
      # A fresh stream begins at a large absolute LSN while checkpoint is still 0; the
      # first frame must read lag 0 (floor := wal_end), never wal_end - 0.
      xlog = <<?w, 0::64, 50_000_000::64, 0::64, begin_payload::binary>>

      st =
        state(
          slot_name: "conn_first",
          checkpoint_lsn: 0,
          stream_floor_lsn: nil,
          max_inflight_lag: 100
        )

      assert {:noreply, new_state} = Connection.handle_data(xlog, st)
      assert new_state.received_lsn == 50_000_000
      assert new_state.stream_floor_lsn == 50_000_000
      assert_receive {:"$gen_cast", {:message, %Begin{xid: 7}, _bytes, _from}}
    end

    test "an XLogData whose lag over the stream floor is at/under the bound still forwards" do
      {:ok, _} = Registry.register(Replicant.Registry, {"conn_underbound", :assembler}, nil)
      begin_payload = <<"B", 0::64, 0::64, 7::32>>
      # floor pre-seeded at 1000; wal_end 1100 → lag 100 == bound (not OVER) → forwards.
      xlog = <<?w, 0::64, 1100::64, 0::64, begin_payload::binary>>

      st =
        state(
          slot_name: "conn_underbound",
          checkpoint_lsn: 0,
          stream_floor_lsn: 1000,
          max_inflight_lag: 100
        )

      assert {:noreply, new_state} = Connection.handle_data(xlog, st)
      assert new_state.received_lsn == 1100
      assert_receive {:"$gen_cast", {:message, %Begin{xid: 7}, _bytes, _from}}
    end
  end

  # ---- §4 bounded in-flight window / fail-closed sink-lag halt ----

  describe "handle_data(XLogData) — bounded in-flight window (spec §4)" do
    test "over-bound in-flight lag halts fail-closed with :sink_too_slow and forwards nothing" do
      :telemetry.attach(
        {__MODULE__, :too_slow},
        [:replicant, :connection, :disconnected],
        fn _e, meas, meta, pid -> send(pid, {:disc, meas, meta}) end,
        self()
      )

      {:ok, _} = Registry.register(Replicant.Registry, {"conn_slow", :assembler}, nil)
      begin_payload = <<"B", 0::64, 0::64, 7::32>>
      # floor 1000, wal_end 1201, checkpoint 0, bound 100 → lag 201 > 100 → halt.
      xlog = <<?w, 0::64, 1201::64, 0::64, begin_payload::binary>>

      st =
        state(
          slot_name: "conn_slow",
          checkpoint_lsn: 0,
          stream_floor_lsn: 1000,
          max_inflight_lag: 100
        )

      assert {:disconnect, :sink_too_slow} = Connection.handle_data(xlog, st)

      assert_received {:disc, %{lag: 201}, %{reason: :sink_too_slow}}
      refute_received {:"$gen_cast", _}
      :telemetry.detach({__MODULE__, :too_slow})
    end

    test "lag is measured against the durable checkpoint once it advances past the floor" do
      :telemetry.attach(
        {__MODULE__, :cp_floor},
        [:replicant, :connection, :disconnected],
        fn _e, meas, meta, pid -> send(pid, {:disc, meas, meta}) end,
        self()
      )

      {:ok, _} = Registry.register(Replicant.Registry, {"conn_cpfloor", :assembler}, nil)
      begin_payload = <<"B", 0::64, 0::64, 7::32>>
      # checkpoint has advanced to 5000 (a real commit LSN) above the 1000 floor;
      # wal_end 5150 → lag = 5150 - max(5000, 1000) = 150 > 100 → halt.
      xlog = <<?w, 0::64, 5150::64, 0::64, begin_payload::binary>>

      st =
        state(
          slot_name: "conn_cpfloor",
          checkpoint_lsn: 5000,
          stream_floor_lsn: 1000,
          max_inflight_lag: 100
        )

      assert {:disconnect, :sink_too_slow} = Connection.handle_data(xlog, st)
      assert_received {:disc, %{lag: 150}, %{reason: :sink_too_slow}}
      :telemetry.detach({__MODULE__, :cp_floor})
    end

    test "the high-water received_lsn advances monotonically and never regresses on a lower wal_end" do
      {:ok, _} = Registry.register(Replicant.Registry, {"conn_hw", :assembler}, nil)
      begin_payload = <<"B", 0::64, 0::64, 7::32>>

      st =
        state(
          slot_name: "conn_hw",
          checkpoint_lsn: 0,
          received_lsn: 500,
          stream_floor_lsn: 400,
          max_inflight_lag: 10_000
        )

      # A stale/reordered lower wal_end (50) must not drop the high-water below 500.
      xlog = <<?w, 0::64, 50::64, 0::64, begin_payload::binary>>

      assert {:noreply, new_state} = Connection.handle_data(xlog, st)
      assert new_state.received_lsn == 500
    end
  end

  # ---- slot invalidation (fail-closed halt) ----

  describe "lib_mode?/1 + lib_go_forward_violation?/1 (lib-mode connect helpers)" do
    test "lib_go_forward_violation?/1 fires only for an empty state-mirror without go_forward/snapshot" do
      base = %{
        checkpoint_state: :empty,
        sink: Replicant.Test.RecordingSink,
        go_forward_only: false,
        snapshot: false,
        checkpoint_store: [connection: []]
      }

      assert Replicant.Connection.lib_go_forward_violation?(base)
      refute Replicant.Connection.lib_go_forward_violation?(%{base | go_forward_only: true})
      refute Replicant.Connection.lib_go_forward_violation?(%{base | snapshot: true})
      refute Replicant.Connection.lib_go_forward_violation?(%{base | checkpoint_state: :present})
    end

    test "lib_mode?/1 reflects the presence of a :checkpoint_store" do
      assert Replicant.Connection.lib_mode?(%{checkpoint_store: [connection: []]})
      refute Replicant.Connection.lib_mode?(%{checkpoint_store: nil})
    end
  end

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

    test "PG17 invalidation_reason maps to a fixed atom (never String.to_atom)" do
      assert Connection.classify_slot_status([["reserved", false, "wal_removed", false]]) ==
               {:invalidated, :wal_lost}

      assert Connection.classify_slot_status([["reserved", false, "rows_removed", false]]) ==
               {:invalidated, :rows_removed}

      assert Connection.classify_slot_status([
               ["reserved", false, "wal_level_insufficient", false]
             ]) ==
               {:invalidated, :wal_level_insufficient}

      assert Connection.classify_slot_status([["reserved", false, "some_future_reason", false]]) ==
               {:invalidated, :invalidated}
    end

    test "PG17 healthy slot (no invalidation_reason) is :ok" do
      assert Connection.classify_slot_status([["reserved", false, nil, false]]) == :ok
      assert Connection.classify_slot_status([["reserved", false, "", false]]) == :ok
    end

    test "PG17 legacy signals still classify first" do
      assert Connection.classify_slot_status([["lost", false, nil, false]]) ==
               {:invalidated, :wal_lost}

      assert Connection.classify_slot_status([["reserved", true, nil, false]]) ==
               {:invalidated, :conflict}
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

    test "an absent slot with an EMPTY checkpoint advances to create_slot (first run / go-forward)" do
      result = [%Postgrex.Result{rows: []}]

      assert {:query, sql, new_state} =
               Connection.handle_result(
                 result,
                 state(step: :invalidation_check, checkpoint_lsn: 0)
               )

      assert sql =~ "CREATE_REPLICATION_SLOT conn_test"
      assert new_state.step == :create_slot
    end

    test "an absent slot with a NON-EMPTY checkpoint halts fail-closed (data gap — never silently recreate)" do
      # The sink has durable state (checkpoint > 0) but the slot is gone. Creating a
      # fresh slot would stream from its creation LSN, silently skipping the WAL
      # between the checkpoint and now (spec §8: never silently drop and recreate).
      :telemetry.attach(
        {__MODULE__, :data_gap},
        [:replicant, :connection, :slot_invalidated],
        fn _e, _m, meta, pid -> send(pid, {:data_gap, meta}) end,
        self()
      )

      result = [%Postgrex.Result{rows: []}]

      assert {:disconnect, :data_gap} =
               Connection.handle_result(
                 result,
                 state(step: :invalidation_check, checkpoint_lsn: 0x500)
               )

      # NEVER a {:query, CREATE_REPLICATION_SLOT ...} — the disconnect shape structurally
      # excludes slot recreation.
      assert_received {:data_gap, %{reason: :data_gap}}
      :telemetry.detach({__MODULE__, :data_gap})
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

  describe "handle_result(:invalidation_check) — snapshot-mode connect matrix (spec §8)" do
    test "absent slot + empty checkpoint + snapshot: true → creates the EXPORT_SNAPSHOT slot" do
      result = [%Postgrex.Result{rows: []}]

      st =
        state(
          step: :invalidation_check,
          snapshot: true,
          checkpoint_lsn: 0,
          checkpoint_state: :empty
        )

      assert {:query, sql, new_state} = Connection.handle_result(result, st)
      assert sql =~ "CREATE_REPLICATION_SLOT conn_test LOGICAL pgoutput EXPORT_SNAPSHOT"
      assert new_state.step == :create_export_slot
    end

    test "present slot + empty checkpoint + snapshot: true → halts :snapshot_incomplete (never auto-drops)" do
      result = [%Postgrex.Result{rows: [["reserved", false]]}]

      st =
        state(
          step: :invalidation_check,
          snapshot: true,
          checkpoint_lsn: 0,
          checkpoint_state: :empty
        )

      assert {:disconnect, :snapshot_incomplete} = Connection.handle_result(result, st)
    end

    test "checkpoint read fault + snapshot: true → halts :checkpoint_unreadable (fail-closed)" do
      for rows <- [[], [["reserved", false]]] do
        result = [%Postgrex.Result{rows: rows}]

        st =
          state(
            step: :invalidation_check,
            snapshot: true,
            checkpoint_lsn: 0,
            checkpoint_state: :fault
          )

        assert {:disconnect, :checkpoint_unreadable} = Connection.handle_result(result, st)
      end
    end

    test "present slot + present checkpoint + snapshot: true → resumes streaming (already bootstrapped)" do
      result = [%Postgrex.Result{rows: [["reserved", false]]}]

      st =
        state(
          step: :invalidation_check,
          snapshot: true,
          checkpoint_lsn: 0x16E3778,
          checkpoint_state: :present
        )

      assert {:stream, sql, [], new_state} = Connection.handle_result(result, st)
      assert sql =~ "START_REPLICATION SLOT conn_test"
      assert new_state.step == :streaming
    end

    test "NON-snapshot mode is unchanged: absent + empty → NOEXPORT create_slot" do
      result = [%Postgrex.Result{rows: []}]

      st =
        state(
          step: :invalidation_check,
          snapshot: false,
          checkpoint_lsn: 0,
          checkpoint_state: :empty
        )

      assert {:query, sql, new_state} = Connection.handle_result(result, st)
      assert sql =~ "NOEXPORT_SNAPSHOT"
      assert new_state.step == :create_slot
    end
  end

  describe "handle_result(:create_export_slot) + handle_info handoff" do
    test "captures consistent_point + snapshot_name, spawns the linked snapshotter, idles in :snapshotting" do
      # spawn_link links the snapshotter to THIS test process. It connects to the dummy
      # test host, fails inside its value-free boundary, and exits :normal (a :normal exit
      # never propagates over a link), so it cannot disturb the test.
      Process.flag(:trap_exit, true)

      # CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT result row order (probed):
      # [slot_name, consistent_point, snapshot_name, output_plugin].
      result = [
        %Postgrex.Result{rows: [["conn_test", "0/16E3778", "00000003-0000DD8A-1", "pgoutput"]]}
      ]

      st = state(step: :create_export_slot, snapshot: true)

      assert {:noreply, new_state} = Connection.handle_result(result, st)
      assert new_state.step == :snapshotting

      # Flush any stray {:EXIT, _, _} the linked snapshotter may have delivered.
      receive do
        {:EXIT, _, _} -> :ok
      after
        0 -> :ok
      end
    end

    test "{:snapshot_done, lsn} starts streaming from the handoff LSN" do
      st = state(step: :snapshotting, snapshot: true, checkpoint_lsn: 0)

      assert {:stream, sql, [], new_state} =
               Connection.handle_info({:snapshot_done, 0x16E3778}, st)

      assert sql =~ "START_REPLICATION SLOT conn_test"
      assert sql =~ "0/16E3778"
      assert new_state.step == :streaming
      assert new_state.checkpoint_lsn == 0x16E3778
    end

    test "{:snapshot_failed, error} halts the pipeline fail-closed" do
      st = state(step: :snapshotting, snapshot: true)
      err = %Replicant.Error{reason: :snapshot_failed}
      assert {:disconnect, :snapshot_failed} = Connection.handle_info({:snapshot_failed, err}, st)
    end

    test "the lib-mode snapshot handoff does NOT call the sink's handle_snapshot_complete/1" do
      # Lib mode returns {:ok, cp} WITHOUT invoking handle_snapshot_complete/1 (the Connection
      # writes the store handoff instead); sink-owned mode still calls it.
      assert {:ok, 77} =
               Replicant.Snapshotter.complete_for_test(
                 Replicant.Test.FailIfCompleteSink,
                 77,
                 :lib
               )

      assert {:ok, 77} =
               Replicant.Snapshotter.complete_for_test(
                 Replicant.Test.OkCompleteSink,
                 77,
                 :sink_owned
               )
    end
  end

  # ---- store-fault paced retry (spec §4/§7/§9) ----

  describe "read_checkpoint_result/1 + store_retry_step/1 + reset_retry_count/2" do
    test "read_checkpoint_result maps store returns to connect states (permanent vs transient)" do
      # The lib clause of read_checkpoint delegates the store return → connect-state mapping to
      # this @doc false pure function, so it is unit-testable without a live store.
      perm = {:error, %Replicant.Error{reason: :checkpoint_store_schema_mismatch}}
      trans = {:error, %Replicant.Error{reason: :checkpoint_store_failed}}
      assert Replicant.Connection.read_checkpoint_result(perm) == {:fault_permanent, 0}
      assert Replicant.Connection.read_checkpoint_result(trans) == {:fault, 0}
      assert Replicant.Connection.read_checkpoint_result({:ok, 42}) == {:present, 42}
      assert Replicant.Connection.read_checkpoint_result({:ok, nil}) == {:empty, 0}
    end

    test "store_retry_step/1 retries while count < max, halts at the bound, and is a no-op on 0-max" do
      base = %{
        slot_name: "rep_step",
        store_retry_count: 0,
        checkpoint_store: [connection: [], max_retries: 2, retry_backoff_ms: 5]
      }

      assert {:retry, %{store_retry_count: 1}} = Replicant.Connection.store_retry_step(base)

      assert {:retry, %{store_retry_count: 2}} =
               Replicant.Connection.store_retry_step(%{base | store_retry_count: 1})

      assert :halt = Replicant.Connection.store_retry_step(%{base | store_retry_count: 2})

      zero = %{base | checkpoint_store: [connection: [], max_retries: 0, retry_backoff_ms: 5]}
      assert :halt = Replicant.Connection.store_retry_step(zero)
    end

    test "reset_retry_count keeps the count across a fault, resets to 0 on a successful read (self-heal)" do
      # The connect-read self-heal: a transient fault accumulates the counter, then a recovered
      # read resets it so a later separate outage starts fresh.
      assert Replicant.Connection.reset_retry_count(3, :fault) == 3
      assert Replicant.Connection.reset_retry_count(3, :fault_permanent) == 3
      assert Replicant.Connection.reset_retry_count(3, :present) == 0
      assert Replicant.Connection.reset_retry_count(3, :empty) == 0
    end

    test "handle_info(:store_retry_reconnect) disconnects a LIVE retry (count > 0) but ignores a STALE timer (count 0)" do
      # A live paced-retry timer (still mid-fault: store_retry_count > 0) disconnects so the
      # framework re-runs the connect chain and re-reads the store.
      live = %{slot_name: "rep_timer", store_retry_count: 1}

      assert {:disconnect, :checkpoint_store_retry} =
               Replicant.Connection.handle_info(:store_retry_reconnect, live)

      # A STALE timer: an independent replication-connection reconnect during the backoff
      # window already re-read the (recovered) store, which reset store_retry_count to 0 and
      # resumed streaming. The orphaned timer must be a no-op — disconnecting a recovered
      # stream would be a spurious blip (cross-vendor closeout finding).
      stale = %{slot_name: "rep_timer", store_retry_count: 0}
      assert {:noreply, ^stale} = Replicant.Connection.handle_info(:store_retry_reconnect, stale)
    end
  end

  # ---- read_progress/1 sink-mode value-free boundary (spec §6.2/§9) ----
  #
  # In sink-owned incremental mode, read_progress/1 calls the sink's snapshot_progress/0 behind a
  # value-free rescue AND catch: a raise OR a throw is scrubbed to the bare
  # {:error, :snapshot_progress_read_fault}, which classify_progress maps to :fault (fail-closed
  # halt), never leaking the exception's PII-bearing payload. These stubs carry a PII marker in
  # the raised/thrown value; the tests prove it is NOT present in the scrubbed result.

  defmodule RaisingProgressSink do
    @pii "PII_SSN_078051120_from_raise"
    def pii, do: @pii
    def snapshot_progress, do: raise("progress read blew up: #{@pii}")
  end

  defmodule ThrowingProgressSink do
    @pii "PII_TOKEN_xyz789_from_throw"
    def pii, do: @pii
    def snapshot_progress, do: throw({:leaked, @pii})
  end

  describe "read_progress/1 — sink-mode value-free rescue/catch (spec §6.2/§9)" do
    test "a sink snapshot_progress/0 that RAISES a PII-bearing error → :fault, value-free" do
      # RED PROOF: without the `rescue` arm the raise propagates and ERRORS this test (it never
      # reaches the assert), so a green match proves the value-free boundary caught it.
      result = Connection.read_progress(%{sink: RaisingProgressSink})

      assert result == {:error, :snapshot_progress_read_fault}
      assert Connection.classify_progress(result) == :fault
      refute inspect(result) =~ RaisingProgressSink.pii()
    end

    test "a sink snapshot_progress/0 that THROWS a PII-bearing term → :fault, value-free" do
      # RED PROOF: without the `catch` arm the throw propagates as an (uncaught throw) exit and
      # ERRORS this test, so a green match proves the catch arm scrubbed it.
      result = Connection.read_progress(%{sink: ThrowingProgressSink})

      assert result == {:error, :snapshot_progress_read_fault}
      assert Connection.classify_progress(result) == :fault
      refute inspect(result) =~ ThrowingProgressSink.pii()
    end
  end

  # ---- connection opts precedence ----

  describe "connection_opts/1 — library control opts win over caller :connection" do
    test "name/sync_connect/auto_reconnect override caller-supplied same keys" do
      config = %{
        slot_name: "conn_test",
        connection: [
          hostname: "h",
          port: 5432,
          # A caller passing these must NOT be able to break the facade contract:
          sync_connect: true,
          auto_reconnect: false,
          name: :caller_chosen_name
        ]
      }

      opts = Connection.connection_opts(config)

      assert Keyword.get(opts, :sync_connect) == false
      assert Keyword.get(opts, :auto_reconnect) == true
      assert Keyword.get(opts, :name) == Connection.via("conn_test")
      assert Keyword.get(opts, :hostname) == "h"
      # No stale duplicate of an overridden key survives.
      assert Enum.count(opts, fn {k, _} -> k == :sync_connect end) == 1
      assert Enum.count(opts, fn {k, _} -> k == :name end) == 1
    end
  end

  # ---- connect chain ----

  describe "handle_connect/1 + recovery telemetry" do
    test "reads the sink checkpoint and queries recovery status" do
      {:query, sql, new_state} =
        Connection.handle_connect(state(checkpoint_lsn: 0, step: :disconnected))

      assert sql == Replicant.QueryBuilder.recovery_and_version()
      assert new_state.checkpoint_lsn == 0x100
      assert new_state.step == :recovery_check
    end

    test "a reconnect resets the spilled_bytes mirror to 0 (never carries a stale-high value)" do
      # init/1 runs ONCE; RECONNECTS re-enter through handle_connect/1. The assembler zeroes its
      # spilled_total on reconnect (reset_streams), but the {:spilled_bytes,_} signal only fires on
      # a CHANGE during message observation — never on reset. So handle_connect MUST re-zero the
      # Connection's mirror, or a stale-high spilled_bytes over-subtracts the §4 numerator and a
      # slow-sink reconnect that does not re-spill evades the RAM-bound halt indefinitely.
      {:query, _sql, new_state} =
        Connection.handle_connect(state(spilled_bytes: 4242, step: :disconnected))

      assert new_state.spilled_bytes == 0
    end

    test "recovery_check emits [:connection, :connected] with the source kind" do
      :telemetry.attach(
        {__MODULE__, :conn},
        [:replicant, :connection, :connected],
        fn _e, _m, meta, pid -> send(pid, {:connected, meta}) end,
        self()
      )

      result = [%Postgrex.Result{rows: [[true, 170_010]]}]

      assert {:query, _sql, new_state} =
               Connection.handle_result(result, state(step: :recovery_check))

      assert new_state.step == :invalidation_check
      assert new_state.server_version_num == 170_010
      assert new_state.in_recovery == true
      assert_received {:connected, %{kind: :standby}}
      :telemetry.detach({__MODULE__, :conn})
    end

    test "failover on PG16 halts fail-closed and STAYS IDLE (never disconnects → no reconnect spin)" do
      :telemetry.attach(
        {__MODULE__, :failover_unsup},
        [:replicant, :connection, :slot_invalidated],
        fn _e, _m, meta, pid -> send(pid, {:failover_unsup, meta}) end,
        self()
      )

      result = [%Postgrex.Result{rows: [[false, 160_014]]}]
      st = state(step: :recovery_check, failover: true)

      # STAY IDLE: {:noreply, _} — NOT {:disconnect, _} (mirrors halt_store_permanent; a
      # disconnect would let auto_reconnect re-read the unchangeable version and re-halt in a spin).
      assert {:noreply, new_state} = Connection.handle_result(result, st)
      assert new_state.server_version_num == 160_014
      assert_received {:failover_unsup, %{reason: :failover_unsupported}}
      :telemetry.detach({__MODULE__, :failover_unsup})
    end

    test "failover on PG17 proceeds to the invalidation check" do
      result = [%Postgrex.Result{rows: [[false, 170_010]]}]
      st = state(step: :recovery_check, failover: true)
      assert {:query, sql, new_state} = Connection.handle_result(result, st)
      assert sql =~ "pg_replication_slots"
      assert new_state.step == :invalidation_check
    end

    test "no failover on PG16 proceeds normally (unchanged)" do
      result = [%Postgrex.Result{rows: [[false, 160_014]]}]
      st = state(step: :recovery_check, failover: false)
      assert {:query, _sql, new_state} = Connection.handle_result(result, st)
      assert new_state.step == :invalidation_check
    end
  end

  describe "sink-owned batch delivery casts (spec §6/§9)" do
    # Register the TEST process as the assembler under {slot, :assembler} — the connection casts
    # to AssemblerServer.via(slot), so casts arrive as {:"$gen_cast", msg} in the test mailbox.
    # This is the file's existing pattern (connection_test.exs:101-108) — no stub GenServer.
    defp bd_state(slot, step) do
      %Replicant.Connection{
        slot_name: slot,
        publication: "p",
        sink: Replicant.Test.RecordingSink,
        connection: [hostname: "h"],
        checkpoint_store: nil,
        batch_delivery: [max_transactions: 10, max_delay_ms: 1000, max_span: 1_000_000],
        checkpoint_lsn: 0,
        received_lsn: 0,
        stream_floor_lsn: nil,
        step: step
      }
    end

    test "the first XLogData frame casts {:stream_floor} to the assembler in sink-owned batch mode" do
      {:ok, _} = Registry.register(Replicant.Registry, {"conn_bd_floor", :assembler}, nil)
      # A DECODABLE Begin payload (connection_test.exs:102) so forward_message succeeds and does
      # NOT halt on a decode failure; wal_end = 999 is the stream floor.
      begin_payload = <<"B", 0::64, 0::64, 7::32>>
      frame = <<?w, 0::64, 999::64, 0::64, begin_payload::binary>>

      assert {:noreply, _state} =
               Replicant.Connection.handle_data(frame, bd_state("conn_bd_floor", :streaming))

      # {:stream_floor} is cast BEFORE forward_message (connection.ex:281-283), so it arrives.
      assert_receive {:"$gen_cast", {:stream_floor, 999}}
    end

    test "start_streaming casts {:reset_batch} in sink-owned batch mode" do
      {:ok, _} = Registry.register(Replicant.Registry, {"conn_bd_reset", :assembler}, nil)
      # handle_result([%Postgrex.Result{}], %{step: :create_slot}) routes to start_streaming
      # (connection.ex:226-229), which primes the assembler → {:reset_batch} for sink-owned batch.
      # The :create_slot handle_result clause matches on the struct type alone (connection.ex:226).
      assert {:stream, _sql, [], _state} =
               Replicant.Connection.handle_result(
                 [%Postgrex.Result{}],
                 bd_state("conn_bd_reset", :create_slot)
               )

      assert_receive {:"$gen_cast", {:reset_batch}}
    end
  end

  describe "streaming casts + in-stream tracking (spec §5/§9)" do
    alias Replicant.Decoder.Messages.{StreamStart, StreamStop}

    defp st_state(slot, step, in_stream \\ false) do
      %Replicant.Connection{
        slot_name: slot,
        publication: "p",
        sink: Replicant.Test.RecordingSink,
        connection: [hostname: "h"],
        checkpoint_store: nil,
        batch_delivery: nil,
        streaming: [max_concurrent_txns: 64],
        checkpoint_lsn: 0,
        received_lsn: 0,
        stream_floor_lsn: nil,
        in_stream: in_stream,
        step: step
      }
    end

    test "a StreamStart frame is forwarded and flips in_stream true; StreamStop flips it back" do
      {:ok, _} = Registry.register(Replicant.Registry, {"conn_st", :assembler}, nil)
      # ?w frame wrapping a StreamStart payload (S <xid::32> <first::8>)
      start_frame = <<?w, 0::64, 10::64, 0::64, "S", 100::32, 1::8>>

      assert {:noreply, s1} =
               Replicant.Connection.handle_data(start_frame, st_state("conn_st", :streaming))

      assert_receive {:"$gen_cast", {:message, %StreamStart{xid: 100}, _, _}}
      assert s1.in_stream == true

      stop_frame = <<?w, 0::64, 11::64, 0::64, "E">>
      assert {:noreply, s2} = Replicant.Connection.handle_data(stop_frame, s1)
      assert_receive {:"$gen_cast", {:message, %StreamStop{}, _, _}}
      assert s2.in_stream == false
    end

    test "start_streaming casts {:reset_streams} and resets in_stream in streaming mode" do
      {:ok, _} = Registry.register(Replicant.Registry, {"conn_st2", :assembler}, nil)

      assert {:stream, sql, [], state} =
               Replicant.Connection.handle_result([%Postgrex.Result{}], %{
                 st_state("conn_st2", :create_slot, true)
                 | step: :create_slot
               })

      assert sql =~ "proto_version '2'"
      assert_receive {:"$gen_cast", {:reset_streams}}
      assert state.in_stream == false
    end

    test "the snapshot handoff resets a STALE in_stream, casts {:reset_streams}, and negotiates proto-v2" do
      {:ok, _} = Registry.register(Replicant.Registry, {"conn_st3", :assembler}, nil)
      # State at the snapshot-handoff point after a mid-stream reconnect during the snapshot:
      # in_stream is STALE true. The second (re)connect entry point must reset it symmetrically.
      st = %{st_state("conn_st3", :snapshotting, true) | snapshot: true, checkpoint_lsn: 0}

      assert {:stream, sql, [], state} =
               Replicant.Connection.handle_info({:snapshot_done, 0x16E3778}, st)

      assert sql =~ "proto_version '2'"
      assert_receive {:"$gen_cast", {:reset_streams}}
      assert state.in_stream == false
    end
  end

  describe "spill in-flight-lag accounting (spec §4/§9)" do
    defp sp_state(slot, extra \\ %{}) do
      Map.merge(
        %Replicant.Connection{
          slot_name: slot,
          publication: "p",
          sink: Replicant.Test.RecordingSink,
          connection: [hostname: "h"],
          checkpoint_store: nil,
          batch_delivery: nil,
          streaming: [max_concurrent_txns: 64, spill: [dir: "/tmp/x", max_spill_bytes: 500]],
          checkpoint_lsn: 0,
          received_lsn: 0,
          stream_floor_lsn: 0,
          in_stream: false,
          spilled_bytes: 0,
          max_spill_bytes: 500,
          max_inflight_lag: 100,
          step: :streaming
        },
        extra
      )
    end

    test "the halt ceiling is max_inflight_lag + max_spill_bytes (RAM + disk)" do
      # received-floor 620, spilled 0 → resident lag 620 > 100 + 500 = 600 → halt
      s = %{sp_state("c2") | received_lsn: 620, spilled_bytes: 0}
      frame = <<?w, 0::64, 620::64, 0::64, "E">>
      assert {:disconnect, :sink_too_slow} = Replicant.Connection.handle_data(frame, s)
    end

    test "spilled bytes lower the numerator so the SAME frame that halts at spilled=0 forwards when spilled is high" do
      # WITHOUT the -spilled subtraction, received-floor 620 halts; WITH spilled=550,
      # 620-550=70 < 600 → no halt (forwards)
      {:ok, _} = Registry.register(Replicant.Registry, {"c2b", :assembler}, nil)
      s = %{sp_state("c2b") | received_lsn: 620, spilled_bytes: 550}
      frame = <<?w, 0::64, 620::64, 0::64, "E">>
      refute match?({:disconnect, :sink_too_slow}, Replicant.Connection.handle_data(frame, s))
    end

    test "a {:spilled_bytes, total} message updates the connection's spilled counter (handled, not swallowed by the catch-all)" do
      s = sp_state("c3")
      assert {:noreply, s2} = Replicant.Connection.handle_info({:spilled_bytes, 400}, s)
      assert s2.spilled_bytes == 400
    end

    test "a NON-spill connection (max_spill_bytes: nil) halts at EXACTLY max_inflight_lag (ceiling unchanged)" do
      # No spill config → max_spill_bytes nil → effective_lag_bound returns the base
      # max_inflight_lag verbatim (byte-identical to the pre-task §4 halt for existing users).
      {:ok, _} = Registry.register(Replicant.Registry, {"c4", :assembler}, nil)

      # floor 500, bound 100 → wal_end 601 gives lag 101 > 100 → halt.
      halt_state = state(slot_name: "c4", stream_floor_lsn: 500, max_inflight_lag: 100)
      halt_frame = <<?w, 0::64, 601::64, 0::64, "E">>
      assert {:disconnect, :sink_too_slow} = Connection.handle_data(halt_frame, halt_state)

      # wal_end 600 gives lag 100 == bound (not OVER) → forwards.
      fwd_state = state(slot_name: "c4", stream_floor_lsn: 500, max_inflight_lag: 100)
      fwd_frame = <<?w, 0::64, 600::64, 0::64, "E">>
      assert {:noreply, _} = Connection.handle_data(fwd_frame, fwd_state)
    end
  end

  describe "incremental snapshot helpers" do
    test "incremental?/1 discriminates the widened snapshot field" do
      assert Replicant.Connection.incremental?(%{snapshot: [mode: :incremental]})
      refute Replicant.Connection.incremental?(%{snapshot: true})
      refute Replicant.Connection.incremental?(%{snapshot: false})
    end

    test "progress classification: token decodes drive the §8 matrix rows" do
      complete =
        Replicant.SnapshotProgress.new([], 1) |> Replicant.SnapshotProgress.mark_complete()

      inflight =
        Replicant.SnapshotProgress.new(
          [%{qualified: "q", schema: "s", table: "t", pk_raw: [], pk_quoted: []}],
          1
        )

      assert :complete =
               Replicant.Connection.classify_progress(
                 {:ok, Replicant.SnapshotProgress.encode(complete)}
               )

      assert {:in_flight, _} =
               Replicant.Connection.classify_progress(
                 {:ok, Replicant.SnapshotProgress.encode(inflight)}
               )

      assert :none = Replicant.Connection.classify_progress({:ok, nil})

      assert :fault =
               Replicant.Connection.classify_progress(
                 {:error, %Replicant.Error{reason: :checkpoint_store_failed}}
               )

      assert :fault = Replicant.Connection.classify_progress({:ok, <<131, 0, 0>>})
    end
  end

  # ---- exactly-one-reader on reconnect ([[replicant-otp-async-lifetime-hygiene]]) ----
  #
  # The incremental chunk reader is spawn_link'ed to the Connection, but handle_disconnect keeps
  # the Connection alive across an auto_reconnect cycle, so the link never fires and the old reader
  # survives. start_streaming_with_backfill therefore retires the prior reader (via this seam)
  # BEFORE respawning, so exactly one reader ever backfills a slot. This unit test proves the
  # retire seam; the full reconnect→respawn→exactly-one-reader chain is Task 11's marquee (see the
  # requirement noted in the closeout).
  describe "retire_reader/1 — retire the prior reader before a reconnect respawns a fresh one" do
    test "unlinks THEN kills a live prior reader (dead after; NO EXIT fault reaches the Connection)" do
      # Trap exits so a still-linked kill (the bug) would deliver {:EXIT, dummy, :killed} HERE —
      # this test process stands in for the Connection that spawn_link'ed the reader.
      Process.flag(:trap_exit, true)

      # A dummy long-lived reader, spawn_LINKED to this process exactly as Incremental.start links
      # the real reader to the Connection.
      dummy = spawn_link(fn -> Process.sleep(:infinity) end)
      assert Process.alive?(dummy)

      ref = Process.monitor(dummy)
      returned = Connection.retire_reader(state(reader_pid: dummy))

      # Exactly-one-reader: the prior reader is deterministically killed (reason :killed from the
      # untrappable Process.exit/2 :kill). RED PROOF: a no-op retire_reader (`def r(state), do: state`)
      # never kills the dummy, so this assert_receive times out AND `refute Process.alive?` fails.
      assert_receive {:DOWN, ^ref, :process, ^dummy, :killed}
      refute Process.alive?(dummy)

      # reader_pid cleared so start_streaming_with_backfill can thread the fresh reader in.
      assert returned.reader_pid == nil

      # UNLINK-before-KILL: no EXIT signal reached this process. A retire that killed while still
      # linked would deliver {:EXIT, ^dummy, :killed} here and fault the real Connection.
      refute_receive {:EXIT, ^dummy, _reason}
    end

    test "a nil or already-dead reader_pid is a no-op (never crashes, state unchanged)" do
      nil_state = state(reader_pid: nil)
      assert Connection.retire_reader(nil_state) == nil_state

      dead = spawn(fn -> :ok end)
      ref = Process.monitor(dead)
      assert_receive {:DOWN, ^ref, :process, ^dead, _}
      refute Process.alive?(dead)

      # A dead pid takes the live-pid clause but the Process.alive?/1 guard makes it a no-op; the
      # struct is returned with reader_pid cleared and no exit/unlink is attempted on the dead pid.
      assert Connection.retire_reader(state(reader_pid: dead)).reader_pid == nil
    end
  end

  describe "track_txn/2 — transaction-in-flight boundary maintenance (spec A1 §3.1)" do
    test "Begin opens the transaction (in_txn true)" do
      st = Connection.track_txn(state(in_txn: false), %Begin{xid: 7})
      assert st.in_txn == true
    end

    test "StreamStart opens a streamed transaction (adds its xid to open_streams)" do
      st = Connection.track_txn(state([]), %StreamStart{xid: 7, first_segment: 1})
      assert MapSet.member?(st.open_streams, 7)
    end

    test "Commit closes the transaction and records last_commit_lsn" do
      st = Connection.track_txn(state(in_txn: true, last_commit_lsn: 0), %Commit{lsn: 0x500})
      assert st.in_txn == false
      assert st.last_commit_lsn == 0x500
    end

    test "StreamCommit closes its streamed xid and records its commit_lsn" do
      st0 = state(open_streams: MapSet.new([7]), last_commit_lsn: 0)
      st = Connection.track_txn(st0, %StreamCommit{xid: 7, commit_lsn: 0x900})
      refute MapSet.member?(st.open_streams, 7)
      assert st.last_commit_lsn == 0x900
    end

    test "CV1: a StreamCommit for one xid keeps a CONCURRENT open streamed xid open (no premature idle)" do
      st =
        state([])
        |> Connection.track_txn(%StreamStart{xid: 100, first_segment: 1})
        |> Connection.track_txn(%StreamStart{xid: 200, first_segment: 1})
        |> Connection.track_txn(%StreamCommit{xid: 100, commit_lsn: 0x900})

      # 200 is still open — the old single-boolean `in_txn` would have been cleared by A's commit.
      assert MapSet.member?(st.open_streams, 200)
      refute MapSet.member?(st.open_streams, 100)
    end

    test "CV2: a SUBtransaction StreamAbort (xid != subxid) keeps the parent txn open" do
      st =
        Connection.track_txn(state(open_streams: MapSet.new([100])), %StreamAbort{
          xid: 100,
          subxid: 105
        })

      assert MapSet.member?(st.open_streams, 100)
    end

    test "a whole-transaction StreamAbort (xid == subxid) closes the streamed xid, records no commit" do
      st0 = state(open_streams: MapSet.new([100]), last_commit_lsn: 0x500)
      st = Connection.track_txn(st0, %StreamAbort{xid: 100, subxid: 100})
      refute MapSet.member?(st.open_streams, 100)
      assert st.last_commit_lsn == 0x500
    end

    test "last_commit_lsn never regresses on an out-of-order lower commit" do
      st = Connection.track_txn(state(in_txn: true, last_commit_lsn: 0x900), %Commit{lsn: 0x500})
      assert st.last_commit_lsn == 0x900
    end

    test "StreamStop (a pause) leaves the transaction open" do
      st = Connection.track_txn(state(in_txn: true), %Replicant.Decoder.Messages.StreamStop{})
      assert st.in_txn == true
    end

    test "a data message (Insert) does not change the flags" do
      before = state(in_txn: true, last_commit_lsn: 0x500)

      after_ =
        Connection.track_txn(before, %Replicant.Decoder.Messages.Insert{
          relation_id: 1,
          tuple_data: []
        })

      assert after_.in_txn == true
      assert after_.last_commit_lsn == 0x500
    end
  end
end

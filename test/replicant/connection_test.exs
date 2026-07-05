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

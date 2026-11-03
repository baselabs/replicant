defmodule Replicant.ReceiveWatchdogTest do
  use ExUnit.Case, async: false
  alias Replicant.Test.{PG16, SilentProxy}
  @moduletag :integration
  @moduletag :pg10

  defmodule Sink do
    @behaviour Replicant.Sink
    def checkpoint, do: {:ok, Agent.get(__MODULE__, & &1.lsn)}

    def handle_transaction(txn) do
      Agent.update(__MODULE__, fn state ->
        %{
          state
          | lsn: txn.commit_lsn,
            rows: state.rows ++ Enum.map(txn.changes, & &1.record["id"])
        }
      end)

      {:ok, txn.commit_lsn}
    end
  end

  defmodule SnapshotSink do
    @behaviour Replicant.Sink
    defdelegate checkpoint(), to: Sink
    defdelegate handle_transaction(txn), to: Sink

    def handle_snapshot(changes, _context) do
      send(Agent.get(Sink, & &1.owner), :snapshot_entered)
      Process.sleep(1200)
      Agent.update(Sink, &%{&1 | rows: Enum.map(changes, fn change -> change.record["id"] end)})
      :ok
    end

    def handle_snapshot_complete(lsn) do
      Agent.update(Sink, &%{&1 | lsn: lsn})
      {:ok, lsn}
    end
  end

  setup context do
    owner = self()
    snapshot? = Map.get(context, :snapshot, false)

    start_supervised!(%{
      id: Sink,
      start: {Agent, :start_link, [fn -> %{lsn: nil, rows: [], owner: owner} end, [name: Sink]]}
    })

    ctrl = start_supervised!({Postgrex, PG16.pg_opts()})
    proxy = start_supervised!({SilentProxy, PG16.pg_opts()})
    slot = "rep_health_#{System.unique_integer([:positive])}"
    Postgrex.query!(ctrl, "CREATE TABLE #{slot} (id int PRIMARY KEY)", [])
    Postgrex.query!(ctrl, "CREATE PUBLICATION #{slot} FOR TABLE #{slot}", [])
    owner = self()
    telemetry_id = {__MODULE__, make_ref()}

    :telemetry.attach(
      telemetry_id,
      [:replicant, :connection, :receive_timeout],
      fn _, m, meta, pid -> send(pid, {:timeout, m, meta}) end,
      owner
    )

    on_exit(fn ->
      :telemetry.detach(telemetry_id)
      Replicant.stop(slot)
      {:ok, conn} = Postgrex.start_link(PG16.pg_opts())

      PG16.wait_until(fn ->
        Postgrex.query!(conn, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", [slot]).rows in [
          [],
          [[false]]
        ]
      end)

      Postgrex.query!(
        conn,
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1",
        [slot]
      )

      Postgrex.query!(conn, "DROP PUBLICATION #{slot}", [])
      Postgrex.query!(conn, "DROP TABLE #{slot}", [])
      PG16.stop_conn(conn)
    end)

    opts =
      Keyword.merge(PG16.pg_opts(),
        hostname: "127.0.0.1",
        port: SilentProxy.port(proxy),
        parameters: [wal_sender_timeout: "0"]
      )

    if snapshot?, do: Postgrex.query!(ctrl, "INSERT INTO #{slot} VALUES (1)", [])

    {:ok, _} =
      Replicant.start_link(
        connection: opts,
        slot_name: slot,
        publication: slot,
        sink: if(snapshot?, do: SnapshotSink, else: Sink),
        go_forward_only: not snapshot?,
        snapshot: snapshot?,
        max_command_retries: Map.get(context, :max_retries, 5),
        receive_timeout: 800
      )

    unless snapshot? do
      PG16.wait_until(fn -> state(slot).step == :streaming end)
      Postgrex.query!(ctrl, "INSERT INTO #{slot} VALUES (1)", [])
      PG16.wait_until(fn -> Agent.get(Sink, & &1.rows) == [1] end)
    end

    %{ctrl: ctrl, proxy: proxy, slot: slot}
  end

  @tag snapshot: true
  test "exported snapshot may exceed the receive timeout before streaming starts", %{
    ctrl: ctrl,
    slot: slot
  } do
    assert_receive :snapshot_entered, 3000
    assert state(slot).step == :snapshotting
    assert state(slot).flow.health_timer == nil
    PG16.wait_until(fn -> state(slot).step == :streaming end)
    Postgrex.query!(ctrl, "INSERT INTO #{slot} VALUES (2)", [])
    PG16.wait_until(fn -> Agent.get(Sink, & &1.rows) == [1, 2] end)
    refute_receive {:timeout, _, _}
  end

  test "healthy idle ping, silent half-open socket recovery, durable replay and stale timer fencing",
       %{ctrl: ctrl, proxy: proxy, slot: slot} do
    initial = state(slot)
    # Server keepalives disabled: reply-requested client probes keep idle healthy.
    Process.sleep(2000)
    refute_receive {:timeout, _, _}
    assert state(slot).flow.epoch == initial.flow.epoch
    {_, stale_token} = state(slot).flow.health_timer
    {:ok, checkpoint} = Sink.checkpoint()
    :ok = SilentProxy.blackhole(proxy)
    Postgrex.query!(ctrl, "INSERT INTO #{slot} VALUES (2)", [])

    assert_receive {:timeout, %{duration: age}, %{slot_name: ^slot, reason: :receive_timeout}},
                   2000

    assert age >= 800
    PG16.wait_until(fn -> Agent.get(Sink, & &1.rows) == [1, 2] end)
    recovered = state(slot)
    assert recovered.flow.epoch != initial.flow.epoch
    assert recovered.checkpoint_lsn >= checkpoint
    send(connection(slot), {:receive_watchdog, stale_token})
    Process.sleep(1000)
    assert state(slot).flow.epoch == recovered.flow.epoch
    assert Agent.get(Sink, & &1.rows) == [1, 2]
    refute_receive {:timeout, _, _}
  end

  @tag max_retries: 0
  test "zero retry budget hands a silent connection failure to the application supervisor", %{
    proxy: proxy,
    slot: slot
  } do
    :ok = SilentProxy.blackhole(proxy)
    assert_receive {:timeout, _, %{slot_name: ^slot}}, 2000
    PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :connection}) == [] end)
    assert Agent.get(Sink, & &1.rows) == [1]
  end

  test "slot lost during silent outage halts without recreating or skipping data", %{
    ctrl: ctrl,
    proxy: proxy,
    slot: slot
  } do
    :ok = SilentProxy.blackhole(proxy)

    PG16.wait_until(fn ->
      Postgrex.query!(ctrl, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", [slot]).rows ==
        [[false]]
    end)

    Postgrex.query!(ctrl, "SELECT pg_drop_replication_slot($1)", [slot])
    Postgrex.query!(ctrl, "INSERT INTO #{slot} VALUES (2)", [])
    assert_receive {:timeout, _, %{slot_name: ^slot}}, 2000
    PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :connection}) == [] end)

    assert Postgrex.query!(
             ctrl,
             "SELECT slot_name FROM pg_replication_slots WHERE slot_name=$1",
             [slot]
           ).rows == []

    assert Agent.get(Sink, & &1.rows) == [1]
  end

  defp connection(slot), do: GenServer.whereis(Replicant.Connection.via(slot))

  defp state(slot) do
    {_, %{state: {_, state}}} = :sys.get_state(connection(slot))
    state
  end
end

defmodule Replicant.BackpressureTest do
  use ExUnit.Case, async: false

  alias Replicant.Test.PG16

  @moduletag :integration
  @moduletag :pg10

  defmodule SlowSink do
    @behaviour Replicant.Sink

    def checkpoint, do: {:ok, Agent.get(__MODULE__, & &1.lsn)}

    def handle_transaction(txn) do
      {owner, block?} = Agent.get(__MODULE__, &{&1.owner, &1.lsn == nil})

      if block? do
        send(owner, {:sink_blocked, self()})

        receive do
          :release_sink -> :ok
        after
          20_000 -> raise "test sink was not released"
        end
      end

      Agent.update(__MODULE__, fn state ->
        %{state | lsn: txn.commit_lsn, transactions: [txn | state.transactions]}
      end)

      {:ok, txn.commit_lsn}
    end
  end

  @tag timeout: 40_000
  test "a blocked sink pauses input, keeps its slot alive, then drains every transaction in order" do
    owner = self()

    start_supervised!(%{
      id: SlowSink,
      start:
        {Agent, :start_link,
         [fn -> %{owner: owner, lsn: nil, transactions: []} end, [name: SlowSink]]}
    })

    ctrl = start_supervised!({Postgrex, PG16.pg_opts()})
    slot = "rep_pressure_#{System.unique_integer([:positive])}"
    table = slot <> "_rows"
    publication = slot <> "_pub"
    Postgrex.query!(ctrl, "CREATE TABLE #{table} (id int PRIMARY KEY, payload text)", [])
    Postgrex.query!(ctrl, "CREATE PUBLICATION #{publication} FOR TABLE #{table}", [])

    on_exit(fn ->
      Replicant.stop(slot)
      {:ok, cleanup} = Postgrex.start_link(PG16.pg_opts())

      PG16.wait_until(fn ->
        Postgrex.query!(cleanup, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", [
          slot
        ]).rows in [[], [[false]]]
      end)

      Postgrex.query!(
        cleanup,
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1",
        [slot]
      )

      Postgrex.query!(cleanup, "DROP PUBLICATION #{publication}", [])
      Postgrex.query!(cleanup, "DROP TABLE #{table}", [])
      PG16.stop_conn(cleanup)
    end)

    {:ok, _pipeline} =
      Replicant.start_link(
        connection: PG16.pg_opts() ++ [parameters: [wal_sender_timeout: "8000"]],
        slot_name: slot,
        publication: publication,
        sink: SlowSink,
        go_forward_only: true,
        max_inflight_lag: 2048
      )

    PG16.wait_until(fn -> connection_state(slot).step == :streaming end)
    [[backend, initial_flush]] = slot_status(ctrl, slot)

    Postgrex.query!(ctrl, "INSERT INTO #{table} VALUES (1, repeat('x', 300))", [])
    assert_receive {:sink_blocked, assembler}, 5000

    for id <- 2..100 do
      Postgrex.query!(ctrl, "INSERT INTO #{table} VALUES ($1, repeat('x', 300))", [id])
    end

    PG16.wait_until(fn -> connection_state(slot).flow.paused end)
    paused = connection_state(slot)
    assert Replicant.FlowControl.queued_bytes(paused.flow) < 2500
    assert Replicant.FlowControl.queued_messages(paused.flow) <= 512
    assert SlowSink.checkpoint() == {:ok, nil}

    # Longer than wal_sender_timeout: outgoing durable feedback must keep the
    # same backend alive even though incoming keepalive delivery is paused.
    Process.sleep(10_000)
    assert [[^backend, ^initial_flush]] = slot_status(ctrl, slot)
    assert connection_state(slot).flow.paused
    assert connection_state(slot).flow.sent_bytes == paused.flow.sent_bytes

    send(assembler, :release_sink)
    PG16.wait_until(fn -> Agent.get(SlowSink, &(length(&1.transactions) == 100)) end)
    PG16.wait_until(fn -> not connection_state(slot).flow.paused end)
    assert [[^backend, _flush]] = slot_status(ctrl, slot)

    transactions = Agent.get(SlowSink, &Enum.reverse(&1.transactions))

    assert Enum.flat_map(transactions, fn txn -> Enum.map(txn.changes, & &1.record["id"]) end) ==
             Enum.to_list(1..100)

    lsns = Enum.map(transactions, & &1.commit_lsn)
    assert lsns == Enum.sort(Enum.uniq(lsns))

    # A segment switch advances WAL positions without adding publication rows.
    Postgrex.query!(ctrl, "SELECT pg_switch_wal()", [])
    Postgrex.query!(ctrl, "INSERT INTO #{table} VALUES (101, 'after switch')", [])
    PG16.wait_until(fn -> Agent.get(SlowSink, &(length(&1.transactions) == 101)) end)
    assert [[^backend, _flush]] = slot_status(ctrl, slot)
  end

  defp connection_state(slot) do
    case Registry.lookup(Replicant.Registry, {slot, :connection}) do
      [{pid, _}] ->
        {:no_state, %{state: {Replicant.Connection, state}}} = :sys.get_state(pid)
        state

      [] ->
        %{step: :disconnected}
    end
  end

  defp slot_status(ctrl, slot) do
    Postgrex.query!(
      ctrl,
      "SELECT active_pid, confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1",
      [slot]
    ).rows
  end
end

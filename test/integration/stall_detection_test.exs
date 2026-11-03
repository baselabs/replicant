defmodule Replicant.StallDetectionTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  alias Replicant.Test.PG16
  @moduletag :integration
  @moduletag :pg10

  defmodule Sink do
    @behaviour Replicant.Sink
    def checkpoint, do: {:ok, Agent.get(__MODULE__, & &1.lsn)}

    def handle_transaction(txn) do
      {owner, block?} =
        Agent.get_and_update(__MODULE__, &{{&1.owner, &1.block?}, %{&1 | block?: false}})

      if block? do
        send(owner, {:blocked, self()})

        receive do
          :release -> :ok
        after
          30_000 -> raise "test sink was not released"
        end
      end

      Agent.update(__MODULE__, &%{&1 | lsn: txn.commit_lsn, count: &1.count + 1})
      {:ok, txn.commit_lsn}
    end
  end

  setup context do
    owner = self()

    start_supervised!(%{
      id: Sink,
      start:
        {Agent, :start_link,
         [fn -> %{owner: owner, lsn: nil, count: 0, block?: true} end, [name: Sink]]}
    })

    ctrl = start_supervised!({Postgrex, PG16.pg_opts()})
    slot = "rep_stall_#{System.unique_integer([:positive])}"
    Postgrex.query!(ctrl, "CREATE TABLE #{slot} (id int PRIMARY KEY)", [])
    Postgrex.query!(ctrl, "CREATE PUBLICATION #{slot} FOR TABLE #{slot}", [])
    handler = {__MODULE__, make_ref()}

    :telemetry.attach(
      handler,
      [:replicant, :connection, :stalled],
      fn _, measurements, metadata, pid -> send(pid, {:stalled, measurements, metadata}) end,
      owner
    )

    on_exit(fn ->
      :telemetry.detach(handler)
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

    {:ok, _} =
      Replicant.start_link(
        connection: PG16.pg_opts(),
        slot_name: slot,
        publication: slot,
        sink: Sink,
        go_forward_only: true,
        max_inflight_lag: Map.get(context, :budget, 1_000_000)
      )

    PG16.wait_until(fn -> state(slot).step == :streaming end)
    %{ctrl: ctrl, slot: slot, handler: handler}
  end

  @tag :quiet
  test "quiet hung sink signals without pausing, warns without a handler, and stops after drain",
       %{ctrl: ctrl, slot: slot, handler: handler} do
    log =
      capture_log(fn ->
        Postgrex.query!(ctrl, "INSERT INTO #{slot} VALUES (1)", [])
        assert_receive {:blocked, sink}, 3000
        refute state(slot).flow.paused
        assert Replicant.FlowControl.queued_messages(state(slot).flow) > 0

        assert_receive {:stalled, %{duration: duration, change_count: count},
                        %{slot_name: ^slot}},
                       11_000

        assert duration >= 5000
        assert count > 0
        refute state(slot).flow.paused
        :telemetry.detach(handler)
        # A second notification must still warn when nobody subscribes to telemetry.
        Process.sleep(5500)
        send(sink, :release)
        PG16.wait_until(fn -> Replicant.FlowControl.queued_messages(state(slot).flow) == 0 end)
        assert state(slot).flow.stall_timer == nil

        IO.puts(
          "STALL-QUIET paused=false queued=#{count} first_duration_ms=#{duration} drained=true"
        )
      end)

    assert length(Regex.scan(~r/Replicant assembler stalled/, log)) >= 2
    assert capture_log(fn -> Process.sleep(5500) end) == ""
  end

  @tag budget: 256
  test "short pause cycles with completed frames do not accumulate into a stall", %{
    ctrl: ctrl,
    slot: slot
  } do
    log =
      capture_log(fn ->
        for cycle <- 1..6 do
          Agent.update(Sink, &%{&1 | block?: true})

          for offset <- 1..12 do
            Postgrex.query!(ctrl, "INSERT INTO #{slot} VALUES ($1)", [cycle * 100 + offset])
          end

          assert_receive {:blocked, sink}, 3000
          PG16.wait_until(fn -> state(slot).flow.paused end)
          Process.sleep(1000)
          send(sink, :release)
          PG16.wait_until(fn -> Agent.get(Sink, & &1.count) == cycle * 12 end)
          PG16.wait_until(fn -> Replicant.FlowControl.queued_messages(state(slot).flow) == 0 end)
        end

        refute_receive {:stalled, _, _}
        IO.puts("STALL-PROGRESS short_pause_cycles=6 committed_transactions=72 false_stalls=0")
      end)

    refute log =~ "Replicant assembler stalled"
  end

  defp state(slot) do
    {_, %{state: {_, state}}} = :sys.get_state(GenServer.whereis(Replicant.Connection.via(slot)))
    state
  end
end

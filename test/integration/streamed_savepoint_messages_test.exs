defmodule Replicant.StreamedSavepointMessagesTest do
  use ExUnit.Case, async: false
  alias Replicant.Test.PG16
  @moduletag :integration
  @moduletag :pg14

  defmodule Sink do
    @behaviour Replicant.Sink
    def checkpoint, do: {:ok, Agent.get(__MODULE__, & &1.lsn)}

    def handle_transaction(txn) do
      ids = Enum.map(txn.changes, & &1.record["id"])
      Agent.update(__MODULE__, &%{&1 | lsn: txn.commit_lsn, ids: ids})
      {:ok, txn.commit_lsn}
    end

    def handle_message(_message, _context), do: :ok
  end

  for spill? <- [false, true] do
    @tag spill: spill?
    test "live pgoutput savepoint message attribution, spill=#{spill?}", %{spill: spill?} do
      start_supervised!(%{
        id: Sink,
        start: {Agent, :start_link, [fn -> %{lsn: nil, ids: []} end, [name: Sink]]}
      })

      ctrl = start_supervised!({Postgrex, PG16.pg_opts()})
      flush = start_supervised!(Supervisor.child_spec({Postgrex, PG16.pg_opts()}, id: :flush))
      slot = "rep_savepoint_msg_#{System.unique_integer([:positive])}"
      spill_dir = Path.join(System.tmp_dir!(), slot)
      Postgrex.query!(ctrl, "CREATE TABLE #{slot} (id int PRIMARY KEY, payload text)", [])
      Postgrex.query!(ctrl, "CREATE PUBLICATION #{slot} FOR TABLE #{slot}", [])
      [[version]] = Postgrex.query!(ctrl, "SHOW server_version_num", []).rows

      on_exit(fn ->
        Replicant.stop(slot)
        {:ok, conn} = Postgrex.start_link(PG16.pg_opts())

        PG16.wait_until(fn ->
          Postgrex.query!(conn, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", [
            slot
          ]).rows in [[], [[false]]]
        end)

        Postgrex.query!(
          conn,
          "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1",
          [slot]
        )

        Postgrex.query!(conn, "DROP PUBLICATION #{slot}", [])
        Postgrex.query!(conn, "DROP TABLE #{slot}", [])
        PG16.stop_conn(conn)
        File.rm_rf!(spill_dir)
      end)

      streaming = if spill?, do: [spill: [dir: spill_dir, max_spill_bytes: 8_000_000]], else: []

      {:ok, _} =
        Replicant.start_link(
          connection: PG16.pg_opts() ++ [parameters: [logical_decoding_work_mem: "64kB"]],
          slot_name: slot,
          publication: slot,
          sink: Sink,
          go_forward_only: true,
          messages: true,
          streaming: streaming,
          max_inflight_lag: if(spill?, do: 32_000, else: 8_000_000)
        )

      PG16.wait_until(fn ->
        Postgrex.query!(ctrl, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", [slot]).rows ==
          [[true]]
      end)

      {:ok, :ok} =
        Postgrex.transaction(
          ctrl,
          fn conn ->
            Postgrex.query!(conn, "INSERT INTO #{slot} VALUES (1, 'parent')", [])
            [[top]] = Postgrex.query!(conn, "SELECT txid_current()", []).rows
            Postgrex.query!(conn, "SAVEPOINT discarded", [])

            Postgrex.query!(
              conn,
              "SELECT pg_logical_emit_message(true, 'savepoint_test', 'synthetic')",
              []
            )

            Postgrex.query!(
              conn,
              "INSERT INTO #{slot} SELECT g, repeat(md5(g::text), 64) FROM generate_series(2, 1001) g",
              []
            )

            flush_wal(flush)
            PG16.wait_until(fn -> match?(%{messages: [_ | _]}, buffer(slot, top)) end, 200)
            before = buffer(slot, top)
            message_xids = Enum.map(before.messages, & &1.xid) |> Enum.uniq()

            subxids =
              (Enum.map(before.changes, &elem(&1, 0)) ++ Map.keys(before.spilled_by_subxid))
              |> Enum.uniq()
              |> Enum.reject(&(&1 == top))

            assert message_xids == [top]
            assert [sub] = subxids
            if spill?, do: assert(before.spill != nil)
            Postgrex.query!(conn, "ROLLBACK TO SAVEPOINT discarded", [])
            flush_wal(flush)
            PG16.wait_until(fn -> MapSet.member?(buffer(slot, top).aborted, sub) end, 200)
            after_abort = buffer(slot, top)

            IO.puts(
              "SAVEPOINT-WIRE pg=#{version} spill=#{spill?} top_xid=#{top} row_subxid=#{sub} message_xids=#{inspect(message_xids)} messages_after_abort=#{length(after_abort.messages)} resident_before=#{before.resident_bytes} resident_after=#{after_abort.resident_bytes}"
            )

            Postgrex.query!(conn, "INSERT INTO #{slot} VALUES (1002, 'parent_after')", [])
            :ok
          end,
          timeout: 20_000
        )

      PG16.wait_until(fn -> Agent.get(Sink, & &1.ids) == [1, 1002] end)
      assert Postgrex.query!(ctrl, "SELECT id FROM #{slot} ORDER BY id", []).rows == [[1], [1002]]
    end
  end

  defp buffer(slot, top),
    do:
      :sys.get_state(GenServer.whereis(Replicant.AssemblerServer.via(slot))).asm.stream_txns[top]

  defp flush_wal(conn),
    do: Postgrex.query!(conn, "SELECT pg_logical_emit_message(false, 'flush_test', '')", [])
end

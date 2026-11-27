defmodule Replicant.PG16ConformanceTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.Decoder
  alias Replicant.Decoder.Messages.{Begin, Commit, Insert, Relation}
  alias Replicant.Test.PG16

  defmodule CaptureSink do
    @behaviour Replicant.Sink
    use Agent

    def start_link, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    @impl true
    def checkpoint, do: {:ok, nil}
    @impl true
    def handle_transaction(txn) do
      Agent.update(__MODULE__, &[txn | &1])
      {:ok, txn.commit_lsn}
    end

    def transactions, do: Agent.get(__MODULE__, &Enum.reverse/1)
  end

  test "real PG16 pgoutput for an insert decodes to the same struct Plan 1 asserts" do
    {:ok, ctrl} = PG16.named_conn(Replicant.Test.LedgerConn)
    {:ok, _} = CaptureSink.start_link()
    slot = "rep_conf_#{System.unique_integer([:positive])}"

    Postgrex.query!(ctrl, "DROP PUBLICATION IF EXISTS conf_pub", [])
    Postgrex.query!(ctrl, "DROP TABLE IF EXISTS conf_orders", [])
    Postgrex.query!(ctrl, "CREATE TABLE conf_orders (id int PRIMARY KEY, note text)", [])
    Postgrex.query!(ctrl, "CREATE PUBLICATION conf_pub FOR TABLE conf_orders", [])

    on_exit(fn ->
      Replicant.stop(slot)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 200)
      {:ok, c} = Postgrex.start_link(PG16.pg_opts())

      Postgrex.query!(
        c,
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
        [slot]
      )
    end)

    :telemetry.attach(
      {__MODULE__, slot},
      [:replicant, :connection, :slot_active],
      fn _e, _m, _meta, pid -> send(pid, :active) end,
      self()
    )

    {:ok, _} =
      Replicant.start_link(
        connection: PG16.pg_opts(),
        slot_name: slot,
        publication: "conf_pub",
        sink: CaptureSink,
        go_forward_only: true
      )

    assert_receive :active, 15_000

    Postgrex.query!(ctrl, "INSERT INTO conf_orders (id, note) VALUES (1, 'hello')", [])
    PG16.wait_until(fn -> CaptureSink.transactions() != [] end)

    [%Replicant.Transaction{changes: [change]} = txn] = CaptureSink.transactions()
    assert is_integer(txn.commit_lsn) and txn.commit_lsn > 0
    assert change.op == :insert
    assert change.table == "conf_orders"
    assert change.record["id"] == 1
    assert change.record["note"] == "hello"
    assert Enum.all?(Map.keys(change.record), &is_binary/1)
  end

  @moduledoc false
  def _referenced, do: {Decoder, Begin, Commit, Insert, Relation}
end

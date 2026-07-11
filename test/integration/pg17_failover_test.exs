defmodule Replicant.Integration.PG17FailoverTest do
  @moduledoc """
  PG17-only integration coverage (spec §12 Definition of Done). Tagged `:integration` +
  `:pg17`; `test_helper.exs` excludes `:pg17` when the connected server is < 17, so these
  RUN on PG17 (:5617) and are cleanly SKIPPED (never vacuously passed) on PG16 (:5599).
  Observes the REAL code path: a full `Replicant` pipeline (Config → Connection → QueryBuilder
  → PG), not a hand-rolled driver.
  """
  use ExUnit.Case, async: false

  alias Replicant.{Connection, QueryBuilder}
  alias Replicant.Test.{PG16, RecordingSink}

  @moduletag :integration
  @moduletag :pg17

  setup do
    {:ok, ctrl} = Postgrex.start_link(PG16.pg_opts())
    {:ok, _} = RecordingSink.start_link()
    RecordingSink.reset()

    slot = "repl_pg17_fo_#{System.unique_integer([:positive])}"
    pub = "repl_pg17_pub_#{System.unique_integer([:positive])}"

    Postgrex.query!(ctrl, "CREATE TABLE IF NOT EXISTS pg17_fo (id int PRIMARY KEY)", [])
    Postgrex.query!(ctrl, "DROP PUBLICATION IF EXISTS #{pub}", [])
    Postgrex.query!(ctrl, "CREATE PUBLICATION #{pub} FOR TABLE pg17_fo", [])
    drop_slot(ctrl, slot)

    on_exit(fn ->
      Replicant.stop(slot)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 200)
      {:ok, c} = Postgrex.start_link(PG16.pg_opts())
      drop_slot(c, slot)
      Postgrex.query!(c, "DROP PUBLICATION IF EXISTS #{pub}", [])
    end)

    %{ctrl: ctrl, slot: slot, pub: pub}
  end

  @tag timeout: 60_000
  test "a pipeline started with failover: true creates a slot with failover=true (end-to-end)",
       %{ctrl: ctrl, slot: slot, pub: pub} do
    {:ok, _pid} =
      Replicant.start_link(
        connection: PG16.pg_opts(),
        slot_name: slot,
        publication: pub,
        sink: RecordingSink,
        go_forward_only: true,
        failover: true
      )

    PG16.wait_until(fn -> slot_failover(ctrl, slot) == [[true]] end, 400)

    assert slot_failover(ctrl, slot) == [[true]],
           "the failover slot was not created with failover=true — the FAILOVER grammar did not reach PG"
  end

  @tag timeout: 60_000
  test "the version-gated 4-col invalidation query runs on live PG17 and a healthy slot is :ok",
       %{ctrl: ctrl, slot: slot} do
    Postgrex.query!(ctrl, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", [slot])
    version = server_version_num(ctrl)
    assert version >= 170_000

    {:ok, status_sql} = QueryBuilder.slot_invalidation_status(slot, version)
    assert status_sql =~ "invalidation_reason"
    assert status_sql =~ "synced"

    rows = Postgrex.query!(ctrl, status_sql, []).rows
    assert Connection.classify_slot_status(rows) == :ok
  end

  defp slot_failover(conn, slot),
    do:
      Postgrex.query!(conn, "SELECT failover FROM pg_replication_slots WHERE slot_name = $1", [
        slot
      ]).rows

  defp server_version_num(conn),
    do:
      Postgrex.query!(conn, "SHOW server_version_num", []).rows
      |> hd()
      |> hd()
      |> String.to_integer()

  defp drop_slot(conn, slot) do
    Postgrex.query!(
      conn,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  rescue
    _ -> :ok
  end
end

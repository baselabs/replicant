defmodule Replicant.Integration.VersionBehaviorTest do
  @moduledoc """
  Cross-version transport + version-gated failover coverage (ticket R05, spec §12 Definition
  of Done). Tagged `:integration` only (NOT `:pg17`), so it runs against WHATEVER live server
  `REPLICANT_TEST_URL` points at — PostgreSQL 15, 16, 17, or 18 — and branches on the live
  `server_version_num`. This is the CI matrix's per-row proof: each version row runs the SAME
  test and proves the version-appropriate behaviour, so a row that pointed at the wrong version
  (or skipped integration) is caught.

  Observes the REAL code path (Config → Connection → QueryBuilder → PG), not a hand-rolled
  driver. Failover is proved created where PostgreSQL supports it (17, 18) and structurally
  rejected where it does not (15, 16 → `{:config, :failover_unsupported}` halt).
  """
  use ExUnit.Case, async: false

  alias Replicant.{Connection, QueryBuilder}
  alias Replicant.Test.{PG16, RecordingSink}

  @moduletag :integration

  setup do
    {:ok, ctrl} = Postgrex.start_link(PG16.pg_opts())
    {:ok, _} = RecordingSink.start_link()
    RecordingSink.reset()

    slot = "repl_r05_ver_#{System.unique_integer([:positive])}"
    pub = "repl_r05_pub_#{System.unique_integer([:positive])}"

    Postgrex.query!(ctrl, "CREATE TABLE IF NOT EXISTS r05_ver (id int PRIMARY KEY)", [])
    Postgrex.query!(ctrl, "DROP PUBLICATION IF EXISTS #{pub}", [])
    Postgrex.query!(ctrl, "CREATE PUBLICATION #{pub} FOR TABLE r05_ver", [])
    drop_slot(ctrl, slot)

    on_exit(fn ->
      Replicant.stop(slot)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 200)
      {:ok, c} = Postgrex.start_link(PG16.pg_opts())
      drop_slot(c, slot)
      Postgrex.query!(c, "DROP PUBLICATION IF EXISTS #{pub}", [])
    end)

    %{ctrl: ctrl, slot: slot, pub: pub, version: server_version_num(ctrl)}
  end

  @tag timeout: 30_000
  test "substrate receipt — the live server major matches EXPECTED_PG_MAJOR (CI matrix-row proof)",
       %{version: version} do
    major = div(version, 10_000)

    # The discoverable receipt CI greps for to prove THIS matrix row actually ran integration
    # tests against the version it claims (non-vacuity — a skipped/mis-wired row emits nothing).
    IO.puts("R05-SUBSTRATE-RECEIPT pg=#{major} version_num=#{version}")

    assert major in [15, 16, 17, 18],
           "R05 supports PostgreSQL 15-18; live server_version_num=#{version} is out of range"

    case System.get_env("EXPECTED_PG_MAJOR") do
      nil ->
        :ok

      "" ->
        :ok

      expected ->
        assert major == String.to_integer(expected),
               "matrix row expected PG#{expected} but the live server is PG#{major} " <>
                 "(version_num=#{version}) — REPLICANT_TEST_URL points at the wrong container"
    end
  end

  @tag timeout: 60_000
  test "the version-gated invalidation query runs on the live server and a healthy slot is :ok",
       %{ctrl: ctrl, slot: slot, version: version} do
    # Directly exercises the per-version column gate against the real catalog. On PG15 the
    # query is `wal_status`-only; selecting `conflicting` (as pre-R05) would error here.
    Postgrex.query!(ctrl, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", [slot])

    {:ok, status_sql} = QueryBuilder.slot_invalidation_status(slot, version)

    cond do
      version >= 170_000 ->
        assert status_sql =~ "invalidation_reason"
        assert status_sql =~ "synced"

      version >= 160_000 ->
        assert status_sql =~ "conflicting"
        refute status_sql =~ "invalidation_reason"

      true ->
        assert status_sql =~ "wal_status"
        refute status_sql =~ "conflicting"
    end

    rows = Postgrex.query!(ctrl, status_sql, []).rows
    assert Connection.classify_slot_status(rows) == :ok
  end

  @tag timeout: 60_000
  test "failover is version-gated: created on PG17+, structurally rejected on PG<17 (halt)",
       %{ctrl: ctrl, slot: slot, pub: pub, version: version} do
    if version >= 170_000 do
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
             "PG#{div(version, 10_000)} supports failover slots but the slot was not created " <>
               "with failover=true — the FAILOVER grammar did not reach PG"
    else
      handler = {__MODULE__, :failover_unsup, make_ref()}

      :ok =
        :telemetry.attach(
          handler,
          [:replicant, :connection, :slot_invalidated],
          fn _e, _m, meta, pid -> send(pid, {:failover_unsup, meta}) end,
          self()
        )

      on_exit(fn -> :telemetry.detach(handler) end)

      {:ok, _pid} =
        Replicant.start_link(
          connection: PG16.pg_opts(),
          slot_name: slot,
          publication: pub,
          sink: RecordingSink,
          go_forward_only: true,
          failover: true
        )

      assert_receive {:failover_unsup, %{reason: :failover_unsupported}},
                     10_000,
                     "PG#{div(version, 10_000)} rejects failover slots; the pipeline must halt " <>
                       "{:config, :failover_unsupported} BEFORE emitting FAILOVER to the server"

      # The slot must NOT have been created — the gate halts before CREATE_REPLICATION_SLOT.
      # Query by slot_name only: PG<17 has no `failover` column (selecting it would itself error).
      assert slot_present?(ctrl, slot) == [],
             "a failover-unsupported halt must never create the slot"
    end
  end

  defp slot_failover(conn, slot),
    do:
      Postgrex.query!(conn, "SELECT failover FROM pg_replication_slots WHERE slot_name = $1", [
        slot
      ]).rows

  # Version-agnostic slot-existence probe — `slot_name` exists on every supported major
  # (unlike `failover`, which is PG17+). Used on the PG<17 rejection path.
  defp slot_present?(conn, slot),
    do:
      Postgrex.query!(conn, "SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1", [
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

defmodule Replicant.DecoderHaltsTest do
  @moduledoc "Live pglogical schema-drift and pgoutput replica-identity refusal coverage."

  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag timeout: 120_000

  alias Replicant.Test.{ParitySink, PG16}

  import Replicant.Test.DecoderParity, only: [conn_opts: 1]

  setup do
    start_supervised!(ParitySink)
    :ok
  end

  # Every test registers its slot for teardown — a failed assert must not leak the
  # slot (leaks exhaust max_replication_slots and cascade into later tests).
  defp cleanup_slot(url, slot) do
    ExUnit.Callbacks.on_exit({:slot, slot}, fn ->
      try do
        {:ok, c} = Postgrex.start_link(conn_opts(url) ++ [backoff_type: :stop])

        Postgrex.query(
          c,
          "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1",
          [slot]
        )

        # A terminated walsender releases the slot ASYNCHRONOUSLY server-side; an
        # immediate drop races it ("... is active for PID") and the swallowed
        # {:error, _} leaks one slot per run until the server cap exhausts and every
        # later test fails slot creation (OBSERVED: ten leaked halt slots across the
        # day's runs, 2026-09-30). Wait for the SERVER's own release first, bounded;
        # a vanished slot (already dropped by the test body) also satisfies the wait.
        PG16.wait_until(
          fn ->
            Postgrex.query!(
              c,
              "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
              [slot]
            ).rows == [[1]] or
              Postgrex.query!(
                c,
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                [slot]
              ).rows == [[0]]
          end,
          200
        )

        Postgrex.query(
          c,
          "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
          [slot]
        )

        GenServer.stop(c)
      rescue
        _ -> :ok
      end
    end)
  end

  defp wait_until_gone(slot) do
    PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 200)
  end

  defp attach_event(name, event) do
    test_pid = self()

    :telemetry.attach(name, event, fn _e, _m, meta, _c -> send(test_pid, {name, meta}) end, nil)

    name
  end

  defp detach_event(name), do: :telemetry.detach(name)

  @tag :pglogical
  test "pglogical halts when an UPDATE follows a fixed-width column drop" do
    unless PG16.enabled?() do
      flunk("REPLICANT_TEST_URL not set")
    end

    url = System.fetch_env!("REPLICANT_TEST_URL")
    {:ok, ctrl} = Postgrex.start_link(conn_opts(url) ++ [backoff_type: :stop])

    Postgrex.query!(ctrl, "DROP TABLE IF EXISTS halt_wire CASCADE", [])
    Postgrex.query!(ctrl, "CREATE TABLE halt_wire (id int PRIMARY KEY, n int)", [])

    Postgrex.query!(
      ctrl,
      "SELECT pglogical.replication_set_add_table('default', 'public.halt_wire', false)",
      []
    )

    slot = "rep_halt_wire_#{System.unique_integer([:positive])}"
    cleanup_slot(url, slot)

    event = attach_event({:wire_halted, slot}, [:replicant, :schema_change, :halted])
    on_exit(fn -> detach_event(event) end)

    {:ok, _} =
      Replicant.start_link(
        connection: conn_opts(url),
        slot_name: slot,
        sink: ParitySink,
        go_forward_only: true,
        decoder: :pglogical,
        replication_sets: ["default"]
      )

    PG16.wait_until(fn ->
      rows =
        Postgrex.query!(
          ctrl,
          "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
          [slot]
        ).rows

      rows != [] and rows != [[nil]]
    end)

    :timer.sleep(500)

    mark = ParitySink.mark()
    Postgrex.query!(ctrl, "INSERT INTO halt_wire VALUES (1, 10)", [])
    PG16.wait_until(fn -> ParitySink.since(mark) != [] end)

    Postgrex.query!(ctrl, "ALTER TABLE halt_wire DROP COLUMN n", [])
    Postgrex.query!(ctrl, "UPDATE halt_wire SET id = 2 WHERE id = 1", [])

    assert_receive {^event, %{kind: :destructive}}, 10_000
    wait_until_gone(slot)
    GenServer.stop(ctrl)
  end

  test "pgoutput refuses updates on a table with replica identity nothing" do
    url = System.fetch_env!("REPLICANT_TEST_URL")
    {:ok, ctrl} = Postgrex.start_link(conn_opts(url) ++ [pool_size: 2, backoff_type: :stop])
    Postgrex.query!(ctrl, "DROP TABLE IF EXISTS halt_nothing CASCADE", [])
    Postgrex.query!(ctrl, "CREATE TABLE halt_nothing (id int PRIMARY KEY, v text)", [])
    Postgrex.query!(ctrl, "ALTER TABLE halt_nothing REPLICA IDENTITY NOTHING", [])

    pub = "halt_nothing_pub_#{System.unique_integer([:positive])}"
    Postgrex.query!(ctrl, "DROP PUBLICATION IF EXISTS #{pub}", [])
    Postgrex.query!(ctrl, "CREATE PUBLICATION #{pub} FOR TABLE halt_nothing", [])

    # OBSERVED live: PostgreSQL refuses the WRITE itself with object_not_in_
    # prerequisite_state ("does not have a replica identity and publishes updates")
    # — pgoutput never sees a keyless change from an update-publishing table at
    # all. The fail-closed boundary is the server's, before the stream.
    {:ok, _} = Postgrex.query(ctrl, "INSERT INTO halt_nothing VALUES (1, 'a')", [])

    assert {:error, %Postgrex.Error{postgres: %{code: :object_not_in_prerequisite_state}}} =
             Postgrex.query(ctrl, "UPDATE halt_nothing SET v = 'b' WHERE id = 1", [])

    Postgrex.query!(ctrl, "DROP PUBLICATION IF EXISTS #{pub}", [])
  end
end

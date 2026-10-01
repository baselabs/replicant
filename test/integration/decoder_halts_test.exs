defmodule Replicant.DecoderHaltsTest do
  @moduledoc """
  ADR-0009's halt and divergence behaviors on the live substrate. The unit-only halts
  (`:decoder_protocol_unsupported` — the live pglogical always reports protocol 1/1 —
  and `:decoder_lsn_missing` — `include-lsn` always carries the commit LSN) are driven
  red-capably in `test/replicant/decoder/wal2json_test.exs` and
  `pglogical_test.exs`; this module proves the LIVE-reachable ones.
  """

  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag timeout: 120_000

  alias Replicant.Test.{ParitySink, PG16}

  import Replicant.Test.DecoderParity, only: [conn_opts: 1, normalize: 1]

  @pg96_url System.get_env("REPLICANT_PG96_URL", "")

  setup do
    unless PG16.enabled?() do
      :ok
    end

    {:ok, _} = ParitySink.start_link()
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

  defp attach(slot, reason) do
    test_pid = self()

    :telemetry.attach(
      {__MODULE__, slot, reason},
      [:replicant, :connection, :slot_invalidated],
      fn _name, _m, meta, _ctx ->
        if meta[:reason] == reason, do: send(test_pid, {:halted, reason})
      end,
      :ok
    )
  end

  describe "wal2json schema guard (dropped-column backstop)" do
    @describetag :pg_old_decoders

    test "a dropped VARLENA column on an update-only table halts within the guard interval" do
      # The exact residual the wire rules cannot see: a dropped TOASTable column and an
      # untouched-TOAST sentinel are identical on the wal2json wire, and an update-only
      # table never gives the INSERT rule a chance. The periodic catalog guard re-reads
      # the configured tables and routes the subset Relation through the shipped
      # column_dropped classification (destructive, fail-closed).
      unless PG16.enabled?() do
        flunk("REPLICANT_TEST_URL not set")
      end

      url = System.fetch_env!("REPLICANT_TEST_URL")
      {:ok, ctrl} = Postgrex.start_link(conn_opts(url) ++ [backoff_type: :stop])

      Postgrex.query!(ctrl, "DROP TABLE IF EXISTS halt_guard CASCADE", [])
      Postgrex.query!(ctrl, "CREATE TABLE halt_guard (id int PRIMARY KEY, blob text)", [])

      slot = "rep_halt_grd_#{System.unique_integer([:positive])}"
      cleanup_slot(url, slot)

      _halted =
        attach_event(:schema_guard_halted, [:replicant, :schema_change, :halted])

      {:ok, _} =
        Replicant.start_link(
          connection: conn_opts(url),
          slot_name: slot,
          sink: ParitySink,
          go_forward_only: true,
          decoder: :wal2json,
          tables: [{"public", "halt_guard"}],
          schema_check_interval: 1_000
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

      Postgrex.query!(
        ctrl,
        "INSERT INTO halt_guard VALUES (1, repeat('x', 22000))",
        []
      )

      PG16.wait_until(fn -> ParitySink.since(mark) != [] end)

      # update WITHOUT touching the TOASTed column (the sentinel), then drop it
      Postgrex.query!(ctrl, "UPDATE halt_guard SET id = 2 WHERE id = 1", [])
      :timer.sleep(1_500)

      mark2 = ParitySink.mark()
      Postgrex.query!(ctrl, "ALTER TABLE halt_guard DROP COLUMN blob", [])
      # an update-only table: the next change STILL cannot reveal the drop on the wire
      Postgrex.query!(ctrl, "UPDATE halt_guard SET id = 3 WHERE id = 2", [])

      PG16.wait_until(fn -> ParitySink.since(mark2) != [] end, 200)

      assert_receive {:schema_guard_halted, %{kind: :destructive}}, 10_000
      wait_until_gone(slot)
      detach_event(:schema_guard_halted)
      GenServer.stop(ctrl)
    end
  end

  describe "wal2json wire-level dropped-column detection (immediate)" do
    @describetag :pg_old_decoders

    test "an UPDATE missing a FIXED-WIDTH column halts at the change (no guard wait)" do
      unless PG16.enabled?() do
        flunk("REPLICANT_TEST_URL not set")
      end

      url = System.fetch_env!("REPLICANT_TEST_URL")
      {:ok, ctrl} = Postgrex.start_link(conn_opts(url) ++ [backoff_type: :stop])

      Postgrex.query!(ctrl, "DROP TABLE IF EXISTS halt_wire CASCADE", [])
      Postgrex.query!(ctrl, "CREATE TABLE halt_wire (id int PRIMARY KEY, n int)", [])

      slot = "rep_halt_wire_#{System.unique_integer([:positive])}"
      cleanup_slot(url, slot)

      _halted = attach_event(:wire_halted, [:replicant, :schema_change, :halted])

      # a LONG guard interval: this leg must halt on the WIRE rule alone (the dropped
      # int4 is absent from the update, and a fixed-width value is never TOAST-omitted)
      {:ok, _} =
        Replicant.start_link(
          connection: conn_opts(url),
          slot_name: slot,
          sink: ParitySink,
          go_forward_only: true,
          decoder: :wal2json,
          tables: [{"public", "halt_wire"}],
          schema_check_interval: 60_000
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

      assert_receive {:wire_halted, %{kind: :destructive}}, 10_000
      wait_until_gone(slot)
      detach_event(:wire_halted)
      GenServer.stop(ctrl)
    end
  end

  # Old-rows-only by construction: the wal2json2_4 plugin name exists only in the
  # committed pg_old image (rows 9/12) — a stock 15-18 primary refuses the library
  # outright (42501 output_plugin_libraries, OBSERVED on an 18 substrate).
  describe ":decoder_option_unsupported (an old wal2json build rejects the option set)" do
    @describetag :pg_old_decoders

    test "a slot on the wal2json2_4 build halts instead of watchdog-looping" do
      unless PG16.enabled?() do
        flunk("REPLICANT_TEST_URL not set")
      end

      url = System.fetch_env!("REPLICANT_TEST_URL")
      {:ok, ctrl} = Postgrex.start_link(conn_opts(url) ++ [backoff_type: :stop])

      # The committed image carries wal2json 2.4 under a second plugin name — a build
      # that predates numeric-data-types-as-string (2.6). The pipeline reuses this
      # slot and its START_REPLICATION is rejected with invalid_parameter_value.
      slot = "rep_halt_old_w2j_#{System.unique_integer([:positive])}"

      Postgrex.query!(
        ctrl,
        "SELECT * FROM pg_create_logical_replication_slot($1, 'wal2json2_4')",
        [slot]
      )

      attach(slot, :decoder_option_unsupported)
      cleanup_slot(url, slot)

      {:ok, _} =
        Replicant.start_link(
          connection: conn_opts(url),
          slot_name: slot,
          sink: ParitySink,
          go_forward_only: true,
          decoder: :wal2json,
          tables: [{"public", "parity_all"}]
        )

      assert_receive {:halted, :decoder_option_unsupported}, 10_000
      wait_until_gone(slot)
      Postgrex.query!(ctrl, "SELECT pg_drop_replication_slot($1)", [slot])
      GenServer.stop(ctrl)
    end
  end

  defp pgoutput_96_refusal(url) do
    # warm postgrex's shared type cache over a REGULAR connection first: a
    # replication connection to a pre-10 walsender cannot bootstrap types (it
    # rejects SQL), and pgoutput pipelines do not pre-warm (the default path stays
    # untouched) — with the cache warm the replication connect succeeds and the
    # version gate fires.
    {:ok, warm} = Postgrex.start_link(conn_opts(url) ++ [sync_connect: true])
    Postgrex.query!(warm, "SELECT 1", [])
    GenServer.stop(warm)

    slot = "rep_halt_pgoutput96_#{System.unique_integer([:positive])}"
    attach(slot, :decoder_unsupported_on_server)
    cleanup_slot(url, slot)

    {:ok, _} =
      Replicant.start_link(
        connection: conn_opts(url),
        slot_name: slot,
        sink: ParitySink,
        go_forward_only: true,
        decoder: :pgoutput,
        publication: "any_pub"
      )

    assert_receive {:halted, :decoder_unsupported_on_server}, 10_000
    wait_until_gone(slot)

    # fail-closed BEFORE slot creation: no slot of that name exists on the server
    {:ok, ctrl} = Postgrex.start_link(conn_opts(url) ++ [backoff_type: :stop])

    assert Postgrex.query!(
             ctrl,
             "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
             [slot]
           ).rows == [[0]]

    GenServer.stop(ctrl)
  end

  # This leg is deliberately NOT :pg_old_decoders-tagged: it needs a real pre-10
  # PRIMARY (or a PG96 secondary) and must run on the 9-row, where the trio's other
  # primary-server legs are excluded. The target server resolves as the PG96
  # secondary when set, else the primary itself when it is pre-10; otherwise the leg
  # skips with a logged reason (never a vacuous pass — and never a flunk on rows
  # that carry no 9.6 at all).
  describe "{:config, :decoder_unsupported_on_server} (pgoutput before PG 10)" do
    test "pgoutput against the real 9.6 server halts before slot creation" do
      primary_pre10? = Replicant.TestHelper.server_version_num() < 100_000

      url =
        cond do
          @pg96_url not in [nil, ""] -> @pg96_url
          primary_pre10? -> System.fetch_env!("REPLICANT_TEST_URL")
          true -> nil
        end

      if is_nil(url) do
        IO.puts("pgoutput-on-9.6 refusal skipped: no pre-10 server on this row")
      else
        pgoutput_96_refusal(url)
      end
    end
  end

  describe "wal2json keyless-table pre-flight (divergence resolution: no silent drops)" do
    @describetag :pg_old_decoders

    test "a configured keyless table halts {:decoder, :table_keyless} at start; allow_keyless_tables opts in" do
      unless PG16.enabled?() do
        flunk("REPLICANT_TEST_URL not set")
      end

      url = System.fetch_env!("REPLICANT_TEST_URL")
      {:ok, ctrl} = Postgrex.start_link(conn_opts(url) ++ [backoff_type: :stop])

      Postgrex.query!(ctrl, "DROP TABLE IF EXISTS halt_keyless CASCADE", [])
      Postgrex.query!(ctrl, "CREATE TABLE halt_keyless (payload text)", [])

      # --- default: fail-closed at start (a keyless table's U/D would be silently
      # dropped plugin-side — wal2json.c skips them with only a server WARNING) ---
      slot = "rep_halt_kl_#{System.unique_integer([:positive])}"
      cleanup_slot(url, slot)
      attach(slot, :decoder_table_keyless)

      {:ok, _} =
        Replicant.start_link(
          connection: conn_opts(url),
          slot_name: slot,
          sink: ParitySink,
          go_forward_only: true,
          decoder: :wal2json,
          tables: [{"public", "halt_keyless"}]
        )

      assert_receive {:halted, :decoder_table_keyless}, 10_000
      wait_until_gone(slot)
      # no explicit drop: the halt precedes slot creation on this path, and the
      # cleanup_slot net tolerates an already-absent slot

      # --- opt-in: the pipeline starts and streams INSERTS (insert-only semantics) ---
      slot2 = "rep_halt_kl2_#{System.unique_integer([:positive])}"
      cleanup_slot(url, slot2)

      {:ok, _} =
        Replicant.start_link(
          connection: conn_opts(url),
          slot_name: slot2,
          sink: ParitySink,
          go_forward_only: true,
          decoder: :wal2json,
          tables: [{"public", "halt_keyless"}],
          allow_keyless_tables: true
        )

      mark = ParitySink.mark()

      PG16.wait_until(fn ->
        rows =
          Postgrex.query!(
            ctrl,
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
            [slot2]
          ).rows

        rows != [] and rows != [[nil]]
      end)

      :timer.sleep(500)
      Postgrex.query!(ctrl, "INSERT INTO halt_keyless VALUES ('k')", [])

      PG16.wait_until(fn ->
        ParitySink.since(mark) |> Enum.flat_map(& &1.changes) != []
      end)

      Replicant.stop(slot2)
      GenServer.stop(ctrl)
    end
  end

  describe "REPLICA IDENTITY NOTHING (the OBSERVED per-decoder divergences)" do
    @describetag :pg_old_decoders

    test "pgoutput never carries keyless changes: the SERVER refuses them (55000)" do
      # Publications (and the update-publishing refusal this proves) are PG10+; on
      # the 9.6 row the plugin decoders own the substrate and publications do not
      # exist — the leg is skipped there (the gate RETURNS before any DDL, so no
      # publication SQL ever reaches the 9.x parser).
      if Replicant.TestHelper.server_version_num() < 100_000 do
        IO.puts("pgoutput RI-NOTHING refusal skipped: publications are PG10+")
      else
        pgoutput_keyless_refusal()
      end
    end

    defp pgoutput_keyless_refusal do
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

    test "wal2json delivers the insert and drops BOTH keyless changes plugin-side" do
      url = System.fetch_env!("REPLICANT_TEST_URL")
      {:ok, ctrl} = Postgrex.start_link(conn_opts(url) ++ [pool_size: 2, backoff_type: :stop])
      Postgrex.query!(ctrl, "DROP TABLE IF EXISTS halt_nothing_w2j CASCADE", [])
      Postgrex.query!(ctrl, "CREATE TABLE halt_nothing_w2j (id int PRIMARY KEY, v text)", [])
      Postgrex.query!(ctrl, "ALTER TABLE halt_nothing_w2j REPLICA IDENTITY NOTHING", [])

      slot = "rep_halt_w2j_#{System.unique_integer([:positive])}"
      cleanup_slot(url, slot)

      {:ok, _} =
        Replicant.start_link(
          connection: conn_opts(url),
          slot_name: slot,
          sink: ParitySink,
          go_forward_only: true,
          decoder: :wal2json,
          tables: [{"public", "halt_nothing_w2j"}],
          # this table is DELIBERATELY keyless (RI NOTHING clears rd_replidindex even
          # with the PK) — this leg tests the insert-delivers/keyless-drop semantics,
          # so it takes the opt-in; the DEFAULT halt is the new leg below
          allow_keyless_tables: true
        )

      # both halves load-bearing (`rows != []` — slot exists, start_link returns
      # before the connect chain creates it; `rows != [[nil]]` — past the
      # consistent point); the [[nil]]-only form passes vacuously pre-creation
      PG16.wait_until(fn ->
        rows =
          Postgrex.query!(
            ctrl,
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
            [slot]
          ).rows

        rows != [] and rows != [[nil]]
      end)

      # settle: same fresh-slot decode-start race as the drift test
      :timer.sleep(500)

      mark = ParitySink.mark()

      Postgrex.query!(ctrl, "INSERT INTO halt_nothing_w2j VALUES (1, 'a')", [])

      PG16.wait_until(fn ->
        normalize(ParitySink.since(mark)) |> Enum.flat_map(& &1.changes) != []
      end)

      # Absence is proven PAST the write WAL, not by a clock: capture the slot's
      # confirmed_flush AFTER the insert landed, then after the U/D wait until it
      # advances beyond that point — the server has decoded everything through the
      # DELETE, so a plugin-side drop is distinguished from a slow delivery.
      {:ok, %Postgrex.Result{rows: [[pre_lsn_text]]}} =
        Postgrex.query(
          ctrl,
          "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
          [slot]
        )

      {:ok, pre_lsn} = Replicant.lsn_from_string(pre_lsn_text)

      Postgrex.query!(ctrl, "UPDATE halt_nothing_w2j SET v = 'b' WHERE id = 1", [])
      Postgrex.query!(ctrl, "DELETE FROM halt_nothing_w2j WHERE id = 1", [])

      # force WAL generation + flush past the delete so the advance is observable
      # (pg_switch_wal is the PG10+ name; 9.6 calls it pg_switch_xlog)
      switch =
        if Replicant.TestHelper.server_version_num() >= 100_000,
          do: "pg_switch_wal",
          else: "pg_switch_xlog"

      Postgrex.query!(ctrl, "SELECT #{switch}()", [])

      PG16.wait_until(fn ->
        {:ok, %Postgrex.Result{rows: [[cur]]}} =
          Postgrex.query(
            ctrl,
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            [slot]
          )

        {:ok, cur_lsn} = Replicant.lsn_from_string(cur)
        cur_lsn > pre_lsn
      end)

      ops =
        normalize(ParitySink.since(mark))
        |> Enum.flat_map(& &1.changes)
        |> Enum.map(& &1.op)

      # OBSERVED (wal2json.c): a table with no replica-identity index and identity ≠
      # FULL has its updates AND deletes silently dropped plugin-side — the insert is
      # all that ever arrives. Documented to operators (ADR-0009 §6 correction).
      assert ops == [:insert]

      Replicant.stop(slot)
    end
  end

  describe "wal2json schema-drift re-emit (ADR-0009 §5)" do
    @tag :pg_old_decoders
    test "a new column mid-stream re-emits the relation and classifies additively" do
      unless PG16.enabled?() do
        flunk("REPLICANT_TEST_URL not set")
      end

      url = System.fetch_env!("REPLICANT_TEST_URL")
      {:ok, ctrl} = Postgrex.start_link(conn_opts(url) ++ [pool_size: 2, backoff_type: :stop])
      Postgrex.query!(ctrl, "DROP TABLE IF EXISTS halt_drift CASCADE", [])
      Postgrex.query!(ctrl, "CREATE TABLE halt_drift (id int PRIMARY KEY, v text)", [])

      slot = "rep_halt_drift_#{System.unique_integer([:positive])}"
      cleanup_slot(url, slot)

      {:ok, _} =
        Replicant.start_link(
          connection: conn_opts(url),
          slot_name: slot,
          sink: ParitySink,
          go_forward_only: true,
          decoder: :wal2json,
          tables: [{"public", "halt_drift"}]
        )

      # both halves load-bearing (`rows != []` — slot exists, start_link returns
      # before the connect chain creates it; `rows != [[nil]]` — past the
      # consistent point); the [[nil]]-only form passes vacuously pre-creation
      PG16.wait_until(fn ->
        rows =
          Postgrex.query!(
            ctrl,
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
            [slot]
          ).rows

        rows != [] and rows != [[nil]]
      end)

      # settle: a write racing the fresh slot's decode-start window can be skipped
      # server-side (OBSERVED: the first of three staggered inserts lost) — let the
      # walsender establish its start point before the asserted writes.
      :timer.sleep(500)

      mark = ParitySink.mark()
      Postgrex.query!(ctrl, "INSERT INTO halt_drift VALUES (1, 'a')", [])

      PG16.wait_until(fn ->
        ParitySink.since(mark) != []
      end)

      # the unannounced schema change: a new column arrives with the NEXT change
      Postgrex.query!(ctrl, "ALTER TABLE halt_drift ADD COLUMN w text", [])
      Postgrex.query!(ctrl, "UPDATE halt_drift SET w = 'new' WHERE id = 1", [])

      PG16.wait_until(fn ->
        ParitySink.since(mark)
        |> Enum.flat_map(& &1.changes)
        |> Enum.any?(&(&1.op == :update and &1.record["w"] == "new"))
      end)

      # the relation re-emit classified ADDITIVELY (no destructive halt) and the
      # pipeline is still alive
      assert Registry.lookup(Replicant.Registry, {slot, :pipeline}) != []
      Replicant.stop(slot)
    end
  end
end

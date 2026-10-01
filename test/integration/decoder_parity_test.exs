defmodule Replicant.DecoderParityTest do
  @moduledoc """
  ADR-0009's marquee acceptance: ONE committed fixture delivered through pgoutput on a
  15+ server and through the pglogical and wal2json decoders on the old-major servers,
  with the delivered `%Replicant.Transaction{}` lists byte-identical after `commit_lsn`,
  `xid` and timestamps are normalized. A difference is a failure, not a documented
  deviation.

  Legs (each skips, never passes vacuously, when its server is not configured):
    * `REPLICANT_TEST_URL` ≥ 15 with `pgoutput` — the reference (also carries the
      truncate + transactional-message legs the old servers cannot);
    * `REPLICANT_TEST_URL` (the 12 row) with `:pglogical` and `:wal2json`;
    * `REPLICANT_PG96_URL` (the 9.6 row) with `:pglogical` and `:wal2json`.

  The common core (every type, unchanged TOAST, all key-carrying replica identities,
  the REPLICA IDENTITY NOTHING update/delete behavior) is compared across ALL legs.
  """

  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag timeout: 180_000

  alias Replicant.Test.{DecoderParity, ParitySink, PG16}

  import Replicant.Test.DecoderParity, only: [conn_opts: 1]

  @pg96_url System.get_env("REPLICANT_PG96_URL", "")

  defp pg96_enabled?, do: @pg96_url not in [nil, ""]

  defp major(url) do
    {:ok, pid} = Postgrex.start_link(conn_opts(url))

    [[v]] =
      Postgrex.query!(pid, "SELECT current_setting('server_version_num')::int", []).rows

    GenServer.stop(pid)
    v
  catch
    _, _ -> 0
  end

  defp leg(url, decoder, variant, tag) do
    uniq = System.unique_integer([:positive])

    {:ok, ctrl} =
      Postgrex.start_link(conn_opts(url) ++ [pool_size: 2, backoff_type: :stop])

    slot = "rep_parity_#{decoder}_#{tag}_#{uniq}"
    publication = "parity_pub_#{uniq}"

    case decoder do
      :pgoutput -> DecoderParity.setup!(ctrl, decoder, publication: publication)
      :pglogical -> DecoderParity.setup!(ctrl, decoder)
      :wal2json -> DecoderParity.setup!(ctrl, decoder)
    end

    drop_slot(ctrl, slot)

    extra =
      case decoder do
        :pgoutput ->
          [decoder: :pgoutput, publication: publication]

        :pglogical ->
          [decoder: :pglogical, replication_sets: ["default"]]

        :wal2json ->
          # the fixture DELIBERATELY carries a keyless table (parity_nothing — its
          # insert-delivers/keyless-changes-drop legs ARE the documented divergence),
          # so this harness opts into insert-only semantics for it (the default is a
          # fail-closed start halt; proven by the decoder_halts keyless leg)
          [
            decoder: :wal2json,
            tables: Enum.map(DecoderParity.tables(), &{"public", &1}),
            allow_keyless_tables: true
          ]
      end

    on_exit(fn ->
      Replicant.stop(slot)
      {:ok, c} = Postgrex.start_link(conn_opts(url))
      drop_slot(c, slot)
      GenServer.stop(c)
    end)

    mark = ParitySink.mark()

    {:ok, _} =
      Replicant.start_link(
        [
          connection: conn_opts(url),
          slot_name: slot,
          sink: ParitySink,
          go_forward_only: true
        ] ++ extra
      )

    # A FRESH slot only sees commits AFTER its creation point, and `start_link/1`
    # returns before the connect chain has created it — waits here, or the fixture
    # would race into the never-decoded window (probed live; see ADR-0009 testing).
    PG16.wait_until(fn ->
      rows =
        Postgrex.query!(
          ctrl,
          "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
          [slot]
        ).rows

      rows != [] and rows != [[nil]]
    end)

    # settle: writes racing a fresh slot's decode-start window can be skipped
    # server-side (OBSERVED live: the first of three staggered inserts lost) — the
    # boundary is PostgreSQL's fresh-slot semantics, not a decoder behavior.
    :timer.sleep(500)

    DecoderParity.apply_fixture!(ctrl, variant)

    # The fixture is ONE transaction; the core variant carries 9 changes, the
    # truncate+message variant 12 (counted below) — wait for a non-empty delivery
    # and let it settle before snapshotting.
    PG16.wait_until(fn ->
      txns = ParitySink.since(mark)
      txns != [] and hd(txns).changes != []
    end)

    Process.sleep(500)
    txns = ParitySink.since(mark)
    Replicant.stop(slot)
    PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 200)
    drop_slot(ctrl, slot)
    GenServer.stop(ctrl)

    DecoderParity.normalize(txns)
  end

  defp drop_slot(ctrl, slot) do
    Postgrex.query!(
      ctrl,
      "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )

    Postgrex.query!(
      ctrl,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  rescue
    _ -> :ok
  end

  setup do
    unless PG16.enabled?() do
      :ok
    end

    {:ok, _} = ParitySink.start_link()

    # Plugin availability (a leg skips — never passes vacuously — when the connected
    # server lacks its output plugin; the dedicated 9.6/12 rows exercise every leg).
    {:ok, %{avail: plugin_availability()}}
  end

  defp plugin_availability do
    if PG16.enabled?() do
      {:ok, ctrl} = Postgrex.start_link(conn_opts(System.fetch_env!("REPLICANT_TEST_URL")))

      pglogical? =
        Postgrex.query!(
          ctrl,
          "SELECT count(*) FROM pg_available_extensions WHERE name = 'pglogical'",
          []
        ).rows ==
          [[1]]

      wal2json? =
        try do
          Postgrex.query!(
            ctrl,
            "SELECT * FROM pg_create_logical_replication_slot('probe_avail_w2j', 'wal2json')",
            []
          )

          Postgrex.query!(ctrl, "SELECT pg_drop_replication_slot('probe_avail_w2j')", [])
          true
        rescue
          _ -> false
        end

      GenServer.stop(ctrl)
      %{pglogical: pglogical?, wal2json: wal2json?}
    else
      %{pglogical: false, wal2json: false}
    end
  end

  @tag :pg_old_decoders
  test "the fixture delivers byte-identically across pgoutput, pglogical and wal2json", %{
    avail: avail
  } do
    unless PG16.enabled?() do
      flunk("REPLICANT_TEST_URL not set — the parity marquee cannot run vacuously")
    end

    version = major(System.fetch_env!("REPLICANT_TEST_URL"))

    # --- the old-major legs (core variant; 9.6 cannot carry truncate or messages).
    # A leg whose plugin the server lacks runs as nil and joins only the comparisons
    # whose other side ran (skip, never vacuous pass). ---
    assert avail.pglogical or avail.wal2json,
           "server carries neither plugin — the parity legs cannot run vacuously"

    pg12_pglogical =
      if avail.pglogical,
        do: leg(System.fetch_env!("REPLICANT_TEST_URL"), :pglogical, :core, "12")

    pg12_wal2json =
      if avail.wal2json,
        do: leg(System.fetch_env!("REPLICANT_TEST_URL"), :wal2json, :core, "12")

    # Every leg must have DELIVERED the fixture's changes (not vacuously equal empties).
    assert table_changes(pg12_pglogical, "parity_all") != [], "pglogical@12 delivered nothing"
    assert table_changes(pg12_wal2json, "parity_all") != [], "wal2json@12 delivered nothing"

    # The compared core: the every-type DEFAULT table (incl. unchanged TOAST) and the
    # USING-INDEX table's update+delete — the tables ALL decoders carry identically.
    # parity_full (RI FULL) is pgoutput/wal2json-only under pglogical (OBSERVED plugin
    # constraint: no usable identity index ⇒ insert-only set); parity_nothing diverges
    # BY PLUGIN (wal2json drops the keyless update server-side) — both asserted in
    # DecoderHaltsTest.
    assert table_changes(pg12_pglogical, "parity_all") ==
             table_changes(pg12_wal2json, "parity_all"),
           diff_legs(pg12_pglogical, pg12_wal2json, "parity_all")

    if pg12_pglogical != nil and pg12_wal2json != nil do
      assert table_changes(pg12_pglogical, "parity_idx") ==
               table_changes(pg12_wal2json, "parity_idx"),
             diff_legs(pg12_pglogical, pg12_wal2json, "parity_idx")
    end

    if pg96_enabled?() do
      pg96_pglogical = if avail.pglogical, do: leg(@pg96_url, :pglogical, :core, "96")
      pg96_wal2json = if avail.wal2json, do: leg(@pg96_url, :wal2json, :core, "96")

      if pg96_pglogical != nil and pg12_pglogical != nil do
        assert table_changes(pg96_pglogical, "parity_all") ==
                 table_changes(pg12_pglogical, "parity_all"),
               diff_legs(pg96_pglogical, pg12_pglogical, "parity_all")
      end

      if pg96_wal2json != nil and pg12_wal2json != nil do
        assert table_changes(pg96_wal2json, "parity_all") ==
                 table_changes(pg12_wal2json, "parity_all"),
               diff_legs(pg96_wal2json, pg12_wal2json, "parity_all")
      end

      if pg96_pglogical != nil and pg96_wal2json != nil do
        assert table_changes(pg96_pglogical, "parity_idx") ==
                 table_changes(pg96_wal2json, "parity_idx")
      end
    end

    # --- the pgoutput reference (15+; carries truncate + the transactional message) ---
    if version >= 150_000 do
      pgoutput_full =
        leg(System.fetch_env!("REPLICANT_TEST_URL"), :pgoutput, :truncate_message, "pg")

      wal2json_full =
        if avail.wal2json,
          do: leg(System.fetch_env!("REPLICANT_TEST_URL"), :wal2json, :truncate_message, "pg")

      assert pgoutput_full != [], "pgoutput delivered nothing"

      if wal2json_full != nil do
        for table <- ["parity_all", "parity_idx", "parity_full"] do
          assert table_changes(pgoutput_full, table) == table_changes(wal2json_full, table),
                 diff_legs(pgoutput_full, wal2json_full, table)
        end
      end

      if pg12_pglogical != nil do
        assert table_changes(pgoutput_full, "parity_all") ==
                 table_changes(pg12_pglogical, "parity_all"),
               diff_legs(pgoutput_full, pg12_pglogical, "parity_all")
      end

      # the truncate + transactional message ride only where a plugin+server carry
      # them (pgoutput, wal2json on PG >= 11/10)
      assert truncate_count(pgoutput_full) == 1
      assert message_count(pgoutput_full) == 1

      if wal2json_full != nil do
        assert truncate_count(wal2json_full) == 1
        assert message_count(wal2json_full) == 1
      end
    end
  end

  # A readable failure message: the per-table projections of both legs.
  defp diff_legs(a, b, table) do
    ca = table_changes(a, table)
    cb = table_changes(b, table)

    "decoder legs disagree on " <>
      table <>
      ":\nleft:  " <>
      inspect(ca, limit: :infinity, pretty: true) <>
      "\nright: " <> inspect(cb, limit: :infinity, pretty: true)
  end

  defp table_changes(txns, table) do
    txns
    |> Enum.flat_map(& &1.changes)
    |> Enum.filter(&(&1.table == table))
    |> Enum.map(fn ch ->
      %{
        op: ch.op,
        record: ch.record,
        old_record: ch.old_record,
        unchanged: Enum.sort(ch.unchanged || [])
      }
    end)
  end

  defp truncate_count(txns) do
    txns |> Enum.flat_map(& &1.changes) |> Enum.count(&(&1.op == :truncate))
  end

  defp message_count(txns) do
    txns |> Enum.flat_map(&(&1.messages || [])) |> Enum.count()
  end
end

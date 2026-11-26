defmodule Replicant.DecoderParityTest do
  @moduledoc """
  ADR-0009's marquee acceptance: ONE committed fixture delivered through pgoutput AND
  through the pglogical and wal2json decoders, with the delivered
  `%Replicant.Transaction{}` lists byte-identical after `commit_lsn`, `xid` and
  timestamps are normalized. A difference is a failure, not a documented deviation.

  Geometry (fail-loud, never silently narrowed — a missing expected input flunks
  rather than shrinking the comparison):
    * the primary (`REPLICANT_TEST_URL`) carries pglogical and runs its reference
      through pgoutput on the same server. Rows declaring wal2json also compare
      its core delivery and, on 15+, the truncate + transactional-message variant;
    * a pre-15 plugin row must ALSO wire `REPLICANT_PG96_URL` to a real 9.6
      carrying both plugins (CI starts the 9.6 beside the 12) — the cross-vintage
      plugin legs are part of the acceptance, not an optional extra;
    * on a 15+ primary carrying the plugins, the 9.6 legs run when the URL is set
      and are simply absent otherwise (no CI lane wires that shape today).

  The common core (every type, NULL scalars, extreme-magnitude floats, unchanged
  TOAST, all key-carrying replica identities, the REPLICA IDENTITY NOTHING
  insert-delivery behavior) is compared across ALL legs.
  """

  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag :pg10
  @moduletag timeout: 180_000

  alias Replicant.Test.{DecoderParity, ParitySink, PG16}

  import Replicant.Test.DecoderParity, only: [conn_opts: 1]

  @pg96_url System.get_env("REPLICANT_PG96_URL", "")

  defp pg96_enabled?, do: @pg96_url not in [nil, ""]

  # FAIL-CLOSED: a version probe that cannot reach its server raises (a catch->0
  # here would let a transient blip flip a 15+ primary to "0" and silently drop the
  # full-variant legs while the run still passes — the fresh-review F2/F10 finding)
  defp major(url) do
    {:ok, pid} = Postgrex.start_link(conn_opts(url) ++ [sync_connect: true, backoff_type: :stop])

    [[v]] =
      Postgrex.query!(pid, "SELECT current_setting('server_version_num')::int", []).rows

    GenServer.stop(pid)
    v
  end

  defp plugin_availability_on(url) do
    {:ok, ctrl} = Postgrex.start_link(conn_opts(url) ++ [backoff_type: :stop])

    try do
      pglogical? =
        Postgrex.query!(
          ctrl,
          "SELECT count(*) FROM pg_available_extensions WHERE name = 'pglogical'",
          []
        ).rows == [[1]]

      %{pglogical: pglogical?, wal2json: wal2json_available?(ctrl)}
    after
      GenServer.stop(ctrl)
    end
  end

  # a UNIQUE slot name per probe: a fixed name makes one aborted run's leftover slot
  # flunk the next run's probe (a self-inflicted false "wal2json unavailable")
  defp wal2json_available?(ctrl) do
    slot = "probe_avail_w2j_#{System.unique_integer([:positive])}"

    Postgrex.query!(ctrl, "SELECT * FROM pg_create_logical_replication_slot($1, 'wal2json')", [
      slot
    ])

    Postgrex.query!(ctrl, "SELECT pg_drop_replication_slot($1)", [slot])
    true
  rescue
    _ -> false
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

    extra = leg_extra(decoder, variant, publication)

    on_exit(fn ->
      Replicant.stop(slot)
      {:ok, c} = Postgrex.start_link(conn_opts(url))

      if decoder == :pgoutput do
        Postgrex.query!(c, "DROP PUBLICATION IF EXISTS #{publication}", [])
        Postgrex.query!(c, "DROP PUBLICATION IF EXISTS #{publication}_nothing", [])
      end

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

    # The fixture is ONE transaction; the core variant carries 10 changes (incl. the
    # NULL + extreme-float row), the truncate+message variant 12 changes plus one
    # transactional message — wait for a non-empty delivery and let it settle
    # before snapshotting.
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

  # The per-decoder pipeline extras for one leg. The keyless parity_nothing table
  # rides insert-only semantics on EVERY decoder (its keyless U/D divergences are
  # the documented, per-decoder-tested behavior — see DecoderHaltsTest): its own
  # insert-only publication for pgoutput (setup! creates both from the base name),
  # both pglogical sets (default + default_insert_only), and wal2json's explicit
  # allow_keyless_tables opt-in. The truncate+message variant adds messages: true
  # — pgoutput suppresses M frames server-side without the option, so the message
  # assert could never pass without it (ParitySink implements handle_message/2 for
  # the config capability gate).
  defp leg_extra(:pgoutput, variant, publication) do
    base = [decoder: :pgoutput, publication: [publication, publication <> "_nothing"]]
    if variant == :truncate_message, do: base ++ [messages: true], else: base
  end

  defp leg_extra(:pglogical, _variant, _publication) do
    [decoder: :pglogical, replication_sets: ["default", "default_insert_only"]]
  end

  defp leg_extra(:wal2json, _variant, _publication) do
    [
      decoder: :wal2json,
      tables: Enum.map(DecoderParity.tables(), &{"public", &1}),
      allow_keyless_tables: true
    ]
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
    if PG16.enabled?() do
      start_supervised!(ParitySink)

      # Plugin availability on BOTH wired servers: the marquee FAILS LOUD when an
      # expected plugin is absent (a missing leg narrows the comparison silently —
      # the exact vacuity this suite refuses).
      avail = plugin_availability_on(System.fetch_env!("REPLICANT_TEST_URL"))

      pg96_avail =
        if pg96_enabled?(), do: plugin_availability_on(@pg96_url), else: nil

      {:ok, %{avail: avail, pg96_avail: pg96_avail}}
    else
      # no primary URL: the test body's flunk delivers the clear message (probing
      # here would only raise a fuzzier fetch_env! error first)
      {:ok, %{avail: %{pglogical: false, wal2json: false}, pg96_avail: nil}}
    end
  end

  @tag :pglogical
  test "the fixture delivers byte-identically across the applicable decoders", %{
    avail: avail,
    pg96_avail: pg96_avail
  } do
    unless PG16.enabled?() do
      flunk("REPLICANT_TEST_URL not set — the parity marquee cannot run vacuously")
    end

    url = System.fetch_env!("REPLICANT_TEST_URL")
    version = major(url)

    assert avail.pglogical, "primary carries no pglogical"

    expect_pg96? = version < 150_000

    expect_wal2json? =
      pg96_enabled?() or expect_pg96? or
        "wal2json" in String.split(System.get_env("EXPECTED_PG_PLUGINS", ""), ",")

    if expect_wal2json?, do: assert(avail.wal2json, "primary carries no wal2json")

    if expect_pg96? and not pg96_enabled?() do
      flunk(
        "REPLICANT_PG96_URL is unset on a pre-15 plugin row — the 9.6 parity legs are part of the acceptance (CI wires the 9.6 beside the 12); refusing to narrow"
      )
    end

    if pg96_enabled?() do
      # the secondary must really be the old vintage — a URL accidentally pointing
      # back at the primary would silently turn the cross-vintage comparison into a
      # same-server one
      assert major(@pg96_url) < 100_000,
             "REPLICANT_PG96_URL is not a pre-10 server — refusing a vacuous cross-vintage comparison"

      assert pg96_avail.pglogical,
             "REPLICANT_PG96_URL server carries no pglogical — the 9.6 legs cannot narrow"

      assert pg96_avail.wal2json,
             "REPLICANT_PG96_URL server carries no wal2json — the 9.6 legs cannot narrow"
    end

    pglogical = leg(url, :pglogical, :core, "pgl")
    pgoutput_core = leg(url, :pgoutput, :core, "pg")

    assert_core_counts(pglogical, 1, "pglogical")
    assert_core_counts(pgoutput_core, 3, "pgoutput")

    # The pglogical fixture puts RI FULL in the insert-only set. Compare its
    # INSERT and all changes from the other tables at the full field projection.
    for {table, op} <- [
          {"parity_all", nil},
          {"parity_idx", nil},
          {"parity_nothing", nil},
          {"parity_full", :insert}
        ] do
      assert table_changes(pgoutput_core, table, op) == table_changes(pglogical, table, op),
             diff_legs(pgoutput_core, pglogical, table)
    end

    wal2json =
      if avail.wal2json do
        deliveries = leg(url, :wal2json, :core, "w2j")
        assert_core_counts(deliveries, 3, "wal2json")
        assert pgoutput_core == deliveries
        deliveries
      end

    # Compare the legacy source's core deliveries with the primary table by table.
    if pg96_enabled?() do
      pg96_pglogical = leg(@pg96_url, :pglogical, :core, "96")
      pg96_wal2json = leg(@pg96_url, :wal2json, :core, "96")

      assert_change_counts(
        pg96_pglogical,
        %{
          "parity_all" => 3,
          "parity_idx" => 3,
          "parity_full" => 1,
          "parity_nothing" => 1
        },
        "pglogical@9.6"
      )

      assert_change_counts(
        pg96_wal2json,
        %{
          "parity_all" => 3,
          "parity_idx" => 3,
          "parity_full" => 3,
          "parity_nothing" => 1
        },
        "wal2json@9.6"
      )

      for {a, b, table, op} <- [
            {pg96_pglogical, pglogical, "parity_all", nil},
            {pg96_pglogical, pglogical, "parity_idx", nil},
            {pg96_pglogical, pglogical, "parity_nothing", nil},
            {pg96_wal2json, wal2json, "parity_all", nil},
            {pg96_wal2json, wal2json, "parity_idx", nil},
            {pg96_wal2json, wal2json, "parity_full", nil},
            {pg96_wal2json, wal2json, "parity_nothing", nil},
            {pg96_pglogical, pg96_wal2json, "parity_idx", nil},
            {pg96_pglogical, pgoutput_core, "parity_all", nil},
            {pg96_wal2json, pgoutput_core, "parity_all", nil},
            {pg96_pglogical, pgoutput_core, "parity_full", :insert}
          ] do
        assert table_changes(a, table, op) == table_changes(b, table, op), diff_legs(a, b, table)
      end
    end

    if avail.wal2json and version >= 150_000 do
      pgoutput_full = leg(url, :pgoutput, :truncate_message, "pgfull")
      wal2json_full = leg(url, :wal2json, :truncate_message, "pgfull")

      assert truncate_count(pgoutput_full) == 1
      assert message_count(pgoutput_full) == 1
      assert truncate_count(wal2json_full) == 1
      assert message_count(wal2json_full) == 1

      assert pgoutput_full == wal2json_full,
             "pgoutput and wal2json full-variant deliveries differ:\nleft:  " <>
               inspect(pgoutput_full, limit: :infinity, pretty: true) <>
               "\nright: " <> inspect(wal2json_full, limit: :infinity, pretty: true)
    end

    major = div(version, 10_000)

    if expected = System.get_env("EXPECTED_PG_MAJOR") do
      assert Integer.to_string(major) == expected
    end

    IO.puts("PGLOGICAL-PARITY-RECEIPT pg=#{major}")
  end

  defp assert_core_counts(txns, full_count, label) do
    assert_change_counts(
      txns,
      %{
        "parity_all" => 3,
        "parity_idx" => 3,
        "parity_full" => full_count,
        "parity_nothing" => 1
      },
      label
    )
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

  # The FULL per-change projection of one table's delivery: every field the
  # normalized %Transaction{} carries per change (op, schema, table, record,
  # old_record, the sorted unchanged list, and the column metadata). The marquee
  # compares THIS — a narrowed field subset would let a divergence in any dropped
  # field pass silently (review finding: the old projection omitted schema and
  # column metadata entirely).
  defp table_changes(txns, table, op \\ nil) do
    txns
    |> Enum.flat_map(& &1.changes)
    |> Enum.filter(&(&1.table == table and (op == nil or &1.op == op)))
    |> Enum.map(fn ch ->
      %{
        op: ch.op,
        schema: ch.schema,
        table: ch.table,
        record: ch.record,
        old_record: ch.old_record,
        unchanged: Enum.sort(ch.unchanged || []),
        columns: ch.columns
      }
    end)
  end

  # Exact per-table change counts — the marquee's non-vacuity floor: a leg that
  # delivered NOTHING for a compared table cannot pass (a bare != [] assert guards
  # only one table per leg; fresh-review F6).
  defp assert_change_counts(txns, expected, label) do
    actual =
      Enum.map(expected, fn {table, _} -> {table, length(table_changes(txns, table))} end)

    assert actual == Enum.to_list(expected),
           "#{label} delivered wrong change counts: #{inspect(actual)} (expected #{inspect(Enum.to_list(expected))})"
  end

  defp truncate_count(txns) do
    txns |> Enum.flat_map(& &1.changes) |> Enum.count(&(&1.op == :truncate))
  end

  defp message_count(txns) do
    txns |> Enum.flat_map(&(&1.messages || [])) |> Enum.count()
  end
end

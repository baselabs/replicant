defmodule Replicant.DecoderCrashResumeTest do
  @moduledoc """
  ADR-0009's crash-injection marquee (loss = 0, effect-dup = 0 through an
  idempotent sink), once per plugin decoder on the live old-major server: N committed
  transactions are written while the pipeline streams, the pipeline is killed
  mid-stream, restarted, and the union of both runs' deliveries must contain every
  transaction AT LEAST ONCE with duplicates bounded to the crash window (the slot
  retains everything past its confirmed_flush) — and an upsert-by-PK ledger turns
  those deliveries into exactly one effect per row, the Critical Rule 3 construction.
  """

  use ExUnit.Case, async: false
  @moduletag :integration
  # A primary-server plugin-decoder leg (like the parity/halts trio): excluded when
  # the primary is PG15+ (no plugins) or pre-10 (the legs assume a PG10+ primary);
  # verified green on the 12-primary wiring with the 9.6 secondary.
  @moduletag :pg_old_decoders
  @moduletag timeout: 120_000

  alias Replicant.Test.{ParitySink, PG16}

  import Replicant.Test.DecoderParity, only: [conn_opts: 1]

  @txns 8

  setup do
    unless PG16.enabled?() do
      :ok
    end

    {:ok, _} = ParitySink.start_link()
    :ok
  end

  for {decoder, extra} <- [
        {:pglogical, [replication_sets: ["default"]]},
        {:wal2json, [tables: [{"public", "crash_t"}]]}
      ] do
    @tag decoder: decoder
    test "crash + resume through #{decoder}: every transaction delivered exactly once", %{
      decoder: unquote(decoder)
    } do
      unless PG16.enabled?() do
        flunk("REPLICANT_TEST_URL not set")
      end

      url = System.fetch_env!("REPLICANT_TEST_URL")
      decoder = unquote(decoder)
      extra = unquote(Macro.escape(extra))

      {:ok, ctrl} =
        Postgrex.start_link(conn_opts(url) ++ [pool_size: 2, backoff_type: :stop])

      Postgrex.query!(ctrl, "DROP TABLE IF EXISTS crash_t CASCADE", [])
      Postgrex.query!(ctrl, "CREATE TABLE crash_t (id int PRIMARY KEY, v text)", [])

      # pglogical-only set membership. The comparison rides unquote(decoder) — the
      # compile-time leg literal — so each leg's arm is dead-code-eliminated and the
      # type checker sees no disjoint comparison (a bare `decoder == :pglogical`
      # narrows decoder to :wal2json on the other leg and warns).
      if unquote(decoder) == :pglogical do
        Postgrex.query!(
          ctrl,
          "SELECT pglogical.replication_set_add_table('default', 'public.crash_t', false)",
          []
        )
      end

      slot = "rep_crash_#{decoder}_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        try do
          {:ok, c} = Postgrex.start_link(conn_opts(url) ++ [backoff_type: :stop])

          Postgrex.query(
            c,
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1",
            [slot]
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

      base =
        [
          connection: conn_opts(url),
          slot_name: slot,
          sink: ParitySink,
          go_forward_only: true,
          decoder: decoder
        ] ++ extra

      # --- run 1: stream, write, kill mid-stream ---
      {:ok, run1} = Replicant.start_link(base)
      mark0 = ParitySink.mark()

      # BOTH halves of the slot gate are load-bearing: `rows != []` (the slot
      # EXISTS — start_link returns before the connect chain creates it) and
      # `rows != [[nil]]` (creation finished past the consistent point). The
      # existence half alone-on-[[nil]] form passes VACUOUSLY on the no-row
      # case, and a warmup insert racing below the slot's creation point is
      # never streamed at all (fresh-slot semantics; OBSERVED under load,
      # 2026-09-30 — same gate as decoder_parity_test).
      PG16.wait_until(fn ->
        rows =
          Postgrex.query!(
            ctrl,
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
            [slot]
          ).rows

        rows != [] and rows != [[nil]]
      end)

      # Prove the stream is LIVE end-to-end before the asserted writes: a warmup
      # insert is delivered first, so every fixture row is committed AFTER decoding
      # started and cannot fall below the fresh slot's consistent point (PostgreSQL
      # does not stream transactions committed before slot creation — a fixed sleep
      # raced that boundary on the loaded 9.6 row, OBSERVED 2026-09-30).
      Postgrex.query!(ctrl, "INSERT INTO crash_t VALUES (0, 'warmup')", [])

      PG16.wait_until(fn ->
        0 in delivered_ids(ParitySink.since(mark0))
      end)

      mark1 = ParitySink.mark()
      Enum.each(1..@txns, &Postgrex.query!(ctrl, "INSERT INTO crash_t VALUES ($1, 'x')", [&1]))

      PG16.wait_until(fn ->
        delivered_ids(ParitySink.since(mark1)) != []
      end)

      # kill mid-stream (some transactions may be undelivered; the slot retains them)
      Process.exit(run1, :kill)

      # wait for the CONNECTION's registry name specifically — it clears after the
      # pipeline's own entry, and run 2's Connection would otherwise race the
      # teardown into {:already_started, _}
      PG16.wait_until(
        fn ->
          Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] and
            Registry.lookup(Replicant.Registry, {slot, :connection}) == []
        end,
        200
      )

      # ALSO wait for the SERVER to release the slot: a killed client's walsender
      # backend can linger seconds on a loaded server ("replication slot ... is
      # active for PID", OBSERVED on the 9.6 row), and a run-2 START_REPLICATION
      # against the still-active slot reconnect-loops instead of streaming. The
      # client-side registry clear does NOT bound this — only the server's own
      # `active` flag does.
      PG16.wait_until(
        fn ->
          Postgrex.query!(
            ctrl,
            "SELECT NOT active FROM pg_replication_slots WHERE slot_name = $1",
            [slot]
          ).rows == [[true]]
        end,
        400
      )

      # --- run 2: resume; every transaction must land exactly once overall ---
      {:ok, _run2} = Replicant.start_link(base)

      # a quiet stream needs a WAL nudge for the resumed run to drain the retained
      # transactions (logical decoding only wakes on new WAL)
      Postgrex.query!(ctrl, "INSERT INTO crash_t VALUES ($1, 'nudge')", [@txns + 1])

      PG16.wait_until(
        fn ->
          # wait on the FULL delivery window (mark1): a transaction run 1 already
          # acked will not re-stream, so run 2's own window cannot prove completeness
          ids = delivered_ids(ParitySink.since(mark1))
          missing = Enum.to_list(1..(@txns + 1)) -- Enum.uniq(ids)

          missing == []
        end,
        800
      )

      all = delivered_ids(ParitySink.since(mark1))
      # loss = 0: every id 1..@txns+1 delivered at least once
      assert Enum.sort(Enum.uniq(all)) == Enum.to_list(1..(@txns + 1))
      # at-least-once, duplicate-bounded: a re-delivered transaction may appear once
      # extra (the crash window between sink-ack and slot-advance) — never more
      assert Enum.all?(Enum.frequencies(all), fn {_id, n} -> n <= 2 end)
      # effect-dup = 0 THROUGH AN IDEMPOTENT SINK: the upsert-by-PK ledger leaves
      # exactly one effect per id (the Critical Rule 3 construction — delivery is
      # at-least-once; effect-once is the sink's half of the contract)
      effects = Map.new(all, &{&1, :applied})
      assert map_size(effects) == @txns + 1

      Replicant.stop(slot)
    end
  end

  defp delivered_ids(txns) do
    txns
    |> Enum.flat_map(& &1.changes)
    |> Enum.filter(&(&1.table == "crash_t" and &1.op == :insert))
    |> Enum.map(& &1.record["id"])
  end
end

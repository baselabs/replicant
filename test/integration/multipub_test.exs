defmodule Replicant.MultiPubTest do
  @moduledoc """
  Integration marquee for the A3 multi-publication feature (decision #18 / #19).

  Three live-PG marquees:
    1. both publications' changes deliver, overlap-deduped, in commit order.
    2. snapshot discovery UNIONS the publication list (DISTINCT over a shared table).
    3. a MISSING publication halts fail-closed at start (the A3 safety gate).

  Gated on `REPLICANT_TEST_URL` (see test/test_helper.exs): each test body is wrapped in
  `if PG16.enabled?() do ... end`, so the suite skips cleanly without a live PG (the
  `@moduletag :integration` excludes the module entirely when the URL is unset).
  """

  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.{Change, Transaction}
  alias Replicant.Test.PG16

  # A transactional sink that records every delivered change into `mp_sink_rows` (one row per
  # delivered change, tagged by source table) and the checkpoint into `mp_sink_cp`, atomically.
  # `mp_sink_rows` is append-only (no PK) so duplicate deliveries are detectable; `table` records
  # which publication's table the change came from (mp_shared for the overlap table, mp_p1_only
  # for p1's exclusive table).
  defmodule MultiPubSink do
    @moduledoc false
    @behaviour Replicant.Sink

    @conn Replicant.Test.MultiPubConn

    @impl true
    def checkpoint do
      case Postgrex.query(@conn, "SELECT lsn FROM mp_sink_cp WHERE id = 1", []) do
        {:ok, %Postgrex.Result{rows: [[lsn]]}} -> {:ok, lsn}
        {:ok, %Postgrex.Result{rows: []}} -> {:ok, nil}
        {:error, _} = err -> err
      end
    end

    @impl true
    def handle_transaction(%Transaction{commit_lsn: lsn, changes: changes}) do
      result =
        Postgrex.transaction(@conn, fn c ->
          Enum.each(changes, &apply_change(c, &1))

          Postgrex.query!(
            c,
            "INSERT INTO mp_sink_cp (id, lsn) VALUES (1, $1) " <>
              "ON CONFLICT (id) DO UPDATE SET lsn = EXCLUDED.lsn",
            [lsn]
          )
        end)

      case result do
        {:ok, _} -> {:ok, lsn}
        {:error, reason} -> {:error, reason}
      end
    end

    defp apply_change(c, %Change{op: op, table: table, record: r})
         when op in [:insert, :update] do
      Postgrex.query!(
        c,
        "INSERT INTO mp_sink_rows (id, source) VALUES ($1, $2)",
        [r["id"], table]
      )
    end

    defp apply_change(_c, _change), do: :ok

    # Snapshot delivery: append each snapshotted row into mp_sink_rows tagged by its source
    # table (mirrors handle_transaction's plain append — mp_sink_rows is append-only with no PK,
    # so a shared table discovered twice would show TWO rows, which is exactly what the DISTINCT
    # discovery marquee asserts against). NO blanket TRUNCATE on first_for_table?: this ledger
    # holds rows from MULTIPLE source tables, so truncating on one table's first batch would wipe
    # a sibling table's already-snapshotted rows. This marquee does not exercise mid-snapshot
    # redo, so per-table reset is unnecessary here.
    @impl true
    def handle_snapshot(changes, %{first_for_table?: _first?}) do
      Postgrex.transaction(@conn, fn c ->
        Enum.each(changes, fn %Change{table: table, record: r} ->
          Postgrex.query!(
            c,
            "INSERT INTO mp_sink_rows (id, source) VALUES ($1, $2)",
            [r["id"], table]
          )
        end)
      end)

      :ok
    rescue
      e -> {:error, e}
    end

    # The snapshot handoff commit: durably persist the known consistent point so checkpoint/0
    # resumes streaming from it.
    @impl true
    def handle_snapshot_complete(lsn) do
      Postgrex.query!(
        @conn,
        "INSERT INTO mp_sink_cp (id, lsn) VALUES (1, $1) " <>
          "ON CONFLICT (id) DO UPDATE SET lsn = EXCLUDED.lsn",
        [lsn]
      )

      {:ok, lsn}
    end
  end

  setup do
    {:ok, ctrl} =
      PG16.named_conn(Replicant.Test.MPCtrlConn, pool_size: 3)

    {:ok, _} =
      PG16.named_conn(Replicant.Test.MultiPubConn, pool_size: 2)

    slot = "rep_mp_#{System.unique_integer([:positive])}"
    reset_schema(ctrl)
    drop_slot(ctrl, slot)

    on_exit(fn ->
      Replicant.stop(slot)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 400)
      {:ok, c} = Postgrex.start_link(PG16.pg_opts())
      drop_slot(c, slot)
    end)

    %{ctrl: ctrl, slot: slot}
  end

  test "MARQUEE: delivers changes from both publications, overlap-deduped, in commit order",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      start_pipeline(slot)

      # Commit 1: insert into mp_p1_only (p1-only) and mp_shared (in BOTH pubs → overlap).
      Postgrex.transaction(ctrl, fn c ->
        Postgrex.query!(c, "INSERT INTO mp_p1_only (id, note) VALUES ($1, $2)", [1, "p1only"])
        Postgrex.query!(c, "INSERT INTO mp_shared (id, note) VALUES ($1, $2)", [10, "overlap"])
      end)

      # Commit 2: insert into mp_shared again (p2 carries mp_shared too).
      Postgrex.transaction(ctrl, fn c ->
        Postgrex.query!(c, "INSERT INTO mp_shared (id, note) VALUES ($1, $2)", [11, "p2too"])
      end)

      # Wait for BOTH commits to land (id 1, 10, 11 all delivered) — the overlap row (id 10, in
      # both publications) MUST deliver EXACTLY ONCE (decision #19 dedup; mp_sink_rows is append-
      # only so a double-delivery of id 10 would show two rows).
      PG16.wait_until(
        fn ->
          ids = delivered_ids(ctrl)
          MapSet.subset?(MapSet.new([1, 10, 11]), ids)
        end,
        800
      )

      rows = delivered_rows(ctrl)

      # All three changes delivered.
      assert MapSet.new(rows |> Enum.map(& &1.id)) == MapSet.new([1, 10, 11])

      # CRITICAL overlap-dedup assertion: id 10 (in BOTH mp_p1 and mp_p2) delivered EXACTLY ONCE.
      # pgoutput emits one change per publication; the dedup seam must collapse the duplicate.
      assert Enum.count(rows, &(&1.id == 10)) == 1

      # The shared table's delivered row's source is mp_shared (not duplicated as two sources).
      assert Enum.any?(rows, &(&1.id == 10 and &1.source == "mp_shared"))
      # The p1-only table's row's source is mp_p1_only.
      assert Enum.any?(rows, &(&1.id == 1 and &1.source == "mp_p1_only"))
    end
  end

  test "MARQUEE: snapshot discovery unions across the list (DISTINCT over a shared table)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      # Seed rows BEFORE starting the pipeline so a snapshot is required to deliver them.
      Postgrex.query!(ctrl, "INSERT INTO mp_shared (id, note) VALUES ($1, $2)", [100, "seed_a"])
      Postgrex.query!(ctrl, "INSERT INTO mp_p1_only (id, note) VALUES ($1, $2)", [101, "seed_b"])

      # snapshot mode is mutually exclusive with go_forward_only (config :conflicting_start_mode),
      # so this marquee overrides the helper's go-forward default.
      start_pipeline(slot, snapshot: true, go_forward_only: false)

      # The snapshot MUST union BOTH tables: mp_shared (in both pubs → DISTINCT collapses to ONE
      # discovery) and mp_p1_only (p1-only). Both seeded rows must land.
      PG16.wait_until(
        fn ->
          ids = delivered_ids(ctrl)
          MapSet.subset?(MapSet.new([100, 101]), ids)
        end,
        800
      )

      rows = delivered_rows(ctrl)

      # The shared table's seed row (id 100) appears in BOTH publications but the snapshot
      # discovery's DISTINCT collapses the pubname dimension → the row snapshots ONCE.
      assert Enum.count(rows, &(&1.id == 100)) == 1
      assert Enum.any?(rows, &(&1.id == 100 and &1.source == "mp_shared"))
      assert Enum.any?(rows, &(&1.id == 101 and &1.source == "mp_p1_only"))
    end
  end

  test "MARQUEE: a missing publication halts fail-closed at start", %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      # Attach a slot_invalidated probe — the A3 gate emits it with reason: :publication_missing
      # (mirrors the unit test in connection_test.exs).
      ref = make_ref()
      test_pid = self()

      :telemetry.attach(
        {__MODULE__, ref},
        [:replicant, :connection, :slot_invalidated],
        fn _e, _m, meta, _ -> send(test_pid, {:slot_invalidated, ref, meta}) end,
        nil
      )

      # Configure a publication list where mp_nonexistent is ABSENT from the server. The A3 gate
      # (handle_result :publication_check) MUST halt: the found set {mp_p1} ⊂ requested set
      # {mp_p1, mp_nonexistent}.
      {:ok, _} =
        Replicant.start_link(
          connection: PG16.pg_opts(),
          slot_name: slot,
          publication: ["mp_p1", "mp_nonexistent"],
          sink: MultiPubSink,
          go_forward_only: true
        )

      assert_receive {:slot_invalidated, ^ref, %{reason: :publication_missing}}, 15_000

      # The pipeline halts and tears down (Supervisor.halt is async) — the registry entry clears.
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 800)
      :telemetry.detach({__MODULE__, ref})

      # Nothing was delivered (the gate fired BEFORE streaming began).
      assert delivered_ids(ctrl) == MapSet.new()
    end
  end

  # ---- helpers ----

  defp start_pipeline(slot, opts \\ []) do
    ref = make_ref()
    test_pid = self()

    :telemetry.attach(
      {__MODULE__, ref},
      [:replicant, :connection, :slot_active],
      fn _e, _m, _meta, _ -> send(test_pid, {:slot_active, ref}) end,
      nil
    )

    {:ok, _} =
      Replicant.start_link(
        Keyword.merge(
          [
            connection: PG16.pg_opts(),
            slot_name: slot,
            publication: ["mp_p1", "mp_p2"],
            sink: MultiPubSink,
            go_forward_only: true
          ],
          opts
        )
      )

    receive do
      {:slot_active, ^ref} -> :ok
    after
      15_000 -> ExUnit.Assertions.flunk("pipeline never reached slot_active for #{slot}")
    end

    :telemetry.detach({__MODULE__, ref})
    PG16.wait_until(fn -> connection_pid(slot) != nil end, 800)
  end

  defp reset_schema(c) do
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS mp_p1", [])
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS mp_p2", [])

    Postgrex.query!(c, "DROP TABLE IF EXISTS mp_shared", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS mp_p1_only", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS mp_sink_rows", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS mp_sink_cp", [])

    Postgrex.query!(c, "CREATE TABLE mp_shared (id int PRIMARY KEY, note text)", [])
    Postgrex.query!(c, "CREATE TABLE mp_p1_only (id int PRIMARY KEY, note text)", [])
    # Append-only (no PK) so duplicate deliveries are detectable; `source` records which table.
    Postgrex.query!(c, "CREATE TABLE mp_sink_rows (id int, source text)", [])
    Postgrex.query!(c, "CREATE TABLE mp_sink_cp (id int PRIMARY KEY, lsn bigint)", [])

    # mp_shared is in BOTH publications (the overlap); mp_p1_only is p1-exclusive.
    Postgrex.query!(c, "CREATE PUBLICATION mp_p1 FOR TABLE mp_shared, mp_p1_only", [])
    Postgrex.query!(c, "CREATE PUBLICATION mp_p2 FOR TABLE mp_shared", [])
  end

  defp delivered_ids(c) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(c, "SELECT id FROM mp_sink_rows", [])
    MapSet.new(rows, fn [id] -> id end)
  end

  defp delivered_rows(c) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(c, "SELECT id, source FROM mp_sink_rows", [])
    Enum.map(rows, fn [id, source] -> %{id: id, source: source} end)
  end

  defp connection_pid(slot) do
    case Registry.lookup(Replicant.Registry, {slot, :connection}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  defp drop_slot(c, slot) do
    Postgrex.query!(
      c,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  rescue
    _ -> :ok
  end
end

defmodule Replicant.StreamingSpillTest do
  use ExUnit.Case, async: false
  @moduletag :integration
  # Each marquee streams a 20k–40k-row txn through logical decoding + disk spill — genuinely
  # ~15s isolated, and its `Postgrex.transaction(..., timeout: 120_000)` already budgets the DB
  # op at 120s. The ExUnit test timeout was left at the 60s default, so under concurrent machine
  # load a test would hit ExUnit.TimeoutError (killed at 60s) while its own DB op still had budget.
  # Match the ExUnit ceiling to the 120s the transactions already use — wall-clock margin only.
  @moduletag timeout: 120_000

  alias Replicant.Test.PG16

  setup do
    {:ok, ctrl} =
      PG16.named_conn(Replicant.Test.SpCtrlConn, pool_size: 3)

    {:ok, _} =
      PG16.named_conn(Replicant.Test.SpillConn, pool_size: 2)

    slot = "rep_sp_#{System.unique_integer([:positive])}"

    spill_dir =
      Path.join(System.tmp_dir!(), "replicant_spill_it_#{System.unique_integer([:positive])}")

    reset_schema(ctrl)
    drop_slot(ctrl, slot)
    Postgrex.query!(ctrl, "ALTER ROLE postgres SET logical_decoding_work_mem = '64kB'", [])

    on_exit(fn ->
      Replicant.stop(slot)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 400)
      {:ok, c} = Postgrex.start_link(PG16.pg_opts())
      Postgrex.query!(c, "ALTER ROLE postgres RESET logical_decoding_work_mem", [])
      drop_slot(c, slot)
      File.rm_rf(spill_dir)
    end)

    %{ctrl: ctrl, slot: slot, spill_dir: spill_dir}
  end

  test "MARQUEE: a single streamed txn LARGER than max_inflight_lag spills to disk and delivers effect-once (dup=0); no stale spill files",
       %{ctrl: ctrl, slot: slot, spill_dir: spill_dir} do
    if PG16.enabled?() do
      start_pipeline(slot, spill_dir,
        max_inflight_lag: 128 * 1024,
        max_spill_bytes: 64 * 1024 * 1024
      )

      Postgrex.transaction(ctrl, fn c -> bulk_insert(c, 20_000) end, timeout: 120_000)

      PG16.wait_until(fn -> cp_lsn(ctrl) not in [nil, 0] end, 2000)
      PG16.wait_until(fn -> MapSet.subset?(MapSet.new([1, 20_000]), row_ids(ctrl)) end, 2000)
      assert MapSet.subset?(MapSet.new(1..20_000), row_ids(ctrl))
      assert calls_count(ctrl) == 1
      assert spill_files(spill_dir, slot) == []
    end
  end

  test "spill fired (proven): the [:stream, :spilled] probe fires for an oversized streamed txn",
       %{ctrl: ctrl, slot: slot, spill_dir: spill_dir} do
    if PG16.enabled?() do
      ref = make_ref()
      test_pid = self()

      :telemetry.attach(
        {__MODULE__, ref},
        [:replicant, :stream, :spilled],
        fn _e, _m, _meta, _ -> send(test_pid, {:spilled, ref}) end,
        nil
      )

      start_pipeline(slot, spill_dir,
        max_inflight_lag: 128 * 1024,
        max_spill_bytes: 64 * 1024 * 1024
      )

      Postgrex.transaction(ctrl, fn c -> bulk_insert(c, 20_000) end, timeout: 120_000)

      assert_receive {:spilled, ^ref}, 10_000
      :telemetry.detach({__MODULE__, ref})
    end
  end

  test "crash-injection: a delivery fault on an oversized txn halts, leaves NO stale spill files, and re-streams effect-once on restart",
       %{ctrl: ctrl, slot: slot, spill_dir: spill_dir} do
    if PG16.enabled?() do
      start_pipeline(slot, spill_dir,
        max_inflight_lag: 128 * 1024,
        max_spill_bytes: 64 * 1024 * 1024
      )

      Postgrex.query!(
        ctrl,
        "ALTER TABLE sp_sink_cp ADD CONSTRAINT tmp_block CHECK (lsn < 0) NOT VALID",
        []
      )

      Postgrex.transaction(ctrl, fn c -> bulk_insert(c, 20_000) end, timeout: 120_000)

      PG16.wait_until(
        fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end,
        3000
      )

      assert cp_lsn(ctrl) in [nil, 0]
      assert row_ids(ctrl) == MapSet.new()
      assert spill_files(spill_dir, slot) == []
      Postgrex.query!(ctrl, "ALTER TABLE sp_sink_cp DROP CONSTRAINT tmp_block", [])

      start_pipeline(slot, spill_dir,
        max_inflight_lag: 128 * 1024,
        max_spill_bytes: 64 * 1024 * 1024
      )

      PG16.wait_until(fn -> MapSet.subset?(MapSet.new([1, 20_000]), row_ids(ctrl)) end, 3000)
      assert calls_count(ctrl) == 1
      assert spill_files(spill_dir, slot) == []
    end
  end

  test "an oversized never-committing streamed txn halts fail-closed :sink_too_slow (the §4 backstop pre-empts the deferred disk-ceiling halt) and cleans up",
       %{
         ctrl: ctrl,
         slot: slot,
         spill_dir: spill_dir
       } do
    if PG16.enabled?() do
      ref = make_ref()
      test_pid = self()

      :telemetry.attach(
        {__MODULE__, ref, :disc},
        [:replicant, :connection, :disconnected],
        fn _e, _m, meta, _ -> send(test_pid, {:disc, ref, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach({__MODULE__, ref, :disc}) end)

      start_pipeline(slot, spill_dir, max_inflight_lag: 64 * 1024, max_spill_bytes: 128 * 1024)

      Postgrex.transaction(ctrl, fn c -> bulk_insert(c, 40_000) end, timeout: 120_000)

      # A single in-progress txn far larger than max_inflight_lag + max_spill_bytes arrives as a
      # burst: the Connection reads its WAL faster than the assembler spills it, so the §4
      # in-flight-lag backstop (`received − floor − spilled` > RAM + disk) halts fail-closed FIRST.
      # The disk-ceiling `:spill_exhausted` halt is DEFERRED to the next StreamCommit — which never
      # arrives for a never-committing oversized txn — so it cannot fire here; the disk ceiling is
      # covered deterministically at the unit level. Red-capable on the halt REASON (a wrong-reason
      # halt would fail this), unlike the prior registry-only assertion.
      assert_receive {:disc, ^ref, %{reason: :sink_too_slow}}, 20_000

      PG16.wait_until(
        fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end,
        3000
      )

      assert cp_lsn(ctrl) in [nil, 0]
      assert spill_files(spill_dir, slot) == []
    end
  end

  defp start_pipeline(slot, spill_dir, opts) do
    ref = make_ref()
    test_pid = self()

    :telemetry.attach(
      {__MODULE__, ref, :active},
      [:replicant, :connection, :slot_active],
      fn _e, _m, _meta, _ -> send(test_pid, {:slot_active, ref}) end,
      nil
    )

    {:ok, _} =
      Replicant.start_link(
        connection: PG16.pg_opts(),
        slot_name: slot,
        publication: "sp_pub",
        sink: Replicant.Test.SpillLedgerSink,
        go_forward_only: true,
        max_inflight_lag: Keyword.fetch!(opts, :max_inflight_lag),
        streaming: [
          max_concurrent_txns: 64,
          spill: [dir: spill_dir, max_spill_bytes: Keyword.fetch!(opts, :max_spill_bytes)]
        ]
      )

    receive do
      {:slot_active, ^ref} -> :ok
    after
      15_000 -> ExUnit.Assertions.flunk("pipeline never reached slot_active for #{slot}")
    end

    :telemetry.detach({__MODULE__, ref, :active})
    PG16.wait_until(fn -> connection_pid(slot) != nil end, 800)
  end

  defp reset_schema(c) do
    for stmt <- [
          "DROP PUBLICATION IF EXISTS sp_pub",
          "DROP TABLE IF EXISTS sp_orders",
          "CREATE TABLE sp_orders (id int PRIMARY KEY)",
          "DROP TABLE IF EXISTS sp_sink_rows",
          "CREATE TABLE sp_sink_rows (id int PRIMARY KEY)",
          "DROP TABLE IF EXISTS sp_sink_cp",
          "CREATE TABLE sp_sink_cp (id int PRIMARY KEY, lsn bigint)",
          "DROP TABLE IF EXISTS sp_sink_calls",
          "CREATE TABLE sp_sink_calls (lsn bigint, n int)",
          "CREATE PUBLICATION sp_pub FOR TABLE sp_orders"
        ],
        do: Postgrex.query!(c, stmt, [])
  end

  defp bulk_insert(c, row_count) do
    Postgrex.query!(
      c,
      "INSERT INTO sp_orders (id) SELECT id FROM generate_series(1, $1) AS id " <>
        "ON CONFLICT (id) DO NOTHING",
      [row_count]
    )
  end

  defp row_ids(c) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(c, "SELECT id FROM sp_sink_rows", [])
    rows |> Enum.map(fn [id] -> id end) |> MapSet.new()
  end

  defp calls_count(c) do
    %Postgrex.Result{rows: [[n]]} = Postgrex.query!(c, "SELECT count(*) FROM sp_sink_calls", [])
    n
  end

  defp cp_lsn(c) do
    case Postgrex.query(c, "SELECT lsn FROM sp_sink_cp WHERE id = 1", []) do
      {:ok, %Postgrex.Result{rows: [[lsn]]}} -> lsn
      {:ok, %Postgrex.Result{rows: []}} -> nil
      {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} -> nil
    end
  end

  defp connection_pid(slot) do
    case Registry.lookup(Replicant.Registry, {slot, :connection}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  defp spill_files(spill_dir, slot) do
    case File.ls(Path.join(spill_dir, slot)) do
      {:ok, files} -> files
      {:error, _} -> []
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

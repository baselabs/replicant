defmodule Replicant.StreamingTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.Test.PG16

  setup do
    {:ok, ctrl} =
      Postgrex.start_link(PG16.pg_opts() ++ [name: Replicant.Test.StCtrlConn, pool_size: 3])

    {:ok, _} =
      Postgrex.start_link(PG16.pg_opts() ++ [name: Replicant.Test.StreamingConn, pool_size: 2])

    slot = "rep_st_#{System.unique_integer([:positive])}"
    reset_schema(ctrl)
    drop_slot(ctrl, slot)
    Postgrex.query!(ctrl, "ALTER ROLE postgres SET logical_decoding_work_mem = '64kB'", [])

    on_exit(fn ->
      Replicant.stop(slot)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 400)
      {:ok, c} = Postgrex.start_link(PG16.pg_opts())
      Postgrex.query!(c, "ALTER ROLE postgres RESET logical_decoding_work_mem", [])
      drop_slot(c, slot)
    end)

    %{ctrl: ctrl, slot: slot}
  end

  test "MARQUEE: a large streamed txn with a nested-savepoint rollback delivers effect-once (dup=0); sub-aborted rows ABSENT",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      start_pipeline(slot)
      # Anti-vacuous guard: prove the txn actually STREAMED (reassembled via StreamCommit, not
      # the v1 Commit path). If this never fires for a >64kB txn, the marquee is not exercising
      # streaming at all — the assertion below MUST go red.
      stream = attach_stream_probe()

      Postgrex.transaction(
        ctrl,
        fn c ->
          Enum.each(1..1000, &insert(c, &1))
          Postgrex.query!(c, "SAVEPOINT sp1", [])
          Postgrex.query!(c, "SAVEPOINT sp2", [])
          Enum.each(1001..1500, &insert(c, &1))
          Postgrex.query!(c, "ROLLBACK TO SAVEPOINT sp1", [])
          Enum.each(1501..2500, &insert(c, &1))
        end,
        timeout: 60_000
      )

      PG16.wait_until(fn -> cp_lsn(ctrl) not in [nil, 0] end, 800)
      PG16.wait_until(fn -> MapSet.subset?(MapSet.new([1, 2500]), row_ids(ctrl)) end, 800)

      # Streaming actually happened — the large txn was reassembled from StreamStart/StreamCommit.
      assert stream_committed_count(stream) >= 1
      detach_stream_probe(stream)

      committed = MapSet.new(Enum.to_list(1..1000) ++ Enum.to_list(1501..2500))
      assert MapSet.subset?(committed, row_ids(ctrl))

      # Nested-savepoint sub-abort: 1001..1500 (opened under sp1/sp2, rolled back to sp1) are ABSENT.
      assert Enum.all?(1001..1500, fn id -> not MapSet.member?(row_ids(ctrl), id) end)

      # dup=0, load-bearing: EXACTLY ONE transaction is delivered to the sink and it carries the DATA
      # (2000 committed changes = 1..1000 + 1501..2500, sub-aborted 1001..1500 excluded). st_sink_calls
      # is append-only (no PK).
      #   - calls_count == 1 proves NO empty/extra delivery: the sink's ~2000-row writeback to the
      #     UNPUBLISHED tables (st_sink_rows/cp/calls — created by reset_schema but NOT in st_pub, which
      #     publishes only st_orders) spills past the 64kB work_mem and pgoutput v2 streams it as an
      #     empty published-change txn; the assembler.ex StreamCommit guard SUPPRESSES it so a streamed
      #     txn is indistinguishable from a non-streamed one at the sink (spec §7). This is a LIVE
      #     integration red-gate: reverting that guard makes THIS assertion go red here (got 2), so it
      #     complements the synthetic assembler unit test. A re-delivery of the data txn also pushes
      #     this to 2.
      #   - data_calls == [2000] proves the exact filtered change set (a sub-abort leak would push n to
      #     2500) AND dup=0 (a re-delivery adds a second n>0 row). Having BOTH is strongest.
      assert calls_count(ctrl) == 1
      assert data_calls(ctrl) == [2000]
    end
  end

  test "crash-injection: a delivery fault on a large streamed txn halts; restart re-streams effect-once (dup=0)",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      start_pipeline(slot)

      Postgrex.query!(
        ctrl,
        "ALTER TABLE st_sink_cp ADD CONSTRAINT tmp_block CHECK (lsn < 0) NOT VALID",
        []
      )

      Postgrex.transaction(ctrl, fn c -> Enum.each(1..2000, &insert(c, &1)) end, timeout: 60_000)

      PG16.wait_until(
        fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end,
        1500
      )

      assert cp_lsn(ctrl) in [nil, 0]
      assert row_ids(ctrl) == MapSet.new()

      Postgrex.query!(ctrl, "ALTER TABLE st_sink_cp DROP CONSTRAINT tmp_block", [])
      # Attach the stream probe BEFORE the restart: on resume the un-acked txn re-streams from WAL
      # immediately after slot_active, so a probe attached post-start could race past StreamCommit.
      stream = attach_stream_probe()
      start_pipeline(slot)
      PG16.wait_until(fn -> MapSet.subset?(MapSet.new([1, 2000]), row_ids(ctrl)) end, 1500)

      assert stream_committed_count(stream) >= 1
      detach_stream_probe(stream)

      assert MapSet.subset?(MapSet.new(1..2000), row_ids(ctrl))

      # dup=0: EXACTLY ONE transaction is delivered on resume — the 2000-change data txn re-streamed
      # once (the pre-fault attempt's ledger row rolled back atomically with the CHECK-violating
      # checkpoint write). calls_count == 1 proves no empty/extra delivery (empty streamed txns are
      # suppressed, spec §7); data_calls == [2000] proves the exact change set + no duplicate.
      assert calls_count(ctrl) == 1
      assert data_calls(ctrl) == [2000]
      assert Enum.all?(1..2000, fn id -> row_count(ctrl, id) == 1 end)
    end
  end

  test "a small transaction (below the streaming threshold) still delivers via the non-streamed path",
       %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      start_pipeline(slot)
      stream = attach_stream_probe()
      insert(ctrl, 1)
      PG16.wait_until(fn -> MapSet.member?(row_ids(ctrl), 1) end, 800)
      # dup=0: EXACTLY ONE transaction is delivered and it carries the single-row data txn (n=1).
      # calls_count == 1 proves no empty/extra delivery; data_calls == [1] proves the change set.
      assert calls_count(ctrl) == 1
      assert data_calls(ctrl) == [1]
      assert Registry.lookup(Replicant.Registry, {slot, :pipeline}) != []
      # A single-row txn is well below 64kB → it goes via the v1 Commit path, NOT StreamCommit.
      assert stream_committed_count(stream) == 0
      detach_stream_probe(stream)
    end
  end

  defp attach_stream_probe do
    ref = make_ref()
    test_pid = self()

    :telemetry.attach(
      {__MODULE__, :stream, ref},
      [:replicant, :stream, :committed],
      fn _e, _m, _meta, _ -> send(test_pid, {:stream_committed, ref}) end,
      nil
    )

    ref
  end

  defp stream_committed_count(ref), do: drain_stream(ref, 0)

  defp drain_stream(ref, acc) do
    receive do
      {:stream_committed, ^ref} -> drain_stream(ref, acc + 1)
    after
      0 -> acc
    end
  end

  defp detach_stream_probe(ref), do: :telemetry.detach({__MODULE__, :stream, ref})

  defp start_pipeline(slot) do
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
        connection: PG16.pg_opts(),
        slot_name: slot,
        publication: "st_pub",
        sink: Replicant.Test.StreamingLedgerSink,
        go_forward_only: true,
        streaming: [max_concurrent_txns: 64]
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
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS st_pub", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS st_orders", [])
    Postgrex.query!(c, "CREATE TABLE st_orders (id int PRIMARY KEY)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS st_sink_rows", [])
    Postgrex.query!(c, "CREATE TABLE st_sink_rows (id int PRIMARY KEY)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS st_sink_cp", [])
    Postgrex.query!(c, "CREATE TABLE st_sink_cp (id int PRIMARY KEY, lsn bigint)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS st_sink_calls", [])
    Postgrex.query!(c, "CREATE TABLE st_sink_calls (lsn bigint, n int)", [])
    Postgrex.query!(c, "CREATE PUBLICATION st_pub FOR TABLE st_orders", [])
  end

  defp insert(c, id),
    do:
      Postgrex.query!(c, "INSERT INTO st_orders (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", [
        id
      ])

  defp row_ids(c) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(c, "SELECT id FROM st_sink_rows", [])
    rows |> Enum.map(fn [id] -> id end) |> MapSet.new()
  end

  defp row_count(c, id) do
    %Postgrex.Result{rows: [[n]]} =
      Postgrex.query!(c, "SELECT count(*) FROM st_sink_rows WHERE id = $1", [id])

    n
  end

  # The `n` (change count) of every DATA-carrying delivered transaction (n > 0), ascending. One data
  # txn in a test → a single-element list; a duplicate re-delivery → two elements (append-only ledger,
  # no PK, so it cannot be masked). Empty streamed transactions are now SUPPRESSED — the StreamCommit
  # guard returns {:skipped} and never delivers them (spec §7 v1-indistinguishability), so no n=0 row
  # is expected from a streamed empty txn. The `WHERE n > 0` filter is therefore defense-in-depth: it
  # documents the data-vs-empty distinction and isolates dup detection to the data txn, rather than
  # working around delivered empties (there are none). See calls_count/1 for the total-delivery gate.
  defp data_calls(c) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(c, "SELECT n FROM st_sink_calls WHERE n > 0 ORDER BY lsn", [])

    Enum.map(rows, fn [n] -> n end)
  end

  # The TOTAL number of transactions delivered to the sink — one st_sink_calls row per
  # handle_transaction call (append-only ledger, includes n=0). With empty streamed txns suppressed
  # (assembler.ex StreamCommit guard, spec §7), a scenario delivering one data txn must count EXACTLY
  # 1: no empty streamed txn is delivered, and a re-delivery of the data txn would push this to 2.
  defp calls_count(c) do
    %Postgrex.Result{rows: [[n]]} = Postgrex.query!(c, "SELECT count(*) FROM st_sink_calls", [])
    n
  end

  defp cp_lsn(c) do
    case Postgrex.query(c, "SELECT lsn FROM st_sink_cp WHERE id = 1", []) do
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

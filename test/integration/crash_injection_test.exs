defmodule Replicant.CrashInjectionTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.Test.{LedgerSink, PG16}

  # The §4 spike tests drive an EXPLICIT small in-flight ceiling (64 KiB) via the
  # `:max_inflight_lag` override, sized against the live 25ms/txn slow sink so a
  # normal 200-txn burst (~37 KB peak lag) drains under it while a pathological
  # 600-txn burst / a stuck sink trip the fail-closed halt. The PRODUCTION default is
  # the far-larger backlog ceiling (`Replicant.Connection.default_max_inflight_lag/0`,
  # 64 MiB) — this small override only makes the mechanism observable at test scale;
  # it is NOT the shipped default.
  @spike_bound 65_536

  setup do
    unless PG16.enabled?() do
      :ok
    end

    # `LedgerConn` is shared by the sink (one in-flight `Postgrex.transaction` per
    # applied txn, held for the SlowLedgerSink's 25ms) AND the test's own polling
    # queries. `pool_size: 5` (vs the default 1) keeps the slow-sink burst from
    # starving the test's `count`/audit polls of a connection (spike test).
    {:ok, ctrl} =
      Postgrex.start_link(PG16.pg_opts() ++ [name: Replicant.Test.LedgerConn, pool_size: 5])

    slot = "rep_ci_#{System.unique_integer([:positive])}"

    reset_schema(ctrl)
    drop_slot(ctrl, slot)

    on_exit(fn ->
      Replicant.stop(slot)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 200)
      {:ok, c} = Postgrex.start_link(PG16.pg_opts())
      drop_slot(c, slot)
    end)

    %{ctrl: ctrl, slot: slot}
  end

  test "baseline: every committed transaction lands exactly once, checkpoint = last LSN", %{
    ctrl: ctrl,
    slot: slot
  } do
    start_pipeline(slot)
    insert(ctrl, 1, "a")
    insert(ctrl, 2, "b")
    insert(ctrl, 3, "c")

    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 3 end)

    assert rows(ctrl, "SELECT id, note FROM sink_orders ORDER BY id") == [
             [1, "a"],
             [2, "b"],
             [3, "c"]
           ]

    assert applied_counts(ctrl) |> Map.values() |> Enum.all?(&(&1 == 1))
  end

  test "crash-and-resume: killing the Connection mid-stream loses nothing", %{
    ctrl: ctrl,
    slot: slot
  } do
    start_pipeline(slot)
    insert(ctrl, 1, "a")
    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 1 end)

    conn = connection_pid(slot)
    ref = Process.monitor(conn)
    Process.exit(conn, :kill)
    assert_receive {:DOWN, ^ref, :process, ^conn, _}, 5000

    insert(ctrl, 2, "b")
    insert(ctrl, 3, "c")
    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 3 end)

    assert rows(ctrl, "SELECT id, note FROM sink_orders ORDER BY id") == [
             [1, "a"],
             [2, "b"],
             [3, "c"]
           ]
  end

  # RISK #1 (finalized against the live stream): the plan's premise — "a crash after
  # sink-commit before ack re-delivers" — does NOT reach the SINK for a plain
  # `LedgerSink`. Two mechanisms suppress it, and neither is the client `start_lsn`:
  # (a) whether PG re-streams a transaction is governed by the slot's server-side
  # `confirmed_flush_lsn` (the client `START_REPLICATION ... <start_lsn>` value is a
  # clamped hint, never a lower bound PG honors literally); and (b) even when PG does
  # re-deliver, the Assembler's Commit-path pre-skip (`commit_lsn <= sink.checkpoint()`
  # → `{:skipped}`) drops it BEFORE the sink. So the sink's own idempotency dedup
  # (spec §6) is only reachable via the spec §14.15 checkpoint-read-fail-open path:
  # `FailOpenLedgerSink.checkpoint/0` reports `nil`, so after the kill-before-ack the
  # restart resumes from `0/0` (PG re-delivers txn 1) AND the Assembler pre-skip is
  # disabled (the re-delivery reaches the sink), whose DURABLE `_replicant_checkpoint`
  # watermark records it `skipped` and applies it zero more times.
  # The kill hook is `[:replicant, :sink, :committed]` — the real post-durable-commit,
  # pre-ack event: it fires from WITHIN the AssemblerServer AFTER the sink durably
  # commits and BEFORE that process sends `{:sink_committed, lsn}` to the Connection, so
  # the `Process.exit(conn, :kill)` (same sender) is enqueued ahead of the ack and wins.
  test "re-delivery dedup: a crash after sink-commit before ack re-delivers → skipped, applied once",
       %{ctrl: ctrl, slot: slot} do
    :telemetry.attach(
      {__MODULE__, :kill_before_ack},
      [:replicant, :sink, :committed],
      fn _e, _m, _meta, target_slot ->
        :telemetry.detach({__MODULE__, :kill_before_ack})

        case Registry.lookup(Replicant.Registry, {target_slot, :connection}) do
          [{conn, _}] -> Process.exit(conn, :kill)
          [] -> :ok
        end
      end,
      slot
    )

    start_pipeline(slot, sink: Replicant.Test.FailOpenLedgerSink)
    insert(ctrl, 1, "a")

    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 1 end)
    PG16.wait_until(fn -> skipped_count(ctrl) >= 1 end, 400)

    assert rows(ctrl, "SELECT id, note FROM sink_orders ORDER BY id") == [[1, "a"]]
    assert applied_counts(ctrl) |> Map.values() |> Enum.all?(&(&1 == 1))
  end

  # RISK #2 (spec §4 bounded in-flight window, proven against the live stream). The
  # prior `max_len < 800` spike was VACUOUS: with no in-flight bound the mailbox
  # scaled linearly with burst (measured RED: 200-txn → ~537 msgs, 600-txn → ~1629
  # msgs), so a threshold chosen for the 200-burst is meaningless at 600. These three
  # tests encode the REAL §4 property: the in-flight WAL lag ceiling
  # (`received_lsn - max(checkpoint_lsn, stream_floor_lsn)`) holds INDEPENDENT of
  # burst — a normal burst drains under it, a pathological one (or a stuck sink) halts
  # fail-closed before the mailbox grows unbounded. No silent OOM/livelock either way.
  #
  # They drive an EXPLICIT `max_inflight_lag: @spike_bound` (64 KiB) so the mechanism
  # is observable at test scale — NOT the production default (64 MiB backlog ceiling).
  # Bound sizing (live PG16, 25ms/txn slow sink, @spike_bound = 65_536 B):
  #   * 200-txn burst → peak in-flight lag ~37 KB   → UNDER 64 KiB → drains, all 200 apply.
  #   * 600-txn burst → peak in-flight lag ~110+ KB → OVER 64 KiB → fail-closed halt.
  # (measured over 3 live runs: the 200-peak stayed ~37 KB while the natural 600-peak
  # was ~110–118 KB — the halt fires the instant lag crosses 65_536, so the observed
  # halt lag is ~65.6 KB, NOT the unbounded 110 KB the RED reached.)
  @tag :spike
  @tag timeout: 120_000
  test "bounded in-flight window: the lag ceiling holds independent of burst size", %{
    ctrl: ctrl,
    slot: slot
  } do
    # (A) A NORMAL 200-txn burst drains UNDER the configured ceiling (no halt). The
    # peak in-flight lag occurs right after the burst lands (frames arrive at wire
    # speed, the 25ms/txn sink has barely drained), so a dense ~5s sample catches it.
    start_pipeline(slot, sink: Replicant.Test.SlowLedgerSink, max_inflight_lag: @spike_bound)
    conn = connection_pid(slot)
    for i <- 1..200, do: insert(ctrl, i, "n#{i}")

    max_lag_200 =
      sample(125, 0, fn acc ->
        max(acc, inflight_lag(conn))
      end)

    IO.puts(
      "\n[spike A] 200-txn burst peak in-flight lag = #{max_lag_200} B (ceiling #{@spike_bound} B)"
    )

    assert max_lag_200 <= @spike_bound,
           "a normal 200-txn burst peaked at #{max_lag_200} B in-flight lag, over the " <>
             "#{@spike_bound} B ceiling — it should DRAIN under the bound, not halt"

    # And it drains to completion (never halted): all 200 apply exactly once.
    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 200 end, 400)
    assert applied_counts(ctrl) |> Map.values() |> Enum.all?(&(&1 == 1))
  end

  @tag :spike
  @tag timeout: 120_000
  test "bounded in-flight window: a 600-txn burst trips the fail-closed :sink_too_slow halt", %{
    ctrl: ctrl,
    slot: slot
  } do
    # (B) A PATHOLOGICAL 600-txn burst at the same slow sink exceeds the ceiling and
    # halts fail-closed BEFORE the mailbox grows unbounded (the RED was ~1629 msgs).
    attach_sink_too_slow(slot)
    start_pipeline(slot, sink: Replicant.Test.SlowLedgerSink, max_inflight_lag: @spike_bound)
    for i <- 1..600, do: insert(ctrl, i, "n#{i}")

    assert_receive {:sink_too_slow, %{lag: lag}}, 15_000
    IO.puts("\n[spike B] 600-txn burst tripped :sink_too_slow at in-flight lag = #{lag} B")
    assert lag > @spike_bound

    # The pipeline is torn down permanently (fail-closed) — not livelocking/OOMing.
    PG16.wait_until(
      fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end,
      400
    )

    :telemetry.detach({__MODULE__, {:too_slow, slot}})
  end

  @tag :spike
  @tag timeout: 120_000
  test "fail-closed halt: a genuinely-stuck sink halts (no silent OOM/livelock)", %{
    ctrl: ctrl,
    slot: slot
  } do
    # The sink blocks forever on its first txn → the checkpoint never advances → the
    # in-flight lag grows monotonically past the ceiling and MUST fail-closed halt.
    attach_sink_too_slow(slot)
    start_pipeline(slot, sink: Replicant.Test.StuckLedgerSink, max_inflight_lag: @spike_bound)
    for i <- 1..600, do: insert(ctrl, i, "n#{i}")

    assert_receive {:sink_too_slow, %{lag: lag}}, 15_000
    IO.puts("\n[spike C] stuck sink tripped :sink_too_slow at in-flight lag = #{lag} B")
    assert lag > @spike_bound

    PG16.wait_until(
      fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end,
      400
    )

    # Nothing was durably applied (the stuck sink never committed) — no partial state.
    assert count(ctrl, "sink_orders") == 0
    :telemetry.detach({__MODULE__, {:too_slow, slot}})
  end

  # Poll `reader.(acc)` `n` times at 40ms, folding into `acc`.
  defp sample(0, acc, _reader), do: acc

  defp sample(n, acc, reader) do
    acc = reader.(acc)
    Process.sleep(40)
    sample(n - 1, acc, reader)
  end

  # The live in-flight WAL lag from the running Connection's state (0 if unreadable,
  # e.g. mid-teardown after a halt).
  defp inflight_lag(conn) do
    case safe_conn_state(conn) do
      %Replicant.Connection{received_lsn: r, checkpoint_lsn: c} -> r - c
      _ -> 0
    end
  end

  defp safe_conn_state(conn) do
    case :sys.get_state(conn, 100) do
      %{state: {Replicant.Connection, %Replicant.Connection{} = st}} -> st
      {_, %{state: {Replicant.Connection, %Replicant.Connection{} = st}}} -> st
      _ -> nil
    end
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  defp attach_sink_too_slow(slot) do
    :telemetry.attach(
      {__MODULE__, {:too_slow, slot}},
      [:replicant, :connection, :disconnected],
      fn _e, meas, meta, pid ->
        if meta[:reason] == :sink_too_slow, do: send(pid, {:sink_too_slow, meas})
      end,
      self()
    )
  end

  defp start_pipeline(slot, opts \\ []) do
    sink = Keyword.get(opts, :sink, LedgerSink)

    :telemetry.attach(
      {__MODULE__, {:active, slot}},
      [:replicant, :connection, :slot_active],
      fn _e, _m, _meta, pid -> send(pid, {:slot_active, slot}) end,
      self()
    )

    base = [
      connection: PG16.pg_opts(),
      slot_name: slot,
      publication: "orders_pub",
      sink: sink,
      go_forward_only: true
    ]

    # Thread an explicit in-flight ceiling through only when a test sets it (the §4
    # spike tests do; the correctness tests omit it and take the production default).
    extra = Keyword.take(opts, [:max_inflight_lag])
    {:ok, _pid} = Replicant.start_link(base ++ extra)

    assert_receive {:slot_active, ^slot}, 15_000
    :telemetry.detach({__MODULE__, {:active, slot}})
  end

  defp reset_schema(c) do
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS orders_pub", [])

    Postgrex.query!(
      c,
      "DROP TABLE IF EXISTS orders, sink_orders, _replicant_checkpoint, _replicant_calls",
      []
    )

    Postgrex.query!(c, "CREATE TABLE orders (id int PRIMARY KEY, note text)", [])
    Postgrex.query!(c, "CREATE TABLE sink_orders (id int PRIMARY KEY, note text)", [])
    Postgrex.query!(c, "CREATE TABLE _replicant_checkpoint (id int PRIMARY KEY, lsn bigint)", [])

    Postgrex.query!(
      c,
      "CREATE TABLE _replicant_calls (seq bigserial PRIMARY KEY, lsn bigint, outcome text)",
      []
    )

    Postgrex.query!(c, "CREATE PUBLICATION orders_pub FOR TABLE orders", [])
  end

  defp drop_slot(c, slot) do
    Postgrex.query!(
      c,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  end

  defp insert(c, id, note),
    do: Postgrex.query!(c, "INSERT INTO orders (id, note) VALUES ($1, $2)", [id, note])

  defp count(c, table), do: rows(c, "SELECT count(*) FROM #{table}") |> hd() |> hd()
  defp rows(c, sql), do: Postgrex.query!(c, sql, []).rows

  defp applied_counts(c) do
    rows(c, "SELECT lsn, count(*) FROM _replicant_calls WHERE outcome = 'applied' GROUP BY lsn")
    |> Map.new(fn [lsn, n] -> {lsn, n} end)
  end

  defp skipped_count(c),
    do: rows(c, "SELECT count(*) FROM _replicant_calls WHERE outcome = 'skipped'") |> hd() |> hd()

  defp connection_pid(slot), do: lookup_pid({slot, :connection})

  defp lookup_pid(key) do
    PG16.wait_until(fn -> match?([{_, _}], Registry.lookup(Replicant.Registry, key)) end, 200)
    [{pid, _}] = Registry.lookup(Replicant.Registry, key)
    pid
  end
end

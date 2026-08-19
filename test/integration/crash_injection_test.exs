defmodule Replicant.CrashInjectionTest do
  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag timeout: 120_000

  alias Replicant.Test.{LedgerSink, PauseGate, PausingLedgerSink, PG16}

  # The §4 spike tests drive an EXPLICIT small in-flight ceiling (128 KiB) via the
  # `:max_inflight_lag` override, sized against the live 25ms/txn slow sink so a
  # normal 200-txn burst (~40-75 KB peak lag incl. same-server pollution, see below)
  # drains under it while a pathological 900-txn burst / a stuck sink trip the
  # fail-closed halt. The PRODUCTION default is
  # the far-larger backlog ceiling (`Replicant.Connection.default_max_inflight_lag/0`,
  # 64 MiB) — this small override only makes the mechanism observable at test scale;
  # it is NOT the shipped default.
  @spike_bound 131_072

  setup do
    unless PG16.enabled?() do
      :ok
    end

    # `LedgerConn` is shared by the sink (one in-flight `Postgrex.transaction` per
    # applied txn, held for the SlowLedgerSink's 25ms) AND the test's own polling
    # queries. `pool_size: 5` (vs the default 1) keeps the slow-sink burst from
    # starving the test's `count`/audit polls of a connection (spike test).
    {:ok, ctrl} =
      PG16.named_conn(Replicant.Test.LedgerConn, pool_size: 5)

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

  # ADVERSARIAL CRASH-INJECTION (spec §12.2 line 144: "kill the Connection ... MID-
  # TRANSACTION"). In pgoutput proto v1 PG streams a committed txn's frames
  # (Begin → row changes → Commit) as a burst; the AssemblerServer assembles them into
  # its open `txn` buffer and applies the sink SYNCHRONOUSLY at Commit — the transaction
  # is only DURABLY committed (checkpoint advanced) once `handle_transaction/1` returns.
  # A mid-transaction kill is a crash while a committed-in-PG transaction is IN-FLIGHT in
  # the consumer — assembled/being-applied but NOT yet durably committed. `:one_for_all`
  # discards that in-memory in-flight transaction, and on resume PG re-streams the WHOLE
  # transaction from the durable checkpoint — which sits BEFORE it (the prior control
  # txn's LSN) — so it is assembled and applied exactly once (loss=0, effect-dup=0).
  #
  # WHY A SANCTIONED TEST-ONLY HOOK, NOT `:sys.get_state`-POLLING: the task's primary
  # approach (poll the assembler's transient in-buffer `changes` and kill at
  # `0 < changes < N`) proved UNRELIABLE against the live stream — measured over 8 live
  # trials at N=2000 it caught the mid-buffer state only 2/8 times (6/8 MISS): the
  # AssemblerServer churns a large frame burst faster than a `:sys.get_state` poll can
  # observe it, and while it is applying a prior txn `:sys.get_state` times out entirely.
  # A racy catch is not a valid gate. So this uses the task's explicitly-sanctioned
  # fallback: a DETERMINISTIC test-only instrumentation hook (`PausingLedgerSink` +
  # `PauseGate`, test-support only, NO `lib/` change / telemetry) that blocks the
  # AssemblerServer at the sink boundary with the transaction fully ASSEMBLED but NOT
  # durably committed — the exact in-flight precondition a mid-transaction crash must
  # survive. The kill lands in that deterministic window; the injection-is-real evidence
  # is proven three ways at kill time: the sink signalled `{:sink_paused, lsn, N}` (the
  # whole txn assembled, N changes, about to apply), the durable checkpoint is still at
  # the CONTROL txn's LSN (the big txn NOT committed), and ZERO of the big txn's rows are
  # in `sink_orders`.
  @midtxn_rows 5_000
  test "mid-transaction kill: in-flight txn discarded, whole txn re-streamed exactly once", %{
    ctrl: ctrl,
    slot: slot
  } do
    # Arm the pause gate to block the first transaction of >= @midtxn_rows changes,
    # notifying this test process. Torn down on exit.
    {:ok, gate} = PauseGate.start_link(self(), @midtxn_rows)
    on_exit(fn -> if Process.alive?(gate), do: Agent.stop(gate) end)

    start_pipeline(slot, sink: PausingLedgerSink)

    # A control txn FIRST so the durable checkpoint advances to a point strictly BEFORE
    # the large txn — proving resume re-streams the whole large txn, not from 0/0.
    insert(ctrl, 1, "ctrl")
    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 1 end)
    control_cp = checkpoint_lsn(ctrl)
    assert is_integer(control_cp) and control_cp > 0

    # Fire the large single-commit txn from its OWN connection (one Begin→N rows→Commit).
    big_task =
      Task.async(fn ->
        {:ok, c} = Postgrex.start_link(PG16.pg_opts())

        Postgrex.query!(
          c,
          "INSERT INTO orders (id, note) SELECT g, 'big' FROM generate_series(2, #{@midtxn_rows + 1}) g",
          [],
          timeout: 30_000
        )

        GenServer.stop(c)
      end)

    # DETERMINISTIC catch: the sink blocks with the whole txn assembled, signalling us.
    assert_receive {:sink_paused, big_lsn, assembled_changes}, 20_000

    # Injection-is-real evidence #1: the WHOLE txn is assembled (all N changes) and about
    # to be applied — this is a mid-transaction (in-flight, uncommitted) kill.
    assert assembled_changes == @midtxn_rows
    assert big_lsn > control_cp

    # Injection-is-real evidence #2/#3: the big txn is NOT durably committed at kill time —
    # the checkpoint is still the control txn's LSN and ZERO of its rows are applied.
    assert checkpoint_lsn(ctrl) == control_cp,
           "the big txn must NOT be durably committed at kill time (checkpoint still control's)"

    assert count(ctrl, "sink_orders") == 1,
           "the big txn's rows must NOT be applied at kill time (only the control row present)"

    IO.puts(
      "\n[mid-txn] killed Connection with the whole txn IN-FLIGHT: #{assembled_changes} changes " <>
        "assembled, sink entered, checkpoint still at control LSN #{control_cp} " <>
        "(big txn LSN #{big_lsn} NOT committed, 0 of #{@midtxn_rows} rows applied)"
    )

    # Disarm the gate so the re-streamed txn applies (does not pause) after the restart.
    PauseGate.disarm()

    # Kill the Connection mid-transaction: :one_for_all also terminates the blocked
    # AssemblerServer, discarding the in-flight transaction.
    conn = connection_pid(slot)
    ref = Process.monitor(conn)
    Process.exit(conn, :kill)
    assert_receive {:DOWN, ^ref, :process, ^conn, _}, 5000

    Task.await(big_task, 40_000)

    total = @midtxn_rows + 1

    # loss=0: every row of the control txn AND the whole re-streamed large txn is present.
    PG16.wait_until(fn -> count(ctrl, "sink_orders") == total end, 2400)

    assert rows(ctrl, "SELECT id FROM sink_orders ORDER BY id") ==
             Enum.map(1..total, &[&1])

    # effect-dup=0: the ledger (not the PK-upsert, which would MASK a dup) proves every
    # committed LSN applied exactly once — the discarded in-flight txn produced no
    # duplicate on the re-stream.
    assert applied_counts(ctrl) |> Map.values() |> Enum.all?(&(&1 == 1))
  end

  # ADVERSARIAL CRASH-INJECTION (spec §12.2 line 144: "kill the Connection ... DURING A
  # KEEPALIVE"). When the stream is idle (all committed WAL drained, no txn buffering)
  # the only Connection↔PG traffic is the primary-keepalive/standby-status exchange. Per
  # spec A1, the idle-window keepalive now ADVANCES the slot to `wal_end` (not just the
  # durable checkpoint) since a quiet-but-filtered publication carries no unpersisted
  # data past that point. This test kills the Connection in that idle/keepalive window
  # and proves the kill on the ack/keepalive path still loses nothing and duplicates
  # nothing: the assertions hold because a resume clamps over the filtered WAL in the
  # gap (spec §3.4 — no publication data was skipped, so re-running is still a no-op).
  #
  # DETERMINISTIC idle window (not a bare sleep-and-hope): after the pre-kill txns land,
  # `wait_until_idle/2` confirms the pipeline reached the idle state by THREE signals — the
  # durable `checkpoint_lsn` advanced past 0 (a commit landed), the `received_lsn` frontier
  # is STABLE across a settle interval (no new WAL arriving → the stream drained → the only
  # traffic is the keepalive/standby-status exchange), AND the AssemblerServer's open buffer
  # is nil (no txn mid-assembly). Only then is the Connection killed. Post-kill txns are
  # streamed and everything (pre- and post-kill) is asserted present exactly once.
  test "during-keepalive kill: an idle-window crash loses nothing, duplicates nothing", %{
    ctrl: ctrl,
    slot: slot
  } do
    start_pipeline(slot)

    for id <- 1..3, do: insert(ctrl, id, "pre#{id}")
    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 3 end)

    conn = connection_pid(slot)
    asm = assembler_pid(slot)

    # Drive the pipeline to a verified idle/keepalive state before the kill: durable
    # checkpoint advanced, received frontier stable (drained), assembler buffer nil.
    caught_up_lsn = wait_until_idle(conn, asm)

    IO.puts(
      "\n[keepalive] Connection idle at durable checkpoint_lsn #{caught_up_lsn}, received " <>
        "frontier stable (WAL drained), assembler buffer nil — killing on the keepalive/ack path"
    )

    ref = Process.monitor(conn)
    Process.exit(conn, :kill)
    assert_receive {:DOWN, ^ref, :process, ^conn, _}, 5000

    # More txns after the idle-window kill; the pipeline restarts (:one_for_all) and
    # resumes from the durable checkpoint.
    for id <- 4..6, do: insert(ctrl, id, "post#{id}")
    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 6 end, 400)

    # loss=0: the full pre- AND post-kill row set is present after resume.
    assert rows(ctrl, "SELECT id, note FROM sink_orders ORDER BY id") == [
             [1, "pre1"],
             [2, "pre2"],
             [3, "pre3"],
             [4, "post4"],
             [5, "post5"],
             [6, "post6"]
           ]

    # effect-dup=0: the ledger proves every committed LSN applied exactly once — the kill
    # on the keepalive/ack path duplicated nothing.
    assert applied_counts(ctrl) |> Map.values() |> Enum.all?(&(&1 == 1))
  end

  # RISK #2 (spec §4 bounded in-flight window, proven against the live stream). The
  # prior `max_len < 800` spike was VACUOUS: with no in-flight bound the mailbox scaled
  # linearly with burst, so a threshold chosen for one burst is meaningless at another.
  # These three tests encode the REAL §4 property: the in-flight WAL lag ceiling
  # (`received_lsn - max(checkpoint_lsn, stream_floor_lsn)`) holds INDEPENDENT of burst —
  # a small burst drains under it, a large one (or a stuck sink) halts fail-closed before
  # the mailbox grows unbounded. No silent OOM/livelock either way.
  #
  # They drive an EXPLICIT `max_inflight_lag: @spike_bound` (128 KiB) so the mechanism is
  # observable at test scale — NOT the production default (64 MiB backlog ceiling).
  #
  # WHY 128 KiB, not the original 64 KiB (de-flake, 2026-07-05, root-caused): `received_lsn`
  # is pgoutput's `wal_end` = the SERVER's TOTAL WAL position, so it also counts WAL the
  # TEST sink writes to the SAME PG16 (sink_orders/_replicant_checkpoint/_replicant_calls)
  # plus post-churn autovacuum — "same-server pollution" of ~30-40 KB on top of the ~35 KB
  # of real `orders` WAL for a 200-txn burst. At a 64 KiB ceiling the 200-burst peak (~40 KB
  # baseline) sat only ~24 KB under the bound, and that pollution variance occasionally
  # crossed it, HALTING the burst this test asserts must DRAIN (reproduced 1/15 under CPU
  # load; test A alone never halted — only after the 900-txn B/C tests churned the DB). A
  # real sink writes to a DIFFERENT mirror DB, so this is a test artifact. 128 KiB keeps the
  # ~65-75 KB polluted 200-burst peak a safe ~55+ KB under the ceiling while the widened
  # 900-txn bursts (~159 KB orders + pollution) still clear it decisively.
  # Sizing (live PG16, 25ms/txn slow sink, @spike_bound = 131_072 B):
  #   * 200-txn burst → peak lag ~40-75 KB (orders ~35 KB + pollution) → UNDER 128 KiB → drains.
  #   * 900-txn burst → lag ~159 KB+ → OVER 128 KiB → fail-closed halt (fires ~740 txns in).
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

    # And it drains to completion (never halted): all 200 apply exactly once. The
    # SlowLedgerSink applies serially at ~25ms/txn, so draining 200 txns is INHERENTLY
    # ~5-6s and balloons under machine load (sink applies + the test's count polls share
    # the LedgerConn pool). Size this wait generously to that legitimately-slow drain —
    # 800 polls * 25ms = 20s, well under the test's 120s timeout. A genuinely-halted
    # pipeline (a real §4 regression) still never reaches 200 and fails here.
    PG16.wait_until(fn -> count(ctrl, "sink_orders") == 200 end, 800)
    assert applied_counts(ctrl) |> Map.values() |> Enum.all?(&(&1 == 1))
  end

  @tag :spike
  @tag timeout: 120_000
  test "bounded in-flight window: a 900-txn burst trips the fail-closed :sink_too_slow halt", %{
    ctrl: ctrl,
    slot: slot
  } do
    # (B) A PATHOLOGICAL 900-txn burst at the same slow sink exceeds the ceiling and
    # halts fail-closed BEFORE the mailbox grows unbounded (the vacuous RED grew it).
    attach_sink_too_slow(slot)
    start_pipeline(slot, sink: Replicant.Test.SlowLedgerSink, max_inflight_lag: @spike_bound)
    for i <- 1..900, do: insert(ctrl, i, "n#{i}")

    assert_receive {:sink_too_slow, %{lag: lag}}, 15_000
    IO.puts("\n[spike B] 900-txn burst tripped :sink_too_slow at in-flight lag = #{lag} B")
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
    for i <- 1..900, do: insert(ctrl, i, "n#{i}")

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

  # R01 (live): an UNKNOWN checkpoint (a raising `sink.checkpoint/0` → read fault, read as
  # checkpoint_lsn 0 with checkpoint_state :fault) combined with an ABSENT slot must halt
  # fail-closed and NEVER create a fresh slot. The §14.15 streaming fail-open (resume-from-0,
  # the idempotent sink dedups the re-stream) is safe only when the slot is PRESENT — a resume
  # clamps to the slot's confirmed_flush_lsn. With the slot ABSENT there is nothing to resume:
  # a fresh slot begins at its own creation LSN and silently skips every txn between the
  # (unknown) real checkpoint and now — unrecoverable loss. The unit test proves the connect
  # DECISION emits no CREATE_REPLICATION_SLOT; this drives it end-to-end against live PG and
  # proves the structural halt AND that no slot exists on the server afterwards.
  test "R01 live: unknown checkpoint + absent slot halts :data_gap and creates no slot", %{
    ctrl: ctrl,
    slot: slot
  } do
    # Precondition on the server: the slot is absent (setup dropped it).
    assert slot_count(ctrl, slot) == 0

    :telemetry.attach(
      {__MODULE__, :cp_unknown},
      [:replicant, :connection, :slot_invalidated],
      fn _e, _m, meta, pid ->
        if meta[:reason] == :checkpoint_unknown, do: send(pid, :checkpoint_unknown)
      end,
      self()
    )

    on_exit(fn -> :telemetry.detach({__MODULE__, :cp_unknown}) end)

    # Start the pipeline with a sink whose checkpoint/0 RAISES → checkpoint_state :fault. Do
    # NOT wait for :slot_active (it never fires — the pipeline halts at the connect decision
    # before streaming), so this starts directly rather than via `start_pipeline/2`.
    {:ok, _} =
      Replicant.start_link(
        connection: PG16.pg_opts(),
        slot_name: slot,
        publication: "orders_pub",
        sink: Replicant.Test.RaisingCheckpointLedgerSink,
        go_forward_only: true
      )

    # Structural fail-closed halt, distinct value-free reason (never a create-slot query).
    assert_receive :checkpoint_unknown, 15_000

    # The pipeline tore down permanently (fail-closed — not a reconnect livelock)...
    PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 400)

    # ...and — the load-bearing R01 assertion — NO replication slot was created on the server
    # for the unknown checkpoint: CREATE_REPLICATION_SLOT was never emitted.
    assert slot_count(ctrl, slot) == 0
  end

  # Count of server-side replication slots named `slot` (0 = never created / already dropped).
  defp slot_count(c, slot) do
    Postgrex.query!(c, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", [slot]).rows
    |> hd()
    |> hd()
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

  # The durable checkpoint LSN from `_replicant_checkpoint` (nil before the first commit).
  defp checkpoint_lsn(c) do
    case rows(c, "SELECT lsn FROM _replicant_checkpoint WHERE id = 1") do
      [[lsn]] -> lsn
      [] -> nil
    end
  end

  # Drive-and-verify the pipeline into the idle/keepalive window and return the idle LSN.
  #
  # Idle is a VERIFIED precondition of the kill (not a bare sleep): (1) the Connection's
  # durable `checkpoint_lsn` has advanced past 0 (a commit landed) and (2) its
  # `received_lsn` frontier is STABLE across a sampling interval — no new WAL is arriving,
  # so the stream has drained and the only Connection↔PG traffic is the primary-
  # keepalive/standby-status exchange (on which the Connection replies with `checkpoint_lsn`
  # while a txn is in flight, or advances to `wal_end` when idle — spec A1) — and (3) the
  # AssemblerServer's open buffer is nil (no txn mid-assembly). STALE NOTE (pre-A1): idle
  # `received_lsn` used to sit a small CONSTANT gap above `checkpoint_lsn` (the WAL of the
  # commit record's own tail after the last applied commit); this `wait_until_idle`
  # LOGIC is unaffected by A1 — it checks `checkpoint_lsn > 0` plus `received_lsn`
  # STABILITY, never `received == checkpoint` — but now the first idle keepalive advances
  # `checkpoint_lsn` itself to `wal_end >= received_lsn` (spec A1), so idleness is still
  # stability of the frontier, not equality of the two LSNs.
  defp wait_until_idle(conn, asm) do
    PG16.wait_until(fn -> idle_lsn(conn, asm) != nil end, 400)
    idle_lsn(conn, asm) || flunk_idle()
  end

  defp flunk_idle, do: ExUnit.Assertions.flunk("pipeline never reached a verified idle state")

  defp idle_lsn(conn, asm) do
    with %Replicant.Connection{checkpoint_lsn: cp, received_lsn: r1} when cp > 0 <-
           safe_conn_state(conn),
         nil <- buffered_txn(asm),
         :stable <- frontier_stable(conn, r1) do
      cp
    else
      _ -> nil
    end
  end

  # `:stable` when the Connection's `received_lsn` frontier is unchanged after a short
  # settle — i.e. no new WAL frame arrived in the window, the drained/idle signal.
  defp frontier_stable(conn, r1) do
    Process.sleep(60)

    case safe_conn_state(conn) do
      %Replicant.Connection{received_lsn: ^r1} -> :stable
      _ -> :moving
    end
  end

  # nil when the AssemblerServer has no open transaction buffer (idle), the buffer map
  # otherwise, `:unreadable` if the state can't be read. Confirms no txn is mid-assembly.
  defp buffered_txn(asm) do
    case :sys.get_state(asm, 200) do
      %{asm: %Replicant.Assembler{txn: txn}} -> txn
      _ -> :unreadable
    end
  rescue
    _ -> :unreadable
  catch
    _, _ -> :unreadable
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

  # Drop the slot, tolerating the transient "replication slot is active" window: after a
  # `Process.exit(conn, :kill)` the PG-side walsender releases the slot slightly AFTER the
  # BEAM pipeline tears down, so an immediate `pg_drop_replication_slot` can raise
  # `... is active for PID ...`. Retry on any error for ~1s, then a final raising attempt
  # so a genuine teardown fault (not the active-slot race) still surfaces. On a
  # non-existent slot the `WHERE` matches no rows → a no-op success (the `setup` pre-drop).
  defp drop_slot(c, slot), do: drop_slot(c, slot, 20)

  defp drop_slot(c, slot, 0) do
    Postgrex.query!(
      c,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  end

  defp drop_slot(c, slot, tries) do
    Postgrex.query!(
      c,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  rescue
    _ ->
      Process.sleep(50)
      drop_slot(c, slot, tries - 1)
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
  defp assembler_pid(slot), do: lookup_pid({slot, :assembler})

  defp lookup_pid(key) do
    PG16.wait_until(fn -> match?([{_, _}], Registry.lookup(Replicant.Registry, key)) end, 200)
    [{pid, _}] = Registry.lookup(Replicant.Registry, key)
    pid
  end
end

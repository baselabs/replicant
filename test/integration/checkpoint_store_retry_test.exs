defmodule Replicant.CheckpointStoreRetryTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.Test.PG16

  @cp_table "replicant_checkpoints_retry"
  # An unreachable STORE endpoint (the replication PG stays up): port 1 refuses; the short
  # queue params make Postgrex RETURN a %DBConnection.ConnectionError{} in ~150ms per attempt
  # (verified) rather than block. This is the exact F3 scenario — replication connect succeeds
  # while only the separate store conn is down.
  @unreachable [
    hostname: "127.0.0.1",
    port: 1,
    database: "postgres",
    username: "postgres",
    queue_target: 50,
    queue_interval: 50
  ]

  setup do
    {:ok, ctrl} =
      PG16.named_conn(Replicant.Test.CpRetryCtrl, pool_size: 3)

    {:ok, _} =
      PG16.named_conn(Replicant.Test.SinkLedgerConn, pool_size: 2)

    slot = "rep_cpretry_#{System.unique_integer([:positive])}"
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

  # (b)+(f) CONNECT-READ persistent outage (the F3 core): the store is unreachable at connect
  # while the replication PG is up. The connect read faults, paces max_retries FRESH reconnects
  # (emitting :retrying 1..N), then HALTS — and does NOT re-arm an N+1 retry cycle (terminal
  # guard, spec §9 — exhaustion stays idle, no reconnect).
  test "a persistent connect-read outage paces N retries then halts, no N+1 (terminal guard)", %{
    slot: slot
  } do
    if PG16.enabled?() do
      attach_retrying(self())

      {:ok, _} =
        Replicant.start_link(
          connection: PG16.pg_opts(),
          slot_name: slot,
          publication: "cp_retry_pub",
          sink: Replicant.Test.CheckpointRecordingSink,
          checkpoint_store: [
            connection: @unreachable,
            table: @cp_table,
            max_retries: 3,
            retry_backoff_ms: 100
          ]
        )

      assert_receive {:retrying, %{slot_name: ^slot, attempt: 1, max_retries: 3}}, 8000
      assert_receive {:retrying, %{slot_name: ^slot, attempt: 3, max_retries: 3}}, 8000

      PG16.wait_until(
        fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end,
        3000
      )

      refute pipeline_alive?(slot)
      # Terminal guard: exhaustion returns {:noreply} (no disconnect) → no attempt-4 cycle.
      refute_received {:retrying, %{attempt: 4}}
    end
  after
    :telemetry.detach({__MODULE__, :retrying})
  end

  # (c) transient mid-stream write blip self-heals: the CHECK blocks one write, then is
  # dropped within the retry window; the checkpoint advances and the pipeline keeps streaming.
  test "a transient mid-stream write fault self-heals within the retry window", %{
    ctrl: ctrl,
    slot: slot
  } do
    if PG16.enabled?() do
      start_streaming_pipeline(slot, max_retries: 20, retry_backoff_ms: 100)
      insert(ctrl, 1)
      PG16.wait_until(fn -> store_lsn(ctrl, slot) not in [nil, 0] end)
      lsn_after_1 = store_lsn(ctrl, slot)

      Postgrex.query!(
        ctrl,
        "ALTER TABLE #{@cp_table} ADD CONSTRAINT tmp_block CHECK (commit_lsn < 0) NOT VALID",
        []
      )

      insert(ctrl, 2)
      # sink persisted id 2 (autocommit)
      PG16.wait_until(fn -> MapSet.member?(ledger_ids(ctrl), 2) end)
      # within the 2s retry window
      Process.sleep(150)
      Postgrex.query!(ctrl, "ALTER TABLE #{@cp_table} DROP CONSTRAINT tmp_block", [])

      # the retried write landed
      PG16.wait_until(fn -> store_lsn(ctrl, slot) > lsn_after_1 end)
      # self-healed, not halted
      assert pipeline_alive?(slot)
      assert MapSet.subset?(MapSet.new([1, 2]), ledger_ids(ctrl))
      # The retried CHECKPOINT write does NOT re-apply the txn to the sink: the applier
      # blocks on the checkpoint (it never advanced to id 2's next txn), and the sink write
      # already committed exactly once BEFORE the failing checkpoint. So id 2 lands once —
      # the slice's dup≤1 guarantee holds on the self-heal path (no dup at all here).
      assert id_count(ctrl, 2) == 1
    end
  end

  # (d) persistent mid-stream write fault halts after max_retries with paced :retrying events.
  test "a persistent mid-stream write fault halts after max_retries", %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      attach_retrying(self())
      start_streaming_pipeline(slot, max_retries: 3, retry_backoff_ms: 100)
      insert(ctrl, 1)
      PG16.wait_until(fn -> store_lsn(ctrl, slot) not in [nil, 0] end)
      lsn_after_1 = store_lsn(ctrl, slot)

      Postgrex.query!(
        ctrl,
        "ALTER TABLE #{@cp_table} ADD CONSTRAINT tmp_block CHECK (commit_lsn < 0) NOT VALID",
        []
      )

      # sink persists id 2; checkpoint write retries 3× then halts
      insert(ctrl, 2)

      assert_receive {:retrying, %{slot_name: ^slot, attempt: 1, max_retries: 3}}, 8000
      assert_receive {:retrying, %{slot_name: ^slot, attempt: 3, max_retries: 3}}, 8000

      PG16.wait_until(
        fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end,
        3000
      )

      # halted after exhaustion
      refute pipeline_alive?(slot)
      # DISCRIMINATING invariant of the persisted-but-not-checkpointed halt: the sink DID
      # persist id 2 (autocommit) while the checkpoint did NOT advance past id 1 — the write
      # kept faulting through all 3 retries. (Without these, (d) would also pass if the sink
      # silently no-op'd or the checkpoint spuriously advanced before halting.)
      assert MapSet.member?(ledger_ids(ctrl), 2)
      assert store_lsn(ctrl, slot) == lsn_after_1
    end
  after
    :telemetry.detach({__MODULE__, :retrying})
  end

  # (e) a PERMANENT fault (pre-created wrong-type table) halts IMMEDIATELY, 0 :retrying events.
  test "a permanent schema mismatch halts immediately with no retries", %{ctrl: ctrl, slot: slot} do
    if PG16.enabled?() do
      attach_retrying(self())
      Postgrex.query!(ctrl, "DROP TABLE IF EXISTS #{@cp_table}", [])

      Postgrex.query!(
        ctrl,
        "CREATE TABLE #{@cp_table} (slot_name text PRIMARY KEY, commit_lsn text)",
        []
      )

      {:ok, _} =
        Replicant.start_link(
          connection: PG16.pg_opts(),
          slot_name: slot,
          publication: "cp_retry_pub",
          sink: Replicant.Test.CheckpointRecordingSink,
          checkpoint_store: [
            connection: PG16.pg_opts(),
            table: @cp_table,
            max_retries: 5,
            retry_backoff_ms: 100
          ]
        )

      # The connect-time store read probes the shape → schema mismatch → halt-now (no retry).
      PG16.wait_until(
        fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end,
        3000
      )

      refute_received {:retrying, _}
      refute pipeline_alive?(slot)
    end
  after
    :telemetry.detach({__MODULE__, :retrying})
  end

  # NOTE (spec §12 (a) transient connect-read self-heal): a self-CLEARING connect-read
  # CONNECTION fault is not cleanly injectable in this shared-superuser-PG harness (the store
  # is the live PG; a wrong-type table is PERMANENT not transient; a superuser bypasses GRANT
  # revocation). The transient connect-read path shares the exact `pace_store_retry` /
  # `store_retry_step` / reconnect mechanism proven end-to-end by (b), differing only in that
  # the read eventually SUCCEEDS and `reset_retry_count/2` clears the counter — which is
  # unit-covered in Task 4 (`connection_test.exs` reset test). So the self-heal reset is
  # covered; the persistent path is integration-covered.

  # ---- helpers ----

  # Start a lib-mode pipeline and BLOCK on the [:replicant, :connection, :slot_active] event —
  # the ONLY safe readiness gate (waiting on Registry/pipeline presence is a RACE: the Connection
  # registers its name in init BEFORE creating the slot, so an insert in that window commits WAL
  # ahead of the slot's start LSN and is never streamed; verified in checkpoint_store_crash_test).
  defp start_streaming_pipeline(slot, retry_opts) do
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
        publication: "cp_retry_pub",
        sink: Replicant.Test.CheckpointRecordingSink,
        checkpoint_store: [connection: PG16.pg_opts(), table: @cp_table] ++ retry_opts
      )

    receive do
      {:slot_active, ^ref} -> :ok
    after
      15_000 -> ExUnit.Assertions.flunk("pipeline never reached slot_active for #{slot}")
    end

    :telemetry.detach({__MODULE__, ref})
  end

  defp pipeline_alive?(slot), do: Registry.lookup(Replicant.Registry, {slot, :pipeline}) != []

  defp attach_retrying(pid) do
    :telemetry.attach(
      {__MODULE__, :retrying},
      [:replicant, :checkpoint_store, :retrying],
      fn _n, _m, meta, p -> send(p, {:retrying, meta}) end,
      pid
    )
  end

  defp reset_schema(c) do
    Postgrex.query!(c, "DROP PUBLICATION IF EXISTS cp_retry_pub", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS cp_retry_orders", [])
    Postgrex.query!(c, "CREATE TABLE cp_retry_orders (id int PRIMARY KEY)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS sink_ledger", [])
    Postgrex.query!(c, "CREATE TABLE sink_ledger (commit_lsn bigint, id int)", [])
    Postgrex.query!(c, "DROP TABLE IF EXISTS #{@cp_table}", [])
    Postgrex.query!(c, "CREATE PUBLICATION cp_retry_pub FOR TABLE cp_retry_orders", [])
  end

  # Drop the slot, tolerating the transient "replication slot is active" window: after a
  # pipeline teardown the PG-side walsender releases the slot slightly AFTER the BEAM pipeline
  # tears down, so an immediate `pg_drop_replication_slot` can raise `... is active for PID ...`
  # (waiting on `Registry gone` does NOT guarantee the slot is droppable yet). Retry on any
  # error for ~1s, then a final raising attempt so a genuine teardown fault (not the active-slot
  # race) still surfaces. On a non-existent slot the `WHERE` matches no rows → a no-op success
  # (the `setup` pre-drop). Copied verbatim from `checkpoint_store_crash_test.exs`.
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

  defp insert(c, id), do: Postgrex.query!(c, "INSERT INTO cp_retry_orders (id) VALUES ($1)", [id])

  defp ledger_ids(c) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(c, "SELECT DISTINCT id FROM sink_ledger", [])
    rows |> Enum.map(fn [id] -> id end) |> MapSet.new()
  end

  defp id_count(c, id) do
    %Postgrex.Result{rows: [[n]]} =
      Postgrex.query!(c, "SELECT count(*) FROM sink_ledger WHERE id = $1", [id])

    n
  end

  defp store_lsn(c, slot) do
    case Postgrex.query(c, "SELECT commit_lsn FROM #{@cp_table} WHERE slot_name = $1", [slot]) do
      {:ok, %Postgrex.Result{rows: [[lsn]]}} -> lsn
      _ -> nil
    end
  end
end

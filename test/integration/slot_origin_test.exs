defmodule Replicant.SlotOriginIntegrationTest do
  @moduledoc """
  R04 — the public `handle_slot_origin/2` contract, proven against a live PostgreSQL slot.

  Proves the exposed origin comes from the ACTUAL slot operation (not a fabricated value) and
  survives reconnect semantics:

    * NEW slot — the origin (reused?: false) equals the `CREATE_REPLICATION_SLOT` consistent_point,
      which must fall in the source-WAL window the slot was created in.
    * REUSED slot — a forced reconnect re-invokes the callback (reused?: true) with the slot's
      current `confirmed_flush_lsn`, which has ADVANCED past the new-slot origin and is bracketed by
      the live `pg_replication_slots.confirmed_flush_lsn` read across the reconnect.
  """
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag timeout: 60_000

  alias Replicant.Test.PG16

  defmodule OriginSink do
    @behaviour Replicant.Sink
    @pid_key {__MODULE__, :test_pid}
    @cp_key {__MODULE__, :checkpoint}

    def set_test_pid(pid), do: :persistent_term.put(@pid_key, pid)
    def clear_test_pid, do: :persistent_term.erase(@pid_key)
    def reset_checkpoint, do: :persistent_term.put(@cp_key, nil)

    @impl true
    # Sink-owned checkpoint so the ack advances the slot's confirmed_flush_lsn.
    def checkpoint, do: {:ok, :persistent_term.get(@cp_key, nil)}

    @impl true
    def handle_transaction(%Replicant.Transaction{commit_lsn: lsn}) do
      :persistent_term.put(@cp_key, lsn)
      send(:persistent_term.get(@pid_key), {:txn, lsn})
      {:ok, lsn}
    end

    @impl true
    def sink_kind, do: :append_log

    @impl true
    def handle_slot_origin(origin, context) do
      send(:persistent_term.get(@pid_key), {:slot_origin, origin, context})
      :ok
    end
  end

  setup do
    {:ok, ctrl} = PG16.named_conn(Replicant.Test.SlotOriginCtrl, pool_size: 2)
    slot = "rep_origin_#{System.unique_integer([:positive])}"
    publication = "rep_origin_pub"

    Postgrex.query!(ctrl, "DROP PUBLICATION IF EXISTS #{publication}", [])
    Postgrex.query!(ctrl, "DROP TABLE IF EXISTS rep_origin_rows", [])
    Postgrex.query!(ctrl, "CREATE TABLE rep_origin_rows (id bigint PRIMARY KEY)", [])
    Postgrex.query!(ctrl, "CREATE PUBLICATION #{publication} FOR TABLE rep_origin_rows", [])

    OriginSink.set_test_pid(self())
    OriginSink.reset_checkpoint()

    on_exit(fn ->
      OriginSink.clear_test_pid()
      Replicant.stop(slot)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 400)

      {:ok, cleanup} = Postgrex.start_link(PG16.pg_opts())

      Postgrex.query!(
        cleanup,
        "SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1",
        [slot]
      )

      Postgrex.query!(cleanup, "DROP PUBLICATION IF EXISTS #{publication}", [])
      Postgrex.query!(cleanup, "DROP TABLE IF EXISTS rep_origin_rows", [])
      GenServer.stop(cleanup)
    end)

    %{ctrl: ctrl, slot: slot, publication: publication}
  end

  test "exposes the new-slot consistent_point, then the reused slot's advanced confirmed_flush on reconnect",
       %{ctrl: ctrl, slot: slot, publication: publication} do
    if PG16.enabled?() do
      wal_before = source_wal(ctrl)

      {:ok, _pipeline} =
        Replicant.start_link(
          connection: PG16.pg_opts(),
          slot_name: slot,
          publication: publication,
          sink: OriginSink,
          go_forward_only: true
        )

      # --- new slot: origin came from the actual CREATE (reused?: false) ---
      assert_receive {:slot_origin, new_origin, %{slot_name: ^slot, reused?: false}}, 15_000
      wal_after = source_wal(ctrl)

      assert new_origin > 0
      # The consistent_point falls in the source-WAL window the slot was created in — a real
      # creation LSN, not a fabricated constant.
      assert wal_before <= new_origin and new_origin <= wal_after

      # Drive a transaction so the ack advances the slot's confirmed_flush_lsn past the origin.
      Postgrex.query!(ctrl, "INSERT INTO rep_origin_rows (id) VALUES (1)", [])
      assert_receive {:txn, _lsn}, 15_000
      PG16.wait_until(fn -> confirmed_flush(ctrl, slot) > new_origin end, 400)
      cf_before_reconnect = confirmed_flush(ctrl, slot)

      # --- reused slot: reconnect re-notifies with the advanced confirmed_flush (reused?: true) ---
      PG16.wait_until(fn -> connection_pid(slot) != nil end, 400)
      old_connection = connection_pid(slot)
      Process.exit(old_connection, :kill)
      PG16.wait_until(fn -> connection_pid(slot) not in [nil, old_connection] end, 400)

      assert_receive {:slot_origin, reused_origin, %{slot_name: ^slot, reused?: true}}, 15_000
      cf_after_reconnect = confirmed_flush(ctrl, slot)

      # It advanced past the new-slot origin AND is bracketed by the live slot state across the
      # reconnect — the value comes from the actual slot, not a stale creation constant.
      assert reused_origin > new_origin
      assert cf_before_reconnect <= reused_origin and reused_origin <= cf_after_reconnect
    end
  end

  defp source_wal(conn) do
    %Postgrex.Result{rows: [[lsn]]} =
      Postgrex.query!(conn, "SELECT pg_current_wal_lsn()::text", [])

    Replicant.lsn_from_string(lsn)
  end

  defp confirmed_flush(conn, slot) do
    %Postgrex.Result{rows: [[lsn]]} =
      Postgrex.query!(
        conn,
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
        [slot]
      )

    if lsn, do: Replicant.lsn_from_string(lsn), else: 0
  end

  defp connection_pid(slot) do
    case Registry.lookup(Replicant.Registry, {slot, :connection}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end
end

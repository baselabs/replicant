defmodule Replicant.SessionIdentityIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag timeout: 60_000

  alias Replicant.SessionIdentity
  alias Replicant.Test.PG16

  defmodule IdentitySink do
    @behaviour Replicant.Sink
    @key {__MODULE__, :test_pid}

    def set_test_pid(pid), do: :persistent_term.put(@key, pid)
    def clear_test_pid, do: :persistent_term.erase(@key)

    @impl true
    def handle_session_identity(identity, context) do
      send(:persistent_term.get(@key), {:session_identity, identity, context})
      :ok
    end

    @impl true
    def checkpoint do
      send(:persistent_term.get(@key), :checkpoint_read)
      {:ok, nil}
    end

    @impl true
    def handle_transaction(%Replicant.Transaction{commit_lsn: lsn}), do: {:ok, lsn}

    @impl true
    def sink_kind, do: :append_log
  end

  setup do
    {:ok, ctrl} = PG16.named_conn(Replicant.Test.SessionIdentityCtrl, pool_size: 2)
    slot = "rep_identity_#{System.unique_integer([:positive])}"
    publication = "rep_identity_pub"

    Postgrex.query!(ctrl, "DROP PUBLICATION IF EXISTS #{publication}", [])
    Postgrex.query!(ctrl, "DROP TABLE IF EXISTS rep_identity_rows", [])
    Postgrex.query!(ctrl, "CREATE TABLE rep_identity_rows (id bigint PRIMARY KEY)", [])
    Postgrex.query!(ctrl, "CREATE PUBLICATION #{publication} FOR TABLE rep_identity_rows", [])

    IdentitySink.set_test_pid(self())

    on_exit(fn ->
      IdentitySink.clear_test_pid()
      Replicant.stop(slot)
      PG16.wait_until(fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end, 400)

      {:ok, cleanup} = Postgrex.start_link(PG16.pg_opts())

      Postgrex.query!(
        cleanup,
        "SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1",
        [slot]
      )

      Postgrex.query!(cleanup, "DROP PUBLICATION IF EXISTS #{publication}", [])
      Postgrex.query!(cleanup, "DROP TABLE IF EXISTS rep_identity_rows", [])
      GenServer.stop(cleanup)
    end)

    %{ctrl: ctrl, slot: slot, publication: publication}
  end

  test "reports the live source before checkpoint lookup and repeats the check on reconnect",
       %{ctrl: ctrl, slot: slot, publication: publication} do
    if PG16.enabled?() do
      expected = live_identity(ctrl)

      {:ok, _pipeline} =
        Replicant.start_link(
          connection: PG16.pg_opts(),
          slot_name: slot,
          publication: publication,
          sink: IdentitySink,
          go_forward_only: true
        )

      assert_identity_then_checkpoint(expected, slot, publication)

      PG16.wait_until(fn -> connection_pid(slot) != nil end, 400)
      old_connection = connection_pid(slot)
      Process.exit(old_connection, :kill)
      PG16.wait_until(fn -> connection_pid(slot) not in [nil, old_connection] end, 400)

      assert_identity_then_checkpoint(expected, slot, publication)
    end
  end

  defp assert_identity_then_checkpoint(expected, slot, publication) do
    assert_receive first_event, 15_000

    assert {:session_identity, %SessionIdentity{} = identity,
            %{slot_name: ^slot, publication: [^publication]}} = first_event

    assert identity.system_identifier == expected.system_identifier
    assert identity.timeline_id == expected.timeline_id
    assert identity.database == expected.database
    assert identity.current_lsn >= expected.current_lsn

    assert_receive :checkpoint_read, 5_000
  end

  defp live_identity(conn) do
    %Postgrex.Result{rows: [[system_identifier, database, timeline_id, current_lsn]]} =
      Postgrex.query!(
        conn,
        "SELECT (pg_control_system()).system_identifier::text, current_database(), " <>
          "(pg_control_checkpoint()).timeline_id, pg_current_wal_lsn()::text",
        []
      )

    %SessionIdentity{
      system_identifier: system_identifier,
      database: database,
      timeline_id: timeline_id,
      current_lsn: Replicant.lsn_from_string(current_lsn)
    }
  end

  defp connection_pid(slot) do
    case Registry.lookup(Replicant.Registry, {slot, :connection}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end
end

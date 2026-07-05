defmodule Replicant.CheckpointStoreTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.{CheckpointStore, Error}
  alias Replicant.Test.PG16

  setup do
    {:ok, ctrl} = Postgrex.start_link(PG16.pg_opts())
    table = "rep_cp_#{System.unique_integer([:positive])}"
    slot = "rep_cps_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      {:ok, c} = Postgrex.start_link(PG16.pg_opts())
      Postgrex.query(c, "DROP TABLE IF EXISTS #{table}", [])
    end)

    %{ctrl: ctrl, table: table, slot: slot}
  end

  defp start(slot, table) do
    # Replicant.Registry is started by the application (application.ex:8) — do NOT start it
    # here (it would return {:error, {:already_started, _}} and MatchError).
    start_supervised!(
      {CheckpointStore,
       slot_name: slot, checkpoint_store: [connection: PG16.pg_opts(), table: table]}
    )
  end

  @tag :integration
  test "read is nil on an empty store; write then read round-trips", %{slot: slot, table: table} do
    if PG16.enabled?() do
      pid = start(slot, table)
      assert {:ok, nil} == CheckpointStore.read(pid)
      assert :ok == CheckpointStore.write(pid, 0x16E3778)
      assert {:ok, 0x16E3778} == CheckpointStore.read(pid)
      assert :ok == CheckpointStore.write(pid, 0x16E3999)
      assert {:ok, 0x16E3999} == CheckpointStore.read(pid)
    end
  end

  @tag :integration
  test "a pre-existing table with a non-bigint commit_lsn halts fail-closed", %{
    ctrl: ctrl,
    slot: slot,
    table: table
  } do
    if PG16.enabled?() do
      Postgrex.query!(
        ctrl,
        "CREATE TABLE #{table} (slot_name text PRIMARY KEY, commit_lsn text)",
        []
      )

      pid = start(slot, table)

      assert {:error, %Error{reason: :checkpoint_store_schema_mismatch}} =
               CheckpointStore.read(pid)
    end
  end
end

defmodule Replicant.CheckpointStoreFaultTest do
  # Fault-boundary tests that need NO live Postgres. Two fault classes that the
  # integration fault tests do NOT exercise (they inject via a Postgres CHECK
  # violation = %Postgrex.Error{}, and a schema mismatch = %Error{}):
  #
  #   1. a store connection OUTAGE — Postgrex RETURNS {:error, %DBConnection.ConnectionError{}}
  #      (not %Postgrex.Error{}, not a raise), the exact "boot blip self-heals" / persistent
  #      outage path the design relies on;
  #   2. a NOT-STARTED / dead store — a raw GenServer.call exits :noproc in the caller.
  #
  # Both must surface a value-free {:error, %Error{}} THROUGH the store boundary so the
  # Connection (connect :fault) and Assembler (write halt) fail-closed paths engage — never
  # crash the store GenServer, never exit into the calling Connection.
  use ExUnit.Case, async: false

  alias Replicant.{CheckpointStore, Error}

  # A connection that can never be established: port 1 is closed → ECONNREFUSED. With
  # sync_connect: false the store starts; the first query's checkout fails and Postgrex
  # RETURNS {:error, %DBConnection.ConnectionError{}}. The short queue params make that
  # return land in ~150ms (well under the 5s GenServer.call timeout), so the test exercises
  # the ConnectionError-RETURN path deterministically — not a call timeout.
  @unreachable [
    hostname: "127.0.0.1",
    port: 1,
    database: "postgres",
    username: "postgres",
    queue_target: 50,
    queue_interval: 50
  ]

  test "a store connection OUTAGE surfaces a value-free error and the store GenServer survives" do
    slot = "cp_fault_outage_#{System.unique_integer([:positive])}"

    pid =
      start_supervised!(
        {CheckpointStore, slot_name: slot, checkpoint_store: [connection: @unreachable]}
      )

    # A DBConnection.ConnectionError that guarded/1 fails to scrub falls through to ensure/1's
    # `case`, raising CaseClauseError and crashing the store (and exiting the caller). Post-fix
    # it is scrubbed to a value-free error and the store survives.
    assert {:error, %Error{reason: :checkpoint_store_failed}} = CheckpointStore.read(pid)
    assert {:error, %Error{reason: :checkpoint_store_failed}} = CheckpointStore.write(pid, 42)
    assert Process.alive?(pid)
  end

  test "read/write on a NOT-STARTED store return a value-free error, never a :noproc exit" do
    # No CheckpointStore is registered under this slot → a raw GenServer.call exits :noproc
    # and would crash the calling Connection. The client API converts it to {:error, _} so the
    # fail-closed paths (connect :fault / snapshot-handoff halt) engage cleanly.
    via = CheckpointStore.via("cp_fault_noproc_#{System.unique_integer([:positive])}")

    assert {:error, %Error{reason: :checkpoint_store_failed}} = CheckpointStore.read(via)
    assert {:error, %Error{reason: :checkpoint_store_failed}} = CheckpointStore.write(via, 7)
  end
end

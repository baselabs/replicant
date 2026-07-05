defmodule Replicant.SupervisorTest do
  use ExUnit.Case, async: false

  test "the application boots the Registry and the pipeline DynamicSupervisor" do
    assert is_pid(Process.whereis(Replicant.Registry))
    assert is_pid(Process.whereis(Replicant.Supervisor))
    # It is a DynamicSupervisor (responds to count_children with an :active key).
    assert Map.has_key?(DynamicSupervisor.count_children(Replicant.Supervisor), :active)
  end

  test "halt/2 on an unknown slot is a no-op (idempotent teardown, no crash)" do
    assert :ok = Replicant.Supervisor.halt("no_such_slot", :slot_invalidated)
  end

  test "stop_pipeline/1 on an unknown slot is a no-op" do
    assert :ok = Replicant.Supervisor.stop_pipeline("no_such_slot")
  end
end

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

  test "halt telemetry discards values in arbitrary reasons" do
    handler = {__MODULE__, make_ref()}
    :telemetry.attach(handler, [:replicant, :pipeline, :halted], &__MODULE__.halted/4, self())
    on_exit(fn -> :telemetry.detach(handler) end)

    for {input, expected} <- [
          {{:snapshot, "secret"}, %{reason: :snapshot}},
          {%Replicant.Error{reason: :snapshot_failed, message: "secret"},
           %{reason: :snapshot_failed}},
          {{"secret", %{payload: "secret"}}, %{reason: :pipeline_halted}}
        ] do
      Replicant.Supervisor.halt("halt_offline", input)
      assert_receive {:halted, meta}
      assert meta == Map.put(expected, :slot_name, "halt_offline")
      refute inspect(meta) =~ "secret"
    end
  end

  def halted(_, _, meta, pid), do: send(pid, {:halted, meta})

  test "stop_pipeline/1 on an unknown slot is a no-op" do
    assert :ok = Replicant.Supervisor.stop_pipeline("no_such_slot")
  end
end

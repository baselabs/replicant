defmodule Replicant.PipelineTest do
  use ExUnit.Case, async: false

  defmodule StateMirrorEmpty do
    @behaviour Replicant.Sink
    @impl true
    def checkpoint, do: {:ok, nil}
    @impl true
    def handle_transaction(_txn), do: {:ok, 0}
  end

  # Point at a refused port: with sync_connect: false the pipeline starts without a
  # live PG; the Connection retries asynchronously (offline structure test only).
  @conn [hostname: "127.0.0.1", port: 1, username: "u", password: "p", database: "d"]

  defp opts(slot, extra) do
    [connection: @conn, slot_name: slot, publication: "orders_pub", sink: StateMirrorEmpty] ++
      extra
  end

  defp wait_gone(slot, tries \\ 100) do
    cond do
      tries == 0 -> :timeout
      Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] -> :ok
      true -> Process.sleep(20) && wait_gone(slot, tries - 1)
    end
  end

  test "the go-forward guard refuses a state-mirror sink from an empty checkpoint (nothing started)" do
    assert {:error, :go_forward_required} = Replicant.start_link(opts("pl_guard", []))
    assert Registry.lookup(Replicant.Registry, {"pl_guard", :pipeline}) == []
  end

  test "rejects an invalid slot identifier before starting anything" do
    assert {:error, :invalid_identifier} = Replicant.start_link(opts("bad'; DROP", []))
  end

  test "starts a supervised pipeline (Connection + AssemblerServer registered) then stops it" do
    assert {:ok, pid} = Replicant.start_link(opts("pl_ok", go_forward_only: true))
    assert is_pid(pid)
    assert [{^pid, _}] = Registry.lookup(Replicant.Registry, {"pl_ok", :pipeline})
    assert [{_conn, _}] = Registry.lookup(Replicant.Registry, {"pl_ok", :connection})
    assert [{_asm, _}] = Registry.lookup(Replicant.Registry, {"pl_ok", :assembler})

    assert :ok = Replicant.stop("pl_ok")
    assert :ok = wait_gone("pl_ok")
  end

  test "Supervisor.halt/2 tears the pipeline down permanently (temporary child → no restart)" do
    {:ok, pid} = Replicant.start_link(opts("pl_halt", go_forward_only: true))
    ref = Process.monitor(pid)

    Replicant.Supervisor.halt("pl_halt", :slot_invalidated)

    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 2000
    assert :ok = wait_gone("pl_halt")
    # Not restarted: no pipeline child re-registers under the slot.
    assert Registry.lookup(Replicant.Registry, {"pl_halt", :pipeline}) == []
  end
end

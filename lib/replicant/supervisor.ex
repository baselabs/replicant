defmodule Replicant.Supervisor do
  @moduledoc """
  The top `DynamicSupervisor` — one child per running pipeline (a
  `Replicant.Pipeline` supervisor). Pipelines run as **`:temporary`** children:
  a fail-closed halt terminates one permanently (no restart), while transient
  crashes are recovered inside each `Pipeline`'s own `:one_for_all` strategy.
  """
  use DynamicSupervisor

  @spec start_link(term()) :: Supervisor.on_start()
  def start_link(init_arg \\ []) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg), do: DynamicSupervisor.init(strategy: :one_for_one)

  @doc """
  Start a pipeline as a `:temporary` child — a fail-closed halt (`halt/2`) must
  terminate it permanently, so the DynamicSupervisor must not restart it.
  """
  @spec start_pipeline(Replicant.Config.t()) :: DynamicSupervisor.on_start_child()
  def start_pipeline(config) do
    spec = %{
      id: {Replicant.Pipeline, config.slot_name},
      start: {Replicant.Pipeline, :start_link, [config]},
      type: :supervisor,
      restart: :temporary
    }

    DynamicSupervisor.start_child(__MODULE__, spec)
  end

  @doc "Terminate a running pipeline by slot name (idempotent)."
  @spec stop_pipeline(String.t()) :: :ok
  def stop_pipeline(slot_name), do: terminate(slot_name)

  @doc """
  Fail-closed halt of a pipeline from *within* its own tree. Terminates the whole
  pipeline permanently (it is a `:temporary` DynamicSupervisor child → no
  restart). The teardown runs in an **unlinked spawned process** to avoid the
  self-termination deadlock — a child process cannot synchronously stop its own
  ancestor supervisor. The caller owns the distinguishing telemetry
  (`:slot_invalidated` / `:sink :failed` / `:schema_change :halted`); `halt/2`
  performs only the teardown and returns immediately.
  """
  @spec halt(String.t(), term()) :: :ok
  def halt(slot_name, _reason) do
    _pid = spawn(fn -> terminate(slot_name) end)
    :ok
  end

  defp terminate(slot_name) do
    case Registry.lookup(Replicant.Registry, {slot_name, :pipeline}) do
      [{pid, _value}] -> _ = DynamicSupervisor.terminate_child(__MODULE__, pid)
      [] -> :ok
    end

    :ok
  end
end

defmodule Replicant.Pipeline do
  @moduledoc """
  Per-pipeline `Supervisor` (spec §4): supervises the `Replicant.AssemblerServer`
  and the `Replicant.Connection` under **`:one_for_all`** — a crash of either
  restarts both together, so a fresh Connection (resuming from the sink checkpoint)
  is never paired with a stale in-memory assembler buffer. Registered by slot in
  `Replicant.Registry` so `Replicant.Supervisor.halt/2` can terminate the whole
  pipeline permanently on a fail-closed condition.

  The AssemblerServer starts first (the Connection casts decoded messages to it;
  with `sync_connect: false` the Connection does not stream until after boot).
  """
  use Supervisor

  @spec start_link(Replicant.Config.t()) :: Supervisor.on_start()
  def start_link(config) do
    Supervisor.start_link(__MODULE__, config, name: via(config.slot_name))
  end

  @doc "The Registry via-name a pipeline supervisor registers under."
  @spec via(String.t()) :: {:via, module(), term()}
  def via(slot_name), do: {:via, Registry, {Replicant.Registry, {slot_name, :pipeline}}}

  @impl true
  def init(config) do
    children = [
      {Replicant.AssemblerServer, slot_name: config.slot_name, sink: config.sink},
      {Replicant.Connection, config}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end

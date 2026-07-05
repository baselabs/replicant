defmodule Replicant.Pipeline do
  @moduledoc """
  Per-pipeline `Supervisor` (spec §4): supervises the `Replicant.AssemblerServer`
  and the `Replicant.Connection` under **`:one_for_all`** — a crash of either
  restarts both together, so a fresh Connection (resuming from the sink checkpoint)
  is never paired with a stale in-memory assembler buffer. Registered by slot in
  `Replicant.Registry` so `Replicant.Supervisor.halt/2` can terminate the whole
  pipeline permanently on a fail-closed condition.

  The AssemblerServer starts before the Connection (the Connection casts decoded
  messages to it; with `sync_connect: false` the Connection does not stream until
  after boot). In lib mode the CheckpointStore starts first of all, so the
  Connection can read it on connect and the AssemblerServer can write through it.
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
    children =
      checkpoint_store_child(config) ++
        [
          {Replicant.AssemblerServer, assembler_opts(config)},
          {Replicant.Connection, config}
        ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  # Lib mode: the CheckpointStore is the FIRST child (the Connection reads it on connect,
  # the AssemblerServer writes through it). It uses a non-sync Postgrex connect, so a boot
  # blip does not fail the `:temporary` pipeline.
  defp checkpoint_store_child(%{checkpoint_store: store, slot_name: slot}) when is_list(store),
    do: [{Replicant.CheckpointStore, slot_name: slot, checkpoint_store: store}]

  defp checkpoint_store_child(_config), do: []

  defp assembler_opts(%{checkpoint_store: store, slot_name: slot, sink: sink})
       when is_list(store),
       do: [slot_name: slot, sink: sink, checkpoint_store: store]

  defp assembler_opts(%{slot_name: slot, sink: sink}),
    do: [slot_name: slot, sink: sink]
end

defmodule Replicant.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Replicant.Registry},
      Replicant.Supervisor
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Replicant.RootSupervisor)
  end
end

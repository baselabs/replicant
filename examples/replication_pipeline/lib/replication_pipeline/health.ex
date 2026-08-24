defmodule ReplicationPipeline.Health do
  @moduledoc """
  Liveness for the release container: a fail-closed halt (ADR-0006) tears the
  pipeline down permanently while the BEAM node stays up — the container looks
  "running" with dead delivery. `healthy?/0` is the release-rpc healthcheck:
  `:ok` iff a pipeline child is present under `Replicant.Supervisor`; a
  missing child raises, so the rpc (and therefore the compose healthcheck)
  exits non-zero and `docker compose ps` shows the halt.
  """

  @spec healthy?() :: :ok
  def healthy? do
    case DynamicSupervisor.which_children(Replicant.Supervisor) do
      [] ->
        raise "pipeline halted: no pipeline child under Replicant.Supervisor"

      _children ->
        :ok
    end
  end
end

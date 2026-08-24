defmodule ReplicationPipeline.Application do
  @moduledoc """
  Boots the destination connection, then the replicant pipeline against it.

  The `Postgrex` child uses `sync_connect: true` so the named destination
  connection is live BEFORE the pipeline starts delivering — a sink callback
  racing an unfinished connect would otherwise fault on first delivery.
  """

  use Application

  @impl true
  def start(_type, _args) do
    # Attach BEFORE the pipeline starts so an early fail-closed halt is
    # logged rather than silently tearing the pipeline down.
    :ok = ReplicationPipeline.TelemetryLog.attach()

    children = [
      {Postgrex, dest_opts()},
      # Replicant exposes start_link/1 without a child_spec/1, so the child is
      # started through an explicit spec map.
      %{id: :replicant_pipeline, start: {Replicant, :start_link, [pipeline_opts()]}}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: ReplicationPipeline.Supervisor)
  end

  defp dest_opts do
    [
      name: ReplicationPipeline.Dest,
      hostname: env!("DEST_HOST"),
      port: env_int!("DEST_PORT"),
      username: env!("DEST_USER"),
      database: env!("DEST_DB"),
      sync_connect: true
    ]
  end

  defp pipeline_opts do
    [
      connection: [
        hostname: env!("SOURCE_HOST"),
        port: env_int!("SOURCE_PORT"),
        username: env!("SOURCE_USER"),
        database: env!("SOURCE_DB")
      ],
      slot_name: env!("REPLICANT_SLOT_NAME"),
      publication: env!("REPLICANT_PUBLICATION"),
      sink: ReplicationPipeline.Sink,
      # A :state_mirror sink from an empty checkpoint must declare its intent:
      # this example streams NEW changes only. Pre-existing source rows are NOT
      # backfilled — `snapshot: true` is the one-flag alternative (see README).
      go_forward_only: true
    ]
  end

  defp env!(name), do: System.fetch_env!(name)

  defp env_int!(name) do
    case Integer.parse(env!(name)) do
      {value, ""} -> value
      _ -> raise ArgumentError, "env #{name} is not an integer"
    end
  end
end

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
      sink: ReplicationPipeline.Sink,
      # A :state_mirror sink from an empty checkpoint must declare its intent:
      # this example streams NEW changes only. Pre-existing source rows are NOT
      # backfilled — `snapshot: true` is the one-flag alternative (see README).
      go_forward_only: true
    ]
    |> Keyword.merge(decoder_opts(env("REPLICANT_DECODER", "pgoutput")))
  end

  # The decoder's table-set key is per-decoder (ADR-0009): `publication:` for the
  # default pgoutput stack, `replication_sets:` for pglogical, `tables:` for
  # wal2json — so the same container image reads a pre-15 source through its
  # existing output plugin by changing REPLICANT_DECODER (plus its table-set env).
  defp decoder_opts("pgoutput"), do: [publication: env!("REPLICANT_PUBLICATION")]

  defp decoder_opts("pglogical") do
    [
      decoder: :pglogical,
      replication_sets: env_list("REPLICANT_REPLICATION_SETS", ["default"])
    ]
  end

  defp decoder_opts("wal2json") do
    [
      decoder: :wal2json,
      tables: env_tables("REPLICANT_TABLES", [{"public", "orders"}])
    ]
    |> allow_keyless()
  end

  defp allow_keyless(opts) do
    if env("REPLICANT_ALLOW_KEYLESS", "false") == "true",
      do: Keyword.put(opts, :allow_keyless_tables, true),
      else: opts
  end

  defp env(name, default) do
    case System.fetch_env(name) do
      {:ok, value} -> value
      :error -> default
    end
  end

  defp env_list(name, default) do
    case System.fetch_env(name) do
      {:ok, value} -> value |> String.split(",", trim: true) |> Enum.map(&String.trim/1)
      :error -> default
    end
  end

  defp env_tables(name, default) do
    case System.fetch_env(name) do
      {:ok, value} -> value |> String.split(",", trim: true) |> Enum.map(&parse_table/1)
      :error -> default
    end
  end

  defp parse_table(qualified) do
    case qualified |> String.trim() |> String.split(".") do
      [schema, table] -> {schema, table}
      _other -> raise ArgumentError, "REPLICANT_TABLES needs schema.table pairs"
    end
  end

  defp env!(name), do: System.fetch_env!(name)

  defp env_int!(name) do
    case Integer.parse(env!(name)) do
      {value, ""} -> value
      _ -> raise ArgumentError, "env #{name} is not an integer"
    end
  end
end

defmodule ReplicationPipeline.MixProject do
  use Mix.Project

  # The reference pipeline: replicant → idempotent orders replica + durable
  # commit-LSN checkpoint in a destination Postgres. Repo-side example — never
  # shipped in the Hex package (see package files in the root mix.exs). Run it
  # with docker compose; see README.md.
  def project do
    [
      app: :replication_pipeline,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {ReplicationPipeline.Application, []}
    ]
  end

  defp deps do
    [
      # The CDC engine, from THIS checkout (the example is part of its repo).
      {:replicant, path: "../.."},
      # Destination client (mirrors the library's own floor).
      {:postgrex, "~> 0.22.4"},
      # Static gates (same tooling as the library itself; dev/test only).
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end
end

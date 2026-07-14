defmodule Replicant.MixProject do
  use Mix.Project

  @version "0.2.0"
  @source_url "https://github.com/baselabs/replicant"

  def project do
    [
      app: :replicant,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      consolidate_protocols: Mix.env() != :test,
      deps: deps(),
      package: package(),
      docs: docs(),
      aliases: aliases(),
      name: "Replicant",
      description:
        "Framework-agnostic Elixir CDC consumer for Postgres logical replication (pgoutput) " <>
          "with sink-owned, transaction-granularity exactly-once delivery.",
      source_url: @source_url,
      homepage_url: @source_url,
      dialyzer: [
        plt_add_apps: [:mix, :ex_unit, :decimal, :jason, :postgrex],
        plt_core_path: "priv/plts",
        plt_local_path: "priv/plts"
      ]
    ]
  end

  def cli do
    [preferred_envs: [credo: :test, dialyzer: :test]]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Replicant.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # Runtime (Plan 1 consumes decimal + jason via casting; telemetry via the helper)
      {:decimal, "~> 3.1"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.0"},
      # Live streaming (Plan 2): the ReplicationConnection substrate. Floor 0.22.2
      # (NOT ~> 0.20 — 0.20 requires decimal ~> 1.5/2.0 and conflicts with decimal
      # ~> 3.1; 0.22.2 requires decimal ~> 1.5 or ~> 2.0 or ~> 3.0 and accepts 3.1).
      # 0.22.2 (not 0.22) so a lock regeneration can never resolve 0.22.0/0.22.1,
      # which carry CVE-2026-32687 (SQLi in Postgrex.Notifications.listen/3, fixed 0.22.2).
      {:postgrex, "~> 0.22.2"},
      # Dev/Test
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["rjpalermo"],
      files:
        ~w(lib .formatter.exs mix.exs README* LICENSE* CHANGELOG* NOTICE usage-rules.md CONTRIBUTING.md AGENTS.md),
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md",
        "Documentation" => "https://hexdocs.pm/replicant"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      source_url: @source_url,
      extras: [
        "README.md",
        "usage-rules.md",
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        "AGENTS.md",
        "LICENSE",
        "NOTICE"
      ],
      groups_for_extras: [
        Guides: ["usage-rules.md"],
        Project: ["CONTRIBUTING.md", "AGENTS.md", "LICENSE", "NOTICE"]
      ],
      groups_for_modules: [
        "Core API": [Replicant, Replicant.Sink, Replicant.Config],
        "Data structures": [
          Replicant.Transaction,
          Replicant.Change,
          Replicant.SchemaChange,
          Replicant.Error
        ],
        Observability: [Replicant.Telemetry],
        Runtime: [
          Replicant.Pipeline,
          Replicant.Supervisor,
          Replicant.Connection,
          Replicant.AssemblerServer,
          Replicant.Assembler,
          Replicant.CheckpointStore,
          Replicant.Snapshotter
        ],
        "Disk spill": [Replicant.Spill, Replicant.Spill.Reader, Replicant.Spill.Error],
        "Decoding (pgoutput)": [
          Replicant.Decoder,
          Replicant.Decoder.Messages,
          Replicant.Decoder.OidDatabase,
          Replicant.Casting.Types,
          Replicant.Casting.ArrayParser,
          Replicant.Identifier,
          Replicant.QueryBuilder
        ]
      ]
    ]
  end

  defp aliases do
    [
      quality: ["format --check-formatted", "credo --strict", "dialyzer"],
      audit: ["deps.unlock --check-unused", "hex.audit", "deps.audit"]
    ]
  end
end

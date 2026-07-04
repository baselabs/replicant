defmodule Replicant.MixProject do
  use Mix.Project

  @version "0.1.0"
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
        plt_add_apps: [:mix, :ex_unit, :decimal, :jason],
        plt_core_path: "priv/plts",
        plt_local_path: "priv/plts"
      ]
    ]
  end

  def cli do
    [preferred_envs: [credo: :test, dialyzer: :test]]
  end

  def application do
    # Plan 2 replaces this with mod: {Replicant.Application, []} to start the
    # DynamicSupervisor. Plan 1 ships no supervised processes.
    [extra_applications: [:logger]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # Runtime (Plan 1 consumes decimal + jason via casting; telemetry via the helper)
      {:decimal, "~> 3.1"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.0"},
      # Dev/Test
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false}
      # Plan 2 adds: {:postgrex, "~> 0.22"}  (NOT ~> 0.20 — 0.20 requires decimal
      # ~> 1.5/2.0 and conflicts with decimal ~> 3.1; 0.22 accepts decimal ~> 3.0)
    ]
  end

  defp package do
    [
      maintainers: ["rjpalermo"],
      files: ~w(lib .formatter.exs mix.exs README* LICENSE* CHANGELOG* NOTICE usage-rules.md),
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md"
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
        "LICENSE",
        "NOTICE"
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

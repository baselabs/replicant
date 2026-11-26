defmodule Replicant.MixProject do
  use Mix.Project

  @version "1.7.0"
  @source_url "https://github.com/baselabs/replicant"

  def project do
    [
      app: :replicant,
      version: @version,
      elixir: "~> 1.20.3",
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
          "with zero-loss delivery to a pluggable sink — exactly-once for transactional " <>
          "sinks, at-least-once (duplicate-bounded) for non-transactional sinks.",
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
      # ReplicationConnection substrate. Postgrex 0.23 is the floor for clearing
      # password/SSL options after connect and handling async messages during
      # replication startup. Its decimal requirement accepts our 3.x dependency.
      {:postgrex, "~> 0.23"},
      # Security floor: EEF-CVE-2026-133555 (GHSA-gmmq-fw75-wq46) affects every
      # db_connection below 2.10.3. Postgrex alone admits 2.10.2, and the checkpoint
      # store matches DBConnection errors directly, so a consumer must not be able
      # to keep a vulnerable release.
      {:db_connection, ">= 2.10.3 and < 3.0.0-0"},
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
        ~w(lib notebooks docs/INVARIANTS.md docs/ROADMAP.md docs/adr .formatter.exs mix.exs README* LICENSE* CHANGELOG* NOTICE usage-rules.md CONTRIBUTING.md examples/README.md examples/replication_pipeline/README.md),
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
      # The README is the front page on HexDocs: value proposition, quick start,
      # guarantees, compatibility. The `Replicant` module page carries the API.
      main: "readme",
      # Infra tags qualified Hex releases after registry read-back.
      source_ref: "baselabs_replicant-v#{@version}",
      source_url: @source_url,
      # No EPUB: it embeds the build time unless SOURCE_DATE_EPOCH is set.
      # Qualification retains HTML and Markdown for byte comparison at read-back.
      formatters: ["html", "markdown"],
      # Four extras are named README.md; without explicit filenames ex_doc emits
      # readme-1.html … readme-4.html and the front page (readme.html) does not exist.
      extras: [
        {"README.md", filename: "readme"},
        "docs/INVARIANTS.md",
        "docs/ROADMAP.md",
        {"docs/adr/README.md", filename: "adr-index"},
        "docs/adr/0001-logical-decoding-messages-delivery-guarantees.md",
        "docs/adr/0002-multi-publication-per-pipeline.md",
        "docs/adr/0003-value-free-error-boundary.md",
        "docs/adr/0004-commit-lsn-transaction-watermark.md",
        "docs/adr/0005-spill-is-ephemeral-scratch.md",
        "docs/adr/0006-fail-closed-supervision.md",
        "docs/adr/0007-actual-replication-session-identity.md",
        "docs/adr/0008-casting-lenient-value-preserving-fallback.md",
        "docs/adr/0009-pglogical-wal2json-decoders.md",
        "docs/adr/0010-postgresql-server-support.md",
        "notebooks/getting_started.livemd",
        {"examples/README.md", filename: "examples"},
        {"examples/replication_pipeline/README.md", filename: "reference-deployment"},
        "usage-rules.md",
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        "LICENSE",
        "NOTICE"
      ],
      groups_for_extras: [
        "Invariants & ADRs": [
          "docs/INVARIANTS.md",
          "docs/adr/README.md",
          "docs/adr/0001-logical-decoding-messages-delivery-guarantees.md",
          "docs/adr/0002-multi-publication-per-pipeline.md",
          "docs/adr/0003-value-free-error-boundary.md",
          "docs/adr/0004-commit-lsn-transaction-watermark.md",
          "docs/adr/0005-spill-is-ephemeral-scratch.md",
          "docs/adr/0006-fail-closed-supervision.md",
          "docs/adr/0007-actual-replication-session-identity.md",
          "docs/adr/0008-casting-lenient-value-preserving-fallback.md",
          "docs/adr/0009-pglogical-wal2json-decoders.md",
          "docs/adr/0010-postgresql-server-support.md"
        ],
        Guides: [
          "notebooks/getting_started.livemd",
          "examples/README.md",
          "examples/replication_pipeline/README.md",
          "usage-rules.md"
        ],
        Project: ["CONTRIBUTING.md", "LICENSE", "NOTICE", "docs/ROADMAP.md"]
      ],
      groups_for_modules: [
        "Core API": [Replicant, Replicant.Sink, Replicant.Config],
        "Data structures": [
          Replicant.Transaction,
          Replicant.Change,
          Replicant.SchemaChange,
          Replicant.Error,
          Replicant.SnapshotProgress,
          Replicant.SessionIdentity
        ],
        Observability: [Replicant.Telemetry],
        Runtime: [
          Replicant.Pipeline,
          Replicant.Supervisor,
          Replicant.Connection,
          Replicant.AssemblerServer,
          Replicant.Assembler,
          Replicant.CheckpointStore
        ],
        Snapshots: [
          Replicant.Snapshotter,
          Replicant.Snapshotter.Incremental,
          Replicant.SnapshotWindow
        ],
        "Disk spill": [Replicant.Spill, Replicant.Spill.Reader, Replicant.Spill.Error],
        Decoders: [
          Replicant.Decoder,
          Replicant.Decoder.Plugin,
          Replicant.Decoder.PgOutput,
          Replicant.Decoder.Pglogical,
          Replicant.Decoder.Wal2json,
          Replicant.Decoder.Messages,
          Replicant.Decoder.OidDatabase
        ],
        "Casting and SQL": [
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

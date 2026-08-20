defmodule Replicant.MixProject do
  use Mix.Project

  @version "1.2.1"
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
      # Live streaming (Plan 2): the ReplicationConnection substrate. Floor 0.22.4
      # (NOT ~> 0.20 — 0.20 requires decimal ~> 1.5/2.0 and conflicts with decimal
      # ~> 3.1; the 0.22.x line requires decimal ~> 1.5 or ~> 2.0 or ~> 3.0 and accepts 3.1).
      # 0.22.4 (not 0.22) so a lock regeneration can never resolve an earlier 0.22.x:
      #   - 0.22.2 floors CVE-2026-32687 (SQLi in Postgrex.Notifications.listen/3)
      #   - 0.22.3 fixes  CVE-2026-58225 (dollar-quote in Notifications reconnect replay)
      #   - 0.22.4 fixes  CVE-2026-66838 (SQLi via the :comment option in Postgrex.stream/4)
      # No ReplicationConnection API change across 0.22.2 → 0.22.4 (security patches only).
      {:postgrex, "~> 0.22.4"},
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
        ~w(lib notebooks docs/INVARIANTS.md docs/ROADMAP.md docs/adr .formatter.exs mix.exs README* LICENSE* CHANGELOG* NOTICE usage-rules.md CONTRIBUTING.md),
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
      main: "Replicant",
      # HexDocs source links resolve against the exact v#{@version} tag. Release
      # publication uses `git push origin v#{@version}` for only that tag, then uploads
      # the witnessed bytes through scripts/release/upload_candidate.exs; never rebuild
      # them at publish time.
      source_ref: "v#{@version}",
      source_url: @source_url,
      extras: [
        "README.md",
        "docs/INVARIANTS.md",
        "docs/ROADMAP.md",
        "docs/adr/README.md",
        "docs/adr/0001-logical-decoding-messages-delivery-guarantees.md",
        "docs/adr/0002-multi-publication-per-pipeline.md",
        "docs/adr/0003-value-free-error-boundary.md",
        "docs/adr/0004-commit-lsn-transaction-watermark.md",
        "docs/adr/0005-spill-is-ephemeral-scratch.md",
        "docs/adr/0006-fail-closed-supervision.md",
        "docs/adr/0007-actual-replication-session-identity.md",
        "notebooks/getting_started.livemd",
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
          "docs/adr/0007-actual-replication-session-identity.md"
        ],
        Guides: ["notebooks/getting_started.livemd", "usage-rules.md"],
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

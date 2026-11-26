defmodule Replicant.ReleaseContractTest do
  # Release identity is a public contract: the package version, the CHANGELOG's cut
  # release section, and the doc source-ref tag must agree, and the release must
  # supersede the preceding version. Drift here is quiet until publication or a
  # downstream resolution, so it is gated in the cold suite (no live substrate needed).
  use ExUnit.Case, async: true

  @changelog Path.expand("../../CHANGELOG.md", __DIR__)
  @mixfile Path.expand("../../mix.exs", __DIR__)
  @declaration Path.expand("../../.github/hex-packages.json", __DIR__)
  @published_digests Path.expand("../../scripts/release/published_packages.sha256", __DIR__)
  # The preceding release. Bumped as part of cutting each release; a version that fails
  # to advance past it reds here rather than re-minting an already-published version.
  @previous_release "1.6.0"
  # Immutable registry digest for the preceding release, recorded after read-back.
  # Held versions are compared with freshly built bytes before upload; they do not
  # rewrite this ledger.
  @published_digest "0b5f2e188d8ed7713c0832ff06a4a22131d63cdcd69f49ed37b0a7487efcdef6"

  defp version, do: Mix.Project.config()[:version]

  test "the db_connection security floor and startup lag fix require the patch release" do
    assert version() == "1.6.1"
  end

  test "snapshot release documents the halt, token contents, and conservative plugin policy" do
    root = Path.expand("../..", __DIR__)
    usage = File.read!(Path.join(root, "usage-rules.md"))
    assert usage =~ "with `:snapshot_no_publishable_columns` |"
    assert File.read!(@changelog) =~ "[:replicant, :pipeline, :halted]"

    assert File.read!(Path.join(root, "lib/replicant/telemetry.ex")) =~
             "[:replicant, :pipeline, :halted]"

    assert File.read!(Path.join(root, "docs/ROADMAP.md")) =~ "31 structure-only events"
    assert File.read!(Path.join(root, "docs/ROADMAP.md")) =~ "October 10, 2026 correction"

    for path <- ["README.md", "usage-rules.md", "lib/replicant/snapshot_progress.ex"] do
      assert File.read!(Path.join(root, path)) =~ "publication configuration"
    end

    adr = File.read!(Path.join(root, "docs/adr/0009-pglogical-wal2json-decoders.md"))
    assert adr =~ "unions `set_att_list`"
    assert adr =~ "conservative"
  end

  test "the PostgreSQL support ADR ships in Hex and ExDoc" do
    adr = "docs/adr/0010-postgresql-server-support.md"
    manifest = File.read!(@declaration) |> JSON.decode!()
    docs = Mix.Project.config()[:docs]

    assert adr in manifest["replicant"]["files"]
    assert adr in docs[:extras]
    assert Enum.any?(docs[:groups_for_extras], fn {_group, pages} -> adr in pages end)
  end

  test "package version advances past the preceding release" do
    assert Version.compare(version(), @previous_release) == :gt,
           "mix.exs version #{version()} must be strictly greater than the preceding " <>
             "release #{@previous_release}; a re-minted published version is a hard stop"
  end

  test "CHANGELOG cuts a dated section for the package version as the newest release" do
    body = File.read!(@changelog)
    v = version()

    assert body =~ ~r/^## \[#{Regex.escape(v)}\] - \d{4}-\d{2}-\d{2}$/m,
           "CHANGELOG.md has no dated `## [#{v}] - YYYY-MM-DD` release section"

    released =
      Regex.scan(~r/^## \[(\d+\.\d+\.\d+)\]/m, body)
      |> Enum.map(fn [_, ver] -> ver end)

    assert List.first(released) == v,
           "newest released CHANGELOG section is #{inspect(List.first(released))}, expected #{v}"

    assert Version.compare(List.first(released), Enum.at(released, 1)) == :gt,
           "the candidate section must be newer than the section beneath it"
  end

  test "current release changelog has one group for each change type" do
    [_, after_release] =
      File.read!(@changelog) |> String.split("## [#{version()}]", parts: 2)

    [release | _] = String.split(after_release, ~r/^## \[/m)

    headings = Regex.scan(~r/^### (.+)$/m, release) |> Enum.map(fn [_, heading] -> heading end)

    assert headings == Enum.uniq(headings),
           "the current release changelog repeats a change-type heading: #{inspect(headings)}"
  end

  test "CHANGELOG comparison links bind the release to the preceding tag" do
    body = File.read!(@changelog)
    v = version()

    assert body =~
             ~r{^\[#{Regex.escape(v)}\]: https://github.com/baselabs/replicant/compare/v#{Regex.escape(@previous_release)}\.\.\.v#{Regex.escape(v)}$}m,
           "missing/incorrect `[#{v}]:` comparison link to v#{@previous_release}...v#{v}"

    assert body =~
             ~r{^\[Unreleased\]: https://github.com/baselabs/replicant/compare/v#{Regex.escape(v)}\.\.\.HEAD$}m,
           "the [Unreleased] link must compare from the freshly cut v#{v} tag"
  end

  test "docs source_ref uses the scoped Hex release tag" do
    assert Mix.Project.config()[:docs][:source_ref] == "baselabs_replicant-v#{version()}"
  end

  test "shipped release guidance preserves the inspected package bytes" do
    body = File.read!(@mixfile)

    refute body =~ "mix hex.publish"
    assert body =~ "after registry read-back"
    assert Mix.Project.config()[:docs][:formatters] == ["html", "markdown"]
  end

  test "workflows never restore or save Actions caches" do
    workflows = Path.wildcard(Path.expand("../../.github/workflows/*.{yml,yaml}", __DIR__))
    assert workflows != []

    for path <- workflows do
      refute File.read!(path) =~ ~r{actions/cache(?:/|@)}i,
             "#{Path.basename(path)} must not restore or save Actions caches"
    end
  end

  test "published package digest manifest binds the preceding release bytes" do
    manifest = File.read!(@published_digests)

    assert manifest =~
             "#{@published_digest}  replicant-#{@previous_release}.tar"
  end
end

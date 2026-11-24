defmodule Replicant.ReleaseContractTest do
  # Release identity is a public contract: the package version, the CHANGELOG's cut
  # release section, and the doc source-ref tag must agree, and the release must
  # supersede the preceding version. Drift here is quiet until publication or a
  # downstream resolution, so it is gated in the cold suite (no live substrate needed).
  use ExUnit.Case, async: true

  @changelog Path.expand("../../CHANGELOG.md", __DIR__)
  @mixfile Path.expand("../../mix.exs", __DIR__)
  @workflow Path.expand("../../.github/workflows/qualification.yml", __DIR__)
  @builder Path.expand("../../scripts/release/build_candidate.sh", __DIR__)
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

  test "docs source_ref pins the current release tag" do
    assert Mix.Project.config()[:docs][:source_ref] == "v#{version()}",
           "docs source_ref must be v#{version()} so HexDocs source links resolve to the release tag"
  end

  test "shipped release guidance preserves the inspected package bytes" do
    body = File.read!(@mixfile)

    refute body =~ "mix hex.publish"
    assert body =~ "scripts/release/publish_hex_artifacts.exs"
    assert body =~ ".github/workflows/publish.yml"
    assert body =~ "after Hex read-back"
  end

  test "workflows never restore or save Actions caches" do
    workflows = Path.wildcard(Path.expand("../../.github/workflows/*.{yml,yaml}", __DIR__))
    assert workflows != []

    for path <- workflows do
      refute File.read!(path) =~ ~r{actions/cache(?:/|@)}i,
             "#{Path.basename(path)} must not restore or save Actions caches"
    end
  end

  test "post-release package checks keep the published identity and clean main guards" do
    workflow = File.read!(@workflow)
    builder = File.read!(@builder)
    [_, release_job] = String.split(workflow, "  release-artifact:", parts: 2)

    assert release_job =~ "fetch-depth: 0"
    assert builder =~ "Replicant.PackageIdentity.verify_publish!"
    assert builder =~ "Replicant.PackageIdentity.verify_build!"
  end

  test "published package digest manifest binds the preceding release bytes" do
    manifest = File.read!(@published_digests)

    assert manifest =~
             "#{@published_digest}  replicant-#{@previous_release}.tar"
  end

  test "candidate accepts only check mode or one output directory" do
    [_, parser] = Regex.run(~r/(usage\(\).*?)\nlog\(\)/s, File.read!(@builder))
    script = parser <> ~S(printf '%s\n%s\n' "$mode" "$output_dir")

    for {args, expected} <- [
          {["--check"], "check\n--check\n"},
          {["output path"], "publish\noutput path\n"}
        ] do
      assert {expected, 0} == candidate_arguments(script, args)
    end

    for args <- [[], [""], ["--unknown"], ["--check", "other"], ["one", "two"]] do
      {output, status} = candidate_arguments(parser, args)
      assert status == 2
      assert output =~ "usage: build_candidate.sh --check | <output-directory>"
    end
  end

  test "candidate builds and inspects the same two paths the workflow uploads" do
    source = File.read!(@builder)
    assert source =~ ~s(package="$output_dir/package.tar")
    assert source =~ ~s(cmp -s "$package" "$rebuild_tree/package.tar")
    assert source =~ ~s(verify_package.sh" "$package")
    assert source =~ ~s(consume_candidate.sh" "$package" "$output_dir/docs.tar.gz")
    assert source =~ ~s(chmod a-w "$package" "$output_dir/docs.tar.gz")
    refute source =~ ~r/authorization|witness|receipt/
  end

  test "qualification and publication inspect one docs render and preserve package equality" do
    source = File.read!(@builder)
    assert source =~ ~s|consume_candidate.sh" "$package" "$output_dir/docs.tar.gz"|
    assert source =~ ~s|cmp -s "$package" "$rebuild_tree/package.tar"|
    assert length(Regex.scan(~r/consume_candidate\.sh/, source)) == 1
    refute source =~ ~r/cmp[^\n]*docs|docs build is not byte-reproducible/
    assert Mix.Project.config()[:docs][:formatters] == ["html", "markdown"]
    assert File.read!(@workflow) =~ "bash scripts/release/build_candidate.sh --check"
  end

  test "publication compares the inspected package with held Hex bytes before the keyed step" do
    source = File.read!(@builder)
    assert_held_check!(source)

    for {before, replacement} <- [
          {"Replicant.PackageIdentity.verify_publish_archive!(version, commit, archive)", ":ok"},
          {~s|[version, commit, archive] = System.argv()|, ":ok"},
          {~s|' -- "$version" "$commit" "$package"|, ~s|' -- "$version" "$commit" other.tar|}
        ] do
      mutated = String.replace(source, before, replacement)
      refute mutated == source
      assert_raise ExUnit.AssertionError, fn -> assert_held_check!(mutated) end
    end
  end

  defp assert_held_check!(source) do
    [_build, after_inspection] = String.split(source, "consume_candidate.sh\"", parts: 2)
    [checked, _retained] = String.split(after_inspection, "chmod a-w", parts: 2)
    assert checked =~ ~s|if [[ "$mode" == "publish" ]]; then|
    call = "Replicant.PackageIdentity.verify_publish_archive!(version, commit, archive)"
    assert checked =~ call
    assert checked =~ "[version, commit, archive] = System.argv()"
    assert checked =~ ~s|' -- "$version" "$commit" "$package"|
  end

  defp candidate_arguments(script, args) do
    System.cmd(
      "bash",
      ["--noprofile", "--norc", "-euo", "pipefail", "-c", script, "builder" | args],
      stderr_to_stdout: true
    )
  end
end

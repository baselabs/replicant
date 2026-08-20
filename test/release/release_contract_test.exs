defmodule Replicant.ReleaseContractTest do
  # Release identity is a public contract: the package version, the CHANGELOG's cut
  # release section, and the doc source-ref tag must agree, and the release must
  # supersede the preceding version. Drift here is quiet until publication or a
  # downstream resolution, so it is gated in the cold suite (no live substrate needed).
  use ExUnit.Case, async: true

  @changelog Path.expand("../../CHANGELOG.md", __DIR__)
  @mixfile Path.expand("../../mix.exs", __DIR__)
  @workflow Path.expand("../../.github/workflows/ci.yml", __DIR__)
  @builder Path.expand("../../scripts/release/build_candidate.sh", __DIR__)
  @uploader Path.expand("../../scripts/release/upload_candidate.exs", __DIR__)
  @published_digests Path.expand("../../scripts/release/published_packages.sha256", __DIR__)
  @published_digest "14b37f92a5ea54f43a756ee492c41beceae874cd244470d331fa82af64d46285"

  # The preceding release. Bumped as part of cutting each release; a version that fails
  # to advance past it reds here rather than re-minting an already-published version.
  @previous_release "1.2.0"

  defp version, do: Mix.Project.config()[:version]

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

  test "docs source_ref pins the candidate version tag" do
    assert Mix.Project.config()[:docs][:source_ref] == "v#{version()}",
           "docs source_ref must be v#{version()} so HexDocs source links resolve to the release tag"
  end

  test "shipped release guidance preserves the witnessed package bytes" do
    body = File.read!(@mixfile)

    refute body =~ "mix hex.publish"
    assert body =~ "scripts/release/upload_candidate.exs"
    assert body =~ ~S(git push origin v#{@version})
  end

  test "post-release package checks fetch tags without weakening mint identity" do
    workflow = File.read!(@workflow)
    builder = File.read!(@builder)
    uploader = File.read!(@uploader)
    [_, release_job] = String.split(workflow, "  release-artifact:", parts: 2)

    assert release_job =~ "fetch-depth: 0"
    assert builder =~ "--published-check"
    assert uploader =~ "Replicant.PackageIdentity.check_candidate(version)"

    assert uploader =~
             "Replicant.PackageIdentity.check_build(version, source_commit, published_digest)"
  end

  test "published package digest manifest binds this release" do
    manifest = File.read!(@published_digests)

    assert manifest =~ "#{@published_digest}  replicant-#{version()}.tar"
  end

  test "publish authorization tests never write retained package evidence" do
    source = File.read!(Path.expand("publish_candidate_test.exs", __DIR__))

    refute source =~
             ~r/Path\.join\(\[@repo_root,\s*"\.kimosabe",\s*"artifacts"/
  end
end

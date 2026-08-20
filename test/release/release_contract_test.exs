defmodule Replicant.ReleaseContractTest do
  # Release identity is a public contract: the package version, the CHANGELOG's cut
  # release section, and the doc source-ref tag must agree, and the candidate must
  # supersede the last published version. Drift here is quiet until publication or a
  # downstream resolution, so it is gated in the cold suite (no live substrate needed).
  use ExUnit.Case, async: true

  @changelog Path.expand("../../CHANGELOG.md", __DIR__)

  # The last version published to Hex that this candidate supersedes. Bumped as part of
  # cutting each release; a candidate that fails to advance past it reds here rather than
  # re-minting an already-published version.
  @last_published "1.1.0"

  defp version, do: Mix.Project.config()[:version]

  test "candidate version advances past the last published release" do
    assert Version.compare(version(), @last_published) == :gt,
           "mix.exs version #{version()} must be strictly greater than the last published " <>
             "#{@last_published}; a re-minted published version is a hard stop"
  end

  test "CHANGELOG cuts a dated release section for the candidate as the newest release" do
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

  test "CHANGELOG comparison links bind the candidate to the last published tag" do
    body = File.read!(@changelog)
    v = version()

    assert body =~
             ~r{^\[#{Regex.escape(v)}\]: https://github.com/baselabs/replicant/compare/v#{Regex.escape(@last_published)}\.\.\.v#{Regex.escape(v)}$}m,
           "missing/incorrect `[#{v}]:` comparison link to v#{@last_published}...v#{v}"

    assert body =~
             ~r{^\[Unreleased\]: https://github.com/baselabs/replicant/compare/v#{Regex.escape(v)}\.\.\.HEAD$}m,
           "the [Unreleased] link must compare from the freshly cut v#{v} tag"
  end

  test "docs source_ref pins the candidate version tag" do
    assert Mix.Project.config()[:docs][:source_ref] == "v#{version()}",
           "docs source_ref must be v#{version()} so HexDocs source links resolve to the release tag"
  end

  test "exact-byte uploader uses Hex's authenticated API wrapper and never the raw client" do
    body = File.read!(Path.expand("../../scripts/release/upload_candidate.exs", __DIR__))
    builder = File.read!(Path.expand("../../scripts/release/build_candidate.sh", __DIR__))

    assert body =~ "Hex.API.Release.publish"
    refute body =~ ":mix_hex_api_release.publish"
    refute builder =~ ~r/upload_candidate\.exs" --\s/
  end
end

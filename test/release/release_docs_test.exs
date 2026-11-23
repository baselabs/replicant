Mix.Local.append_archives()
for api <- [:mix_hex_tarball, Hex.Repo, Hex.API.Release], do: Code.ensure_loaded!(api)
Code.require_file("../../scripts/release/release_docs.exs", __DIR__)
Code.require_file("../../scripts/release/release_notes.exs", __DIR__)

source = File.read!(Path.expand("../../scripts/release/retain_release_assets.exs", __DIR__))
{:__block__, _, forms} = Code.string_to_quoted!(source)
Code.compile_quoted(Enum.find(forms, &match?({:defmodule, _, _}, &1)))

defmodule Replicant.ReleaseDocsTest do
  use ExUnit.Case, async: true

  alias Replicant.{ReleaseAssets, ReleaseDocs, ReleaseNotes}

  @version "1.6.0"
  @commit String.duplicate("c", 40)
  @section "## [#{@version}] - 2026-11-22\n\n### Fixed\n\n- Notes from the served package."

  setup do
    directory =
      Path.join(System.tmp_dir!(), "replicant-provenance-#{System.unique_integer([:positive])}")

    File.mkdir!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)
    %{directory: directory}
  end

  test "read-back validates exact provenance fields in real archives" do
    assert @commit == ReleaseDocs.provenance!(archive(metadata()), @version)

    for fields <- [
          Map.put(metadata(), "package", "other"),
          Map.put(metadata(), "version", "0.0.0"),
          Map.put(metadata(), "commit", "HEAD"),
          Map.put(metadata(), "commit", String.duplicate("a", 39)),
          Map.put(metadata(), "commit", nil),
          Map.put(metadata(), "extra", true),
          Map.delete(metadata(), "commit"),
          [],
          nil
        ] do
      assert_raise RuntimeError, ~r/release.json is invalid/, fn ->
        ReleaseDocs.provenance!(archive(fields), @version)
      end
    end

    {:ok, invalid_json} =
      :mix_hex_tarball.create_docs([
        {~c"index.html", "index"},
        {~c"release.json", "private-response-value"}
      ])

    error = assert_raise RuntimeError, fn -> ReleaseDocs.provenance!(invalid_json, @version) end
    refute Exception.message(error) =~ "private-response-value"

    {:ok, no_index} = :mix_hex_tarball.create_docs([{~c"release.json", "{}"}])
    assert_raise RuntimeError, ~r/archive is malformed/, fn -> ReleaseDocs.files!(no_index) end
  end

  test "missing provenance is accepted in planning and refused at read-back" do
    {:ok, legacy} = :mix_hex_tarball.create_docs([{~c"index.html", "legacy"}])
    assert [{~c"index.html", "legacy"}] == ReleaseDocs.files!(legacy)

    assert_raise RuntimeError, ~r/no release.json/, fn ->
      ReleaseDocs.provenance!(legacy, @version)
    end
  end

  test "real git ancestry accepts the dispatched commit and its ancestors", %{directory: dir} do
    {ancestor, dispatched} = history!(dir)
    assert :ok == ReleaseAssets.verify_history!(dispatched, dispatched, dir)
    assert :ok == ReleaseAssets.verify_history!(ancestor, dispatched, dir)

    # An unrelated real commit is valid Git data but is not on the default history.
    tree = git!(dir, ["rev-parse", "HEAD^{tree}"])
    unrelated = git!(dir, ["commit-tree", tree, "-m", "unrelated"])

    for {published, source} <- [
          {unrelated, dispatched},
          {dispatched, ancestor},
          {unrelated, unrelated},
          {String.duplicate("0", 40), dispatched},
          {"HEAD", dispatched}
        ] do
      assert_raise RuntimeError, ~r/default-branch history/, fn ->
        ReleaseAssets.verify_history!(published, source, dir)
      end
    end

    git!(dir, ["update-ref", "-d", "refs/remotes/origin/main"])

    assert_raise RuntimeError, ~r/default-branch history/, fn ->
      ReleaseAssets.verify_history!(ancestor, dispatched, dir)
    end
  end

  test "missing and shallow history refuse read-back", %{directory: dir} do
    repository = Path.join(dir, "source")
    File.mkdir!(repository)
    {_ancestor, dispatched} = history!(repository)
    clone = Path.join(dir, "shallow")
    git!(dir, ["clone", "--no-local", "--depth", "1", repository, clone])

    for path <- [clone, Path.join(dir, "missing")] do
      assert_raise RuntimeError, ~r/default-branch history/, fn ->
        ReleaseAssets.verify_history!(dispatched, dispatched, path)
      end
    end
  end

  test "retention writes served bytes and their commit after ancestry passes", %{directory: dir} do
    {ancestor, dispatched} = history!(dir)
    docs = archive(Map.put(metadata(), "commit", ancestor))
    output = Path.join(dir, "release-assets")
    package = package!(dir, @section <> "\n\n## [1.5.1]\n\n- Older notes.\n")
    File.write!(Path.join(dir, "CHANGELOG.md"), "## [#{@version}]\n\n- Workspace-only notes.\n")

    assert ancestor == ReleaseAssets.retain!(package, docs, @version, output, dispatched, dir)
    assert File.read!(Path.join(output, "replicant-#{@version}.tar")) == package
    assert File.read!(Path.join(output, "replicant-#{@version}-docs.tar.gz")) == docs
    assert File.read!(Path.join(output, "published_commit")) == ancestor <> "\n"

    checksums = File.read!(Path.join(output, "SHA256SUMS"))

    for {bytes, name} <- [
          {package, "replicant-#{@version}.tar"},
          {docs, "replicant-#{@version}-docs.tar.gz"}
        ] do
      digest = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
      assert checksums =~ "#{digest}  #{name}\n"
    end

    notes = File.read!(Path.join(output, "RELEASE-NOTES.md"))
    package_digest = Base.encode16(:crypto.hash(:sha256, package), case: :lower)
    docs_digest = Base.encode16(:crypto.hash(:sha256, docs), case: :lower)

    assert notes ==
             "#{@section}\n\nCommit: #{ancestor}\n\n" <>
               "Package SHA-256: #{package_digest}\n\nDocs SHA-256: #{docs_digest}\n"

    refute notes =~ dispatched
    refute notes =~ "Older notes"
    refute notes =~ "Workspace-only notes"

    refused = Path.join(dir, "refused-assets")
    invalid = archive(Map.put(metadata(), "commit", String.duplicate("0", 40)))

    assert_raise RuntimeError, fn ->
      ReleaseAssets.retain!(package, invalid, @version, refused, dispatched, dir)
    end

    refute File.exists?(refused)
  end

  test "pre-build and read-back reject invalid version sections", %{directory: dir} do
    {ancestor, dispatched} = history!(dir)
    docs = archive(Map.put(metadata(), "commit", ancestor))
    mixfile = Path.join(dir, "mix.exs")
    changelog = Path.join(dir, "CHANGELOG.md")
    File.write!(mixfile, "  @version \"#{@version}\"\n")
    File.write!(changelog, @section)
    assert ReleaseNotes.verify_changelog!(mixfile, changelog) == @section

    for invalid <- [
          "## [1.5.1]\n\n- Other release.\n",
          @section <> "\n\n" <> @section,
          @section <> "\n\n## [#{@version}]",
          "## [#{@version}]",
          "## [#{@version}]\n\n",
          "## [#{@version}]\n\n### Fixed\n\n### Changed\n\n",
          "## [#{@version}]\n\n### Fixed\n\n## [1.5.1]\n\n- Older notes.\n"
        ] do
      File.write!(changelog, invalid)

      assert_raise RuntimeError, ~r/missing, duplicated or empty/, fn ->
        ReleaseNotes.verify_changelog!(mixfile, changelog)
      end

      package = package!(dir, invalid)
      output = Path.join(dir, "refused-assets")
      File.write!(changelog, @section)

      assert_raise RuntimeError, ~r/missing, duplicated or empty/, fn ->
        ReleaseAssets.retain!(package, docs, @version, output, dispatched, dir)
      end

      refute File.exists?(output)
    end
  end

  test "pre-build refuses an absent or ambiguous Mix project version", %{directory: dir} do
    mixfile = Path.join(dir, "mix.exs")
    changelog = Path.join(dir, "CHANGELOG.md")
    File.write!(changelog, @section)

    for contents <- ["", "  @version \"#{@version}\"\n  @version \"1.5.1\"\n"] do
      File.write!(mixfile, contents)

      assert_raise RuntimeError, ~r/exactly one Mix project version/, fn ->
        ReleaseNotes.verify_changelog!(mixfile, changelog)
      end
    end
  end

  test "release notes reject malformed or incomplete packages", %{directory: dir} do
    missing_contents = tar!(dir, "package.tar", [])
    missing_changelog = package!(dir, nil)

    duplicate_contents =
      tar!(dir, "package.tar", [
        {~c"contents.tar.gz", "invalid"},
        {~c"contents.tar.gz", "invalid"}
      ])

    contents =
      tar!(
        dir,
        "contents.tar.gz",
        [{~c"CHANGELOG.md", @section}, {~c"CHANGELOG.md", @section}],
        [:compressed]
      )

    duplicate_changelog = tar!(dir, "package.tar", [{~c"contents.tar.gz", contents}])

    for bytes <- [
          "private-response-value",
          missing_contents,
          missing_changelog,
          duplicate_contents,
          duplicate_changelog
        ] do
      error =
        assert_raise RuntimeError, ~r/Hex package changelog is missing or malformed/, fn ->
          ReleaseNotes.package_section!(bytes, @version)
        end

      refute Exception.message(error) =~ "private-response-value"
    end
  end

  defp package!(directory, changelog) do
    files = if is_nil(changelog), do: [], else: [{~c"CHANGELOG.md", changelog}]
    contents = tar!(directory, "contents.tar.gz", files, [:compressed])
    tar!(directory, "package.tar", [{~c"contents.tar.gz", contents}])
  end

  defp tar!(directory, name, files, options \\ []) do
    path = Path.join(directory, name)
    :ok = :erl_tar.create(String.to_charlist(path), files, options)
    File.read!(path)
  end

  defp metadata, do: %{"package" => "replicant", "version" => @version, "commit" => @commit}

  defp archive(fields) do
    json = fields |> :json.encode() |> IO.iodata_to_binary()

    {:ok, bytes} =
      :mix_hex_tarball.create_docs([
        {~c"index.html", "index"},
        {~c"release.json", json}
      ])

    bytes
  end

  defp history!(directory) do
    git!(directory, ["init", "--initial-branch=main"])
    git!(directory, ["commit", "--allow-empty", "-m", "ancestor"])
    ancestor = git!(directory, ["rev-parse", "HEAD"])
    git!(directory, ["commit", "--allow-empty", "-m", "dispatched"])
    dispatched = git!(directory, ["rev-parse", "HEAD"])
    git!(directory, ["update-ref", "refs/remotes/origin/main", dispatched])
    {ancestor, dispatched}
  end

  defp git!(directory, args) do
    identity = ["-c", "user.name=Release Test", "-c", "user.email=release-test@example.invalid"]
    {output, 0} = System.cmd("git", ["-C", directory] ++ identity ++ args, stderr_to_stdout: true)
    String.trim(output)
  end
end

# Exercise the keyed uploader's read decisions against live Hex, without a key.
# Loads the real uploader module and never runs its publication entry point.
if Mix.Project.get(), do: raise("upload preconditions must run without a Mix project")

path = Path.join(__DIR__, "publish_hex_artifacts.exs")
Code.require_file("release_docs.exs", __DIR__)
Code.require_file("release_notes.exs", __DIR__)
{:ok, {:__block__, _, forms}} = Code.string_to_quoted(File.read!(path))
module = Enum.find(forms, &match?({:defmodule, _, _}, &1))
Code.compile_quoted(module, path)
Hex.start()

alias Replicant.{PublishHexArtifacts, ReleaseNotes}

defmodule PublishHexArtifactsSelfTest do
  def reject(expected, function) do
    function.()
    raise "upload preconditions accepted a forbidden release"
  rescue
    error in RuntimeError ->
      unless error.message == expected, do: reraise(error, __STACKTRACE__)
  end
end

# The most recent published release, independent of the candidate version.
# Its immutable digest and served docs provenance are recorded after infra read-back.
version = "1.6.0"
artifact = "replicant-#{version}.tar"

recorded =
  Path.join(__DIR__, "published_packages.sha256")
  |> File.read!()
  |> String.split("\n")
  |> Enum.find_value(fn line ->
    case String.split(line) do
      [digest, ^artifact] -> digest
      _other -> nil
    end
  end) || raise("no recorded digest for replicant #{version}")

:ok = PublishHexArtifacts.existing_package!(Hex.API.Package.get_anonymous(nil, "replicant"))

# A dot cannot occur in a Hex package name, so this real lookup cannot name a package.
PublishHexArtifactsSelfTest.reject(
  "the name replicant is not on Hex; a first publication is the owner's decision",
  fn ->
    PublishHexArtifacts.existing_package!(
      Hex.API.Package.get_anonymous(nil, "replicant.invalid")
    )
  end
)

absent = "999999.0.0"

:fresh =
  PublishHexArtifacts.release_state!(
    Hex.API.Release.get(nil, "replicant", absent),
    absent,
    String.duplicate("0", 64)
  )

release = Hex.API.Release.get(nil, "replicant", version)
{:held, true} = PublishHexArtifacts.release_state!(release, version, recorded)
:ok = PublishHexArtifacts.read_back!(release, version, recorded)

PublishHexArtifactsSelfTest.reject(
  "Hex release identity does not match the built package; publication refused",
  fn -> PublishHexArtifacts.release_state!(release, version, String.duplicate("0", 64)) end
)

tarball = Hex.Repo.get_tarball("hexpm", "replicant", version)
:ok = PublishHexArtifacts.package_read_back!(tarball, recorded)

PublishHexArtifactsSelfTest.reject(
  "Hex package archive checksum differs from the built package; publication refused",
  fn -> PublishHexArtifacts.package_read_back!(tarball, String.duplicate("0", 64)) end
)

docs = Hex.Repo.get_docs("hexpm", "replicant", version)
:skip = PublishHexArtifacts.docs_action!(docs, true)
"8d313d6b06503098e1e5c58186f19e6e5501efed" =
  PublishHexArtifacts.docs_read_back!(docs, version)

{:ok, {200, _headers, package}} = tarball
section = ReleaseNotes.package_section!(package, version)

for changelog <- [
      "## [0.0.0]\n\n- Other release.\n",
      section <> "\n\n" <> section,
      "## [#{version}]\n\n### Fixed\n\n"
    ] do
  PublishHexArtifactsSelfTest.reject(
    "the changelog section for #{version} is missing, duplicated or empty; publication refused",
    fn -> ReleaseNotes.section!(changelog, version) end
  )
end

# Legacy served docs are kept during planning, even without provenance. Read-back
# refuses them: package equality cannot establish which commit rendered the docs.
legacy_version = "1.5.1"
legacy_docs = Hex.Repo.get_docs("hexpm", "replicant", legacy_version)
:skip = PublishHexArtifacts.docs_action!(legacy_docs, true)

PublishHexArtifactsSelfTest.reject(
  "Hex docs archive has no release.json; publication refused",
  fn -> PublishHexArtifacts.docs_read_back!(legacy_docs, legacy_version) end
)

# Absent docs must read as the authoritative 404 that permits the only upload.
missing = Hex.Repo.get_docs("hexpm", "replicant", absent)
:upload = PublishHexArtifacts.docs_action!(missing, false)

PublishHexArtifactsSelfTest.reject(
  "Hex docs metadata and repository disagree; inspect public state before retrying",
  fn -> PublishHexArtifacts.docs_action!(missing, true) end
)

PublishHexArtifactsSelfTest.reject(
  "Hex docs archive is unavailable; inspect public state before retrying",
  fn -> PublishHexArtifacts.docs_read_back!(missing, absent) end
)

IO.puts("upload preconditions: live Hex self-tests PASS (no upload or key)")

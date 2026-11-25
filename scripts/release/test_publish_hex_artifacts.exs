# Exercise the isolated uploader's read decisions against live Hex, without a key.
# Loads the real uploader module and never runs its publication entry point.
if Mix.Project.get(), do: raise("upload preconditions must run without a Mix project")

path = Path.join(__DIR__, "publish_hex_artifacts.exs")
Code.require_file("release_docs.exs", __DIR__)
{:ok, {:__block__, _, forms}} = Code.string_to_quoted(File.read!(path))
module = Enum.find(forms, &match?({:defmodule, _, _}, &1))
Code.compile_quoted(module, path)
Hex.start()

alias Replicant.{PublishHexArtifacts, ReleaseDocs}

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

PublishHexArtifactsSelfTest.reject(
  "Hex release identity does not match the built package; publication refused",
  fn -> PublishHexArtifacts.release_state!(release, version, String.duplicate("0", 64)) end
)

{:ok, {200, _headers, package}} = Hex.Repo.get_tarball("hexpm", "replicant", version)
^recorded = :crypto.hash(:sha256, package) |> Base.encode16(case: :lower)

{:ok, {200, _headers, docs_bytes}} = docs = Hex.Repo.get_docs("hexpm", "replicant", version)
:skip = PublishHexArtifacts.docs_action!(docs, true)
"8d313d6b06503098e1e5c58186f19e6e5501efed" =
  ReleaseDocs.provenance!(docs_bytes, version)

# The uploader keeps legacy served docs. Keyless provenance verification refuses
# their missing release marker.
legacy_version = "1.5.1"
{:ok, {200, _headers, legacy_bytes}} =
  legacy_docs = Hex.Repo.get_docs("hexpm", "replicant", legacy_version)
:skip = PublishHexArtifacts.docs_action!(legacy_docs, true)

PublishHexArtifactsSelfTest.reject(
  "Hex docs archive has no release.json; publication refused",
  fn -> ReleaseDocs.provenance!(legacy_bytes, legacy_version) end
)

# Absent docs must read as the authoritative 404 that permits the only upload.
missing = Hex.Repo.get_docs("hexpm", "replicant", absent)
:upload = PublishHexArtifacts.docs_action!(missing, false)

PublishHexArtifactsSelfTest.reject(
  "Hex docs metadata and repository disagree; inspect public state before retrying",
  fn -> PublishHexArtifacts.docs_action!(missing, true) end
)

IO.puts("upload preconditions: live Hex self-tests PASS (no upload or key)")

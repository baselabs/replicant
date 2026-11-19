# Exercise the keyed uploader's read decisions against live Hex, without a key.
# Loads the real uploader module and never runs its publication entry point.
if Mix.Project.get(), do: raise("upload preconditions must run without a Mix project")

path = Path.join(__DIR__, "publish_hex_artifacts.exs")
{:ok, {:__block__, _, [module, _entry_point]}} = Code.string_to_quoted(File.read!(path))
Code.compile_quoted(module, path)
Hex.start()

alias Replicant.PublishHexArtifacts

defmodule PublishHexArtifactsSelfTest do
  def reject(expected, function) do
    function.()
    raise "upload preconditions accepted a forbidden release"
  rescue
    error in RuntimeError ->
      unless error.message == expected, do: reraise(error, __STACKTRACE__)
  end
end

# A published version whose immutable digest this repository records.
version = "1.5.1"
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

docs_digest =
  case docs do
    {:ok, {200, _headers, <<0x1F, 0x8B, _rest::binary>> = bytes}} ->
      :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

    _other ->
      raise "live Hex docs archive lookup failed"
  end

:skip = PublishHexArtifacts.docs_action!(docs, docs_digest, true)
:ok = PublishHexArtifacts.docs_read_back!(docs, docs_digest)

PublishHexArtifactsSelfTest.reject(
  "Hex docs checksum differs from the built archive; publication refused",
  fn -> PublishHexArtifacts.docs_action!(docs, String.duplicate("0", 64), false) end
)

# Absent docs must read as the authoritative 404 that permits the only upload.
:upload =
  PublishHexArtifacts.docs_action!(
    Hex.Repo.get_docs("hexpm", "replicant", absent),
    docs_digest,
    false
  )

IO.puts("upload preconditions: live Hex self-tests PASS (no upload or key)")

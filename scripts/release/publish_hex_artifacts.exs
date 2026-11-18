# Runs with --no-mix-exs: only Hex and the already inspected archives are loaded.
# No dependency fetch, build, project code, credential persistence, or response logging.
defmodule Replicant.PublishHexArtifacts do
  @moduledoc false

  def main([version, archive, docs]) do
    if Mix.Project.get(), do: raise("publication must run without a Mix project")

    # Read both files before uploading anything. The keyless build inspected these paths.
    package_bytes = File.read!(archive)
    docs_bytes = File.read!(docs)
    key = System.fetch_env!("HEX_API_KEY")
    if key == "", do: raise("HEX_API_KEY is empty; publication stopped")
    Hex.start()
    auth = [key: key]

    # A missing release alone cannot prove the package name already exists.
    existing_package!(Hex.API.Package.get("hexpm", "replicant"))
    absent_release!(Hex.API.Release.get("hexpm", "replicant", version))
    package_state = accepted!(Hex.API.Release.publish("hexpm", package_bytes, auth), "Hex package")

    docs_state =
      accepted!(
        Hex.API.ReleaseDocs.publish("hexpm", "replicant", version, docs_bytes, auth),
        "Hex docs"
      )

    if :staged in [package_state, docs_state] do
      raise "Hex approval pending; inspect public state before any retry"
    end

    digest = :crypto.hash(:sha256, package_bytes) |> Base.encode16(case: :lower)
    checksum!(Hex.API.Release.get("hexpm", "replicant", version), digest)
  end

  def main(_argv),
    do: raise("usage: publish_hex_artifacts.exs VERSION PACKAGE_ARCHIVE DOCS_ARCHIVE")

  def existing_package!({:ok, {200, _headers, _body}}), do: :ok

  def existing_package!({:ok, {404, _headers, _body}}),
    do: raise("the name replicant is not on Hex; a first publication is the owner's decision")

  def existing_package!(_result),
    do: raise("Hex package lookup failed; publication refused")

  def absent_release!({:ok, {404, _headers, _body}}), do: :ok

  def absent_release!({:ok, {200, _headers, _body}}),
    do: raise("Hex already has this replicant version; publication refused")

  def absent_release!(_result),
    do: raise("could not confirm the replicant version is absent; publication refused")

  def accepted!({:ok, {code, _headers, body}}, label) when code in 200..299 do
    IO.puts("#{label}: archive accepted")
    if is_map(body) and body["status"] == "staged", do: :staged, else: :published
  end

  def accepted!(_result, label),
    do: raise("#{label} upload failed or ambiguous; inspect public state before retrying")

  def checksum!({:ok, {200, _headers, %{"checksum" => digest}}}, digest), do: :ok

  def checksum!(_result, _digest),
    do: raise("Hex checksum does not match uploaded bytes; inspect public state before retrying")
end

Replicant.PublishHexArtifacts.main(System.argv())

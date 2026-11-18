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
    digest = :crypto.hash(:sha256, package_bytes) |> Base.encode16(case: :lower)

    case release_state!(Hex.API.Release.get("hexpm", "replicant", version), version, digest) do
      :fresh ->
        Hex.API.Release.publish("hexpm", package_bytes, auth)
        |> accepted!("Hex package")
        |> approved!()

      {:held, _docs?} ->
        :ok
    end

    # Re-read after the package decision: docs may have appeared in the meantime.
    case release_state!(Hex.API.Release.get("hexpm", "replicant", version), version, digest) do
      {:held, false} ->
        Hex.API.ReleaseDocs.publish("hexpm", "replicant", version, docs_bytes, auth)
        |> accepted!("Hex docs")
        |> approved!()

      {:held, true} ->
        :ok

      :fresh ->
        raise "Hex package is not served yet; inspect public state before retrying"
    end

    read_back!(Hex.API.Release.get("hexpm", "replicant", version), version, digest)
    docs_present!(Hex.API.ReleaseDocs.get("hexpm", "replicant", version))
  end

  def main(_argv),
    do: raise("usage: publish_hex_artifacts.exs VERSION PACKAGE_ARCHIVE DOCS_ARCHIVE")

  def existing_package!({:ok, {200, _headers, _body}}), do: :ok

  def existing_package!({:ok, {404, _headers, _body}}),
    do: raise("the name replicant is not on Hex; a first publication is the owner's decision")

  def existing_package!(_result),
    do: raise("Hex package lookup failed; publication refused")

  def release_state!({:ok, {404, _headers, _body}}, _version, _digest), do: :fresh

  def release_state!({:ok, {200, _headers, metadata}}, version, digest) do
    case metadata do
      %{
        "version" => ^version,
        "checksum" => ^digest,
        "retirement" => nil,
        "has_docs" => docs?
      }
      when is_boolean(docs?) ->
        if is_binary(digest) and Regex.match?(~r/\A[0-9a-f]{64}\z/, digest) do
          {:held, docs?}
        else
          raise "Hex release checksum is invalid; publication refused"
        end

      %{"retirement" => retirement} when not is_nil(retirement) ->
        raise "Hex release is retired; publication refused"

      _other ->
        raise "Hex release identity does not match the built package; publication refused"
    end
  end

  def release_state!(_result, _version, _digest),
    do: raise("Hex release lookup failed; publication refused")

  def accepted!({:ok, {code, _headers, body}}, label) when code in 200..299 do
    IO.puts("#{label}: archive accepted")
    if is_map(body) and body["status"] == "staged", do: :staged, else: :published
  end

  def accepted!(_result, label),
    do: raise("#{label} upload failed or ambiguous; inspect public state before retrying")

  def approved!(:published), do: :ok

  def approved!(:staged),
    do: raise("Hex approval pending; inspect public state before retrying the same commit")

  def read_back!(response, version, digest) do
    case release_state!(response, version, digest) do
      {:held, true} -> :ok
      _other -> raise "Hex package or docs are not served; inspect public state before retrying"
    end
  end

  # Presence only: the locked docs renderer generates random highlighting IDs,
  # and the inspected tar retains file timestamps. A retry's docs bytes can differ.
  # Hex follows the docs API redirect to its served archive; never replace it.
  def docs_present!({:ok, {200, _headers, <<0x1F, 0x8B, _rest::binary>>}}), do: :ok

  def docs_present!(_response),
    do: raise("Hex docs archive is unavailable; inspect public state before retrying")
end

Replicant.PublishHexArtifacts.main(System.argv())

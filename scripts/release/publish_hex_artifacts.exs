# Runs with --no-mix-exs: only Hex and the already inspected archives are loaded.
# No dependency fetch, build, project code, credential persistence, or response logging.
Code.require_file("release_docs.exs", __DIR__)

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
    existing_package!(Hex.API.Package.get_anonymous(nil, "replicant"))
    digest = :crypto.hash(:sha256, package_bytes) |> Base.encode16(case: :lower)

    case release_state!(Hex.API.Release.get(nil, "replicant", version), version, digest) do
      :fresh ->
        Hex.API.Release.publish(nil, package_bytes, auth)
        |> accepted!("Hex package")
        |> approved!()

      {:held, _docs?} ->
        :ok
    end

    # Re-read after the package decision: docs may have appeared in the meantime.
    case release_state!(Hex.API.Release.get(nil, "replicant", version), version, digest) do
      {:held, _docs?} ->
        case docs_action!(Hex.Repo.get_docs("hexpm", "replicant", version)) do
          :upload ->
            Hex.API.ReleaseDocs.publish(nil, "replicant", version, docs_bytes, auth)
            |> accepted!("Hex docs")
            |> approved!()

          :skip ->
            :ok
        end

      :fresh ->
        raise "Hex package is not served yet; inspect public state before retrying"
    end

    read_back!(Hex.API.Release.get(nil, "replicant", version), version, digest)
    package_read_back!(Hex.Repo.get_tarball("hexpm", "replicant", version), digest)
    docs_read_back!(Hex.Repo.get_docs("hexpm", "replicant", version), version)
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

  def package_read_back!({:ok, {200, _headers, bytes}}, digest) when is_binary(bytes) do
    if Base.encode16(:crypto.hash(:sha256, bytes), case: :lower) != digest do
      raise "Hex package archive checksum differs from the built package; publication refused"
    end

    :ok
  end

  def package_read_back!(_response, _digest),
    do: raise("Hex package archive is unavailable; inspect public state before retrying")

  # Read the served archive's raw bytes from the repository. The docs API
  # (Hex.API.ReleaseDocs.get) decodes responses by content type and returns a nil
  # body for the served archive. Presence wins even when metadata lags. Comparing
  # a rebuild could strand immutable served docs; never replace or compare them.
  def docs_action!({:ok, {200, _headers, bytes}}) do
    Replicant.ReleaseDocs.files!(bytes)
    :skip
  end

  def docs_action!({:ok, {404, _headers, _body}}), do: :upload

  def docs_action!(_response),
    do: raise("Hex docs archive is unavailable; inspect public state before retrying")

  # The following keyless retention step proves this commit's default-branch ancestry.
  def docs_read_back!({:ok, {200, _headers, bytes}}, version) do
    Replicant.ReleaseDocs.provenance!(bytes, version)
  end

  def docs_read_back!(_response, _version),
    do: raise("Hex docs archive is unavailable; inspect public state before retrying")
end

Replicant.PublishHexArtifacts.main(System.argv())

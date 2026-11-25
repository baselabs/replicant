# Runs with --no-mix-exs: only Hex and the already inspected archives are loaded.
# No dependency fetch, build, or project code. A private runner file allows revocation
# after cancellation. Its contents never enter an argument or log.
Code.require_file("release_docs.exs", __DIR__)

defmodule Replicant.PublishHexArtifacts do
  @moduledoc false

  @scope "package:hexpm/replicant"
  @audience "hexpm"

  def main([version, commit, archive, docs, plan]) do
    if Mix.Project.get(), do: raise("publication must run without a Mix project")

    case String.split(File.read!(plan)) do
      [^commit, ^version, package_plan, docs_plan]
      when package_plan in ["upload", "skip"] and docs_plan in ["upload", "skip"] ->
        if package_plan == "upload" or docs_plan == "upload" do
          publish!(version, File.read!(archive), File.read!(docs), package_plan, docs_plan)
        end

      _other ->
        raise "upload plan does not match this release"
    end
  end

  def main(["revoke"]) do
    if Mix.Project.get(), do: raise("revocation must run without a Mix project")
    Hex.start()
    revoke!(Hex.API.Client.config())
  end

  def main(_argv),
    do: raise("usage: publish_hex_artifacts.exs VERSION COMMIT PACKAGE_ARCHIVE DOCS_ARCHIVE PLAN")

  defp publish!(version, package, docs, package_plan, docs_plan) do
    Hex.start()
    existing_package!(Hex.API.Package.get_anonymous(nil, "replicant"))
    digest = :crypto.hash(:sha256, package) |> Base.encode16(case: :lower)
    config = Hex.API.Client.config()

    try do
      token = exchange!(config)
      auth = [key: token, oauth: true]

      case {package_plan, served_release!(config, version, digest)} do
        {"upload", :fresh} ->
          request(fn ->
            Hex.API.Release.publish(nil, package, auth, fn _ -> :ok end, replace: false)
          end)
          |> accepted!("Hex package")
          |> approved!()

        {"skip", :fresh} ->
          raise "Hex no longer serves the planned package; publication refused"

        {_plan, {:held, _docs?}} ->
          :ok
      end

      if docs_plan == "upload" do
        verify_package_bytes!(repo_archive("tarballs/replicant-#{version}.tar"), digest)

        case served_release!(config, version, digest) do
          {:held, docs?} ->
            # Hex's POST is an upsert. Revalidate the CDN absence after the
            # package checks and make the POST the very next request.
            if docs_action!(repo_archive("docs/replicant-#{version}.tar.gz"), docs?) == :upload do
              request(fn -> Hex.API.ReleaseDocs.publish(nil, "replicant", version, docs, auth) end)
              |> accepted!("Hex docs")
              |> approved!()
            end

          :fresh ->
            raise "Hex package is not served yet; dispatch again with the same release_commit"
        end
      end
    after
      revoke!(config)
    end
  end

  defp exchange!(config) do
    result =
      request(fn ->
        with {:ok, {200, _, %{"audience" => @audience}}} <-
               :mix_hex_api_oauth.oidc_audience(config),
             {:ok, provider} <- :mix_hex_oidc.detect_provider(),
             {:ok, assertion} <- :mix_hex_oidc.fetch_token(config, provider, @audience) do
          :mix_hex_api_oauth.jwt_bearer_token(config, assertion, @scope)
        end
      end)

    case result do
      {:ok, {status, _, %{"access_token" => token} = grant}}
      when is_binary(token) and byte_size(token) > 0 ->
        # Keep even a malformed grant's token for revocation before inspecting
        # the status, scope, type or lifetime.
        case File.write(token_path(), token, [:binary, :exclusive]) do
          :ok -> :ok
          {:error, _reason} ->
            revoke_token!(config, token)
            raise "could not keep the Hex token for cancellation cleanup"
        end

        case {status, grant} do
          {200, %{"scope" => @scope, "token_type" => "bearer", "expires_in" => ttl}}
          when is_integer(ttl) and ttl in 1..900 ->
            token

          _other ->
            raise "Hex workload identity grant metadata is invalid"
        end

      _other ->
        raise "Hex workload identity did not return a Replicant publish token"
    end
  end

  defp revoke!(config) do
    case File.read(token_path()) do
      {:ok, token} ->
        revoke_token!(config, token)
        File.rm!(token_path())

      {:error, :enoent} ->
        :ok

      {:error, _reason} ->
        raise "could not read the runner's Hex token for revocation"
    end
  end

  defp revoke_token!(config, token) do
    case request(fn -> :mix_hex_api.post(config, "oauth/revoke", %{"token" => token}) end) do
      {:ok, {200, _headers, _body}} -> :ok
      _other -> raise "Hex workload identity token revocation failed"
    end
  end

  defp token_path, do: Path.join(System.fetch_env!("RUNNER_TEMP"), "replicant-hex-token")

  def verify_package_bytes!({:ok, {200, _headers, bytes}}, digest) when is_binary(bytes) do
    if Base.encode16(:crypto.hash(:sha256, bytes), case: :lower) != digest do
      raise "Hex package archive checksum differs from the built package; publication refused"
    end

    :ok
  end

  def verify_package_bytes!(_response, _digest),
    do: raise("Hex package archive is unavailable; dispatch again with the same release_commit")

  defp repo_archive(path) do
    request(fn ->
      Hex.HTTP.request(
        :get,
        "https://repo.hex.pm/" <> path,
        %{"cache-control" => "no-cache"},
        :undefined,
        %{max_body_size: 16 * 1024 * 1024}
      )
    end)
  end

  # HTTP adapters can raise with the original request embedded in the error.
  defp request(function) do
    function.()
  rescue
    _error -> :error
  catch
    _kind, _reason -> :error
  end

  defp served_release!(config, version, digest) do
    # Recheck before each write, bypassing the public release metadata's cache.
    config = Map.update!(config, :http_headers, &Map.put(&1, "cache-control", "no-cache"))
    :mix_hex_api_release.get(config, "replicant", version) |> release_state!(version, digest)
  end

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
    do: raise("Hex approval pending; inspect public state then dispatch again with the same release_commit")

  # Read the served archive's raw bytes from the repository. The docs API
  # (Hex.API.ReleaseDocs.get) decodes responses by content type and returns a nil
  # body for the served archive. Presence wins even when metadata lags. Hex allows
  # republishing docs; infra's rule forbids this path from replacing or comparing
  # served docs, preserving their published commit. Comparing a rebuild could
  # refuse a held release whose package bytes match.
  def docs_action!({:ok, {200, _headers, bytes}}, _required?) do
    Replicant.ReleaseDocs.files!(bytes)
    :skip
  end

  def docs_action!({:ok, {404, _headers, _body}}, false), do: :upload

  def docs_action!({:ok, {404, _headers, _body}}, true),
    do: raise("Hex docs metadata and repository disagree; inspect public state before retrying")

  def docs_action!(_response, _required?),
    do: raise("Hex docs archive is unavailable; inspect public state before retrying")
end

Replicant.PublishHexArtifacts.main(System.argv())

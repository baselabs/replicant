unless Code.ensure_loaded?(Replicant.PackageChecksum) do
  Code.require_file("package_checksum.exs", __DIR__)
end

defmodule Replicant.PackagePublisher do
  @moduledoc false

  def publish(version, tar_bytes, opts \\ []) do
    digest = sha256(tar_bytes)
    expected = "#{version}:#{digest}"
    env = Keyword.get(opts, :env, &System.get_env/1)
    release_api = Keyword.get(opts, :release_api, Hex.API.Release)
    checksum = Keyword.get(opts, :checksum, Replicant.PackageChecksum)
    authorization = env.("REPLICANT_PUBLISH_AUTHORIZED")

    if authorization != expected do
      {:error, "--publish requires exact version:digest authorization for the witnessed artifact"}
    else
      case env.("HEX_API_KEY") do
        key when key in [nil, ""] ->
          {:error, "HEX_API_KEY not set"}

        key ->
          publish_exact(release_api, checksum, version, tar_bytes, digest, key)
      end
    end
  end

  defp publish_exact(release_api, checksum, version, tar_bytes, digest, key) do
    response =
      apply(release_api, :publish, ["hexpm", tar_bytes, [key: key], fn _ -> nil end, false])

    case response do
      {:ok, {status, _, _}} when status in 200..299 ->
        case apply(checksum, :verify!, [version, digest]) do
          :ok -> {:ok, digest}
          {:error, message} when is_binary(message) -> {:error, message}
          _ -> {:error, "published release checksum verification failed"}
        end

      {:ok, {status, _, _}} ->
        {:error, "publish failed with HTTP #{status}"}

      {:error, _reason} ->
        {:error,
         "publish response was ambiguous; Hex may have accepted the bytes — verify public state before retry"}
    end
  rescue
    _error ->
      {:error,
       "publish or checksum verification raised; Hex may have accepted the bytes — verify public state before retry"}
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end

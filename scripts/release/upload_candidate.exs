# Guarded exact-byte uploader for a witnessed Replicant package candidate.
# Dry-run is the default and never reads a credential or sends package bytes.
Mix.ensure_application!(:hex)

Code.require_file("package_identity.exs", __DIR__)
Code.require_file("package_witness.exs", __DIR__)

defmodule Replicant.UploadCandidate do
  @moduledoc false

  @repo_root Path.expand("../..", __DIR__)

  def run(argv) do
    {opts, rest, invalid} =
      OptionParser.parse(argv,
        strict: [publish: :boolean, artifact: :string, receipt: :string, witness_ref: :string]
      )

    if rest != [] or invalid != [], do: abort("invalid arguments")

    version = read_version()
    artifacts = Path.join([@repo_root, ".kimosabe", "artifacts"])
    tar = opts[:artifact] || Path.join(artifacts, "replicant-#{version}.tar")
    receipt = opts[:receipt] || Path.join(artifacts, "replicant-#{version}-receipt.txt")
    witness_ref = opts[:witness_ref] || "refs/attestations/packages/replicant/#{version}"
    publish? = opts[:publish] || false

    with :ok <- regular_file(tar),
         :ok <- regular_file(receipt),
         :ok <- Replicant.PackageWitness.verify(@repo_root, witness_ref, tar, receipt),
         {:ok, source_commit} <- Replicant.PackageWitness.receipt_source_commit(receipt),
         :ok <- check_metadata(tar, version),
         :ok <- check_identity(version, source_commit, publish?) do
      if publish? do
        publish(version, tar)
      else
        bytes = File.stat!(tar).size

        IO.puts("""
        upload_candidate: DRY-RUN — all guards passed, nothing uploaded.
          would call: Hex.API.Release.publish("hexpm", <#{bytes} exact witnessed bytes>, [key: <credential>], <progress>, false)
          credential: NOT read
          publication additionally requires --publish and exact version:digest authorization
        """)
      end
    else
      {:error, message} -> abort(message)
    end
  end

  defp read_version do
    Path.join(@repo_root, "mix.exs")
    |> File.read!()
    |> then(&Regex.run(~r/@version "([^"]+)"/, &1))
    |> case do
      [_, version] -> version
      _ -> abort("could not read package version")
    end
  end

  defp regular_file(path) do
    if File.regular?(path), do: :ok, else: {:error, "required regular file missing: #{path}"}
  end

  defp check_metadata(tar, version) do
    dest =
      Path.join(System.tmp_dir!(), "replicant-upload-meta-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dest)

    try do
      case :mix_hex_tarball.unpack(File.read!(tar), String.to_charlist(dest)) do
        {:ok, meta} ->
          metadata = Map.new(meta[:metadata] || %{})
          name = metadata["name"] || metadata[:name]
          found_version = metadata["version"] || metadata[:version]

          cond do
            name != "replicant" -> {:error, "package name is not replicant"}
            found_version != version -> {:error, "package version does not match #{version}"}
            true -> :ok
          end

        {:error, _reason} ->
          {:error, "Hex checksum validation failed"}
      end
    after
      File.rm_rf!(dest)
    end
  end

  defp check_identity(version, source_commit, true),
    do: Replicant.PackageIdentity.check_publish(version, source_commit)

  defp check_identity(version, _source_commit, false),
    do: Replicant.PackageIdentity.check_candidate(version)

  defp publish(version, tar) do
    digest = sha256(File.read!(tar))
    expected = "#{version}:#{digest}"

    unless System.get_env("REPLICANT_PUBLISH_AUTHORIZED") == expected do
      abort("--publish requires exact version:digest authorization for the witnessed artifact")
    end

    key = System.get_env("HEX_API_KEY") || abort("HEX_API_KEY not set")

    case Hex.API.Release.publish("hexpm", File.read!(tar), [key: key], fn _ -> nil end, false) do
      {:ok, {status, _, _}} when status in 200..299 ->
        IO.puts("upload_candidate: published replicant #{version} from exact witnessed bytes")

      {:ok, {status, _, _}} ->
        abort("publish failed with HTTP #{status}")

      {:error, _reason} ->
        abort("publish failed before a successful HTTP response")
    end
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp abort(message) do
    IO.puts(:stderr, "::error::upload_candidate: #{message}")
    System.halt(1)
  end
end

Replicant.UploadCandidate.run(System.argv())

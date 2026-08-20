# Guarded exact-byte uploader for a witnessed Replicant package candidate.
# Dry-run is the default and never reads a credential or sends package bytes.
Mix.ensure_application!(:hex)

Code.require_file("package_identity.exs", __DIR__)
Code.require_file("package_witness.exs", __DIR__)
Code.require_file("package_publisher.exs", __DIR__)

defmodule Replicant.UploadCandidate do
  @moduledoc false

  @repo_root Path.expand("../..", __DIR__)

  def run(argv) do
    {opts, rest, invalid} =
      OptionParser.parse(argv,
        strict: [publish: :boolean, artifact: :string, receipt: :string, witness_ref: :string]
      )

    if rest != [] or invalid != [], do: abort("invalid arguments")

    publish? = opts[:publish] || false

    if publish? and Enum.any?([:artifact, :receipt, :witness_ref], &Keyword.has_key?(opts, &1)) do
      abort("publish mode does not accept path or witness overrides")
    end

    version = read_version()
    artifacts = Path.join([@repo_root, ".kimosabe", "artifacts"])
    tar = opts[:artifact] || Path.join(artifacts, "replicant-#{version}.tar")
    receipt = opts[:receipt] || Path.join(artifacts, "replicant-#{version}-receipt.txt")
    witness_ref = opts[:witness_ref] || "refs/attestations/packages/replicant/#{version}"

    with {:ok, tar_bytes} <- Replicant.PackageWitness.read_immutable(tar),
         {:ok, receipt_bytes} <- Replicant.PackageWitness.read_immutable(receipt),
         :ok <-
           Replicant.PackageWitness.verify_content(
             @repo_root,
             witness_ref,
             tar_bytes,
             receipt_bytes
           ),
         {:ok, source_commit} <-
           Replicant.PackageWitness.receipt_source_commit_content(receipt_bytes),
         :ok <- check_metadata(tar_bytes, version),
         :ok <- check_identity(version, source_commit, publish?) do
      if publish? do
        publish(version, tar_bytes)
      else
        bytes = byte_size(tar_bytes)

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

  defp check_metadata(tar_bytes, version) do
    dest =
      Path.join(System.tmp_dir!(), "replicant-upload-meta-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dest)

    try do
      case :mix_hex_tarball.unpack(tar_bytes, String.to_charlist(dest)) do
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

  defp publish(version, tar_bytes) do
    case Replicant.PackagePublisher.publish(version, tar_bytes) do
      {:ok, _digest} ->
        IO.puts(
          "upload_candidate: published replicant #{version}; Hex checksum matches exact witnessed bytes"
        )

      {:error, message} ->
        abort(message)
    end
  end

  defp abort(message) do
    IO.puts(:stderr, "::error::upload_candidate: #{message}")
    System.halt(1)
  end
end

Replicant.UploadCandidate.run(System.argv())

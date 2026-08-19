# Guarded EXACT-BYTE uploader for the retained Replicant candidate — R07 machinery, landed and
# dry-run-verified in R06. R06 runs this in dry-run ONLY: it never uploads, tags, creates a
# GitHub release, or reads a Hex credential.
#
# Why not `mix hex.publish`: the standard task (`Mix.Tasks.Hex.Publish.create_release/3`) calls
# `Hex.Tar.create!/3` and publishes a freshly rebuilt in-memory tarball — it CANNOT upload the
# retained artifact. The exact-byte path is the pinned hex_core primitive
# `:mix_hex_api_release.publish(config, tarball_bytes, %{replace: false})`, which POSTs the raw
# tarball binary unchanged. (`Hex.API.Release.publish/5` delegates to this same primitive; the
# low-level call is used directly so the uploaded bytes are provably the retained bytes.)
#
# `replace: false` is hardcoded: an existing release is never overwritten.
#
# Usage:
#   mix run scripts/release/upload_candidate.exs            # dry-run (R06 default)
#   mix run scripts/release/upload_candidate.exs --publish  # R07 ONLY; also requires
#                                                           # REPLICANT_PUBLISH_AUTHORIZED=<version>:<sha256>
Mix.ensure_application!(:hex)

defmodule UploadCandidate do
  @repo_root Path.expand("../..", __DIR__)

  def run(argv) do
    publish? = "--publish" in argv
    version = read_version()
    artifacts = Path.join([@repo_root, ".kimosabe", "artifacts"])
    tar = Path.join(artifacts, "replicant-#{version}.tar")
    receipt = Path.join(artifacts, "replicant-#{version}-receipt.txt")

    checks = [
      check_exists(tar, "retained artifact"),
      check_exists(receipt, "receipt"),
      check_digest(tar, receipt),
      check_metadata(tar, version),
      check_source_commit(receipt),
      check_identity_free(version)
    ]

    Enum.each(checks, fn
      {:ok, msg} -> IO.puts("upload_candidate: OK — #{msg}")
      {:error, msg} -> abort(msg)
    end)

    opts = %{replace: false}
    bytes = File.stat!(tar).size

    if publish? do
      do_publish(version, tar, opts)
    else
      IO.puts("""
      upload_candidate: DRY-RUN — all guards passed, nothing uploaded.
        would call: :mix_hex_api_release.publish(config, <#{bytes} exact retained bytes>, #{inspect(opts)})
        credential: NOT read (dry-run reads no Hex API key)
        R07 requires: --publish AND REPLICANT_PUBLISH_AUTHORIZED=#{version}:<sha256>
      """)
    end
  end

  defp read_version do
    Path.join(@repo_root, "mix.exs")
    |> File.read!()
    |> then(&Regex.run(~r/@version "([^"]+)"/, &1))
    |> Enum.at(1)
  end

  defp check_exists(path, label) do
    if File.regular?(path), do: {:ok, "#{label} present (#{path})"}, else: {:error, "#{label} missing: #{path}"}
  end

  defp check_digest(tar, receipt) do
    actual = sha256(File.read!(tar))

    recorded =
      receipt |> File.read!() |> then(&Regex.run(~r/sha256:\s+([0-9a-f]{64})/, &1)) |> Enum.at(1)

    cond do
      is_nil(recorded) -> {:error, "receipt has no sha256"}
      actual == recorded -> {:ok, "artifact bytes match the receipt digest (#{String.slice(actual, 0, 12)}…)"}
      true -> {:error, "artifact digest #{actual} != receipt #{recorded} — bytes are not the recorded candidate"}
    end
  end

  defp check_metadata(tar, version) do
    dest = Path.join(System.tmp_dir!(), "replicant-upload-meta-#{:erlang.phash2(tar)}")
    File.rm_rf!(dest)
    File.mkdir_p!(dest)

    try do
      case :mix_hex_tarball.unpack(File.read!(tar), String.to_charlist(dest)) do
        {:ok, meta} ->
          m = Map.new(meta[:metadata] || %{})
          name = m["name"] || m[:name]
          ver = m["version"] || m[:version]

          cond do
            name != "replicant" -> {:error, "package name #{inspect(name)} != replicant"}
            ver != version -> {:error, "package version #{inspect(ver)} != #{version}"}
            true -> {:ok, "Hex-validated metadata is replicant #{ver}"}
          end

        {:error, reason} ->
          {:error, "Hex checksum validation failed: #{inspect(reason)}"}
      end
    after
      File.rm_rf!(dest)
    end
  end

  defp check_source_commit(receipt) do
    commit =
      receipt |> File.read!() |> then(&Regex.run(~r/source_commit:\s+([0-9a-f]{40})/, &1)) |> Enum.at(1)

    cond do
      is_nil(commit) ->
        {:error, "receipt has no 40-char source_commit"}

      System.cmd("git", ["cat-file", "-e", commit <> "^{commit}"], cd: @repo_root, stderr_to_stdout: true)
      |> elem(1) != 0 ->
        {:error, "recorded source_commit #{commit} is not a commit in this repo"}

      true ->
        {:ok, "source_commit #{String.slice(commit, 0, 12)}… exists"}
    end
  end

  # Identity collisions fail closed: the target version must not already be tagged locally or
  # released on GitHub. (Hex existence is re-checked live at publish time by the server via
  # replace:false, which rejects a duplicate.)
  defp check_identity_free(version) do
    tag = "v#{version}"

    tag_taken? =
      System.cmd("git", ["rev-parse", "-q", "--verify", "refs/tags/#{tag}"], cd: @repo_root, stderr_to_stdout: true)
      |> elem(1) == 0

    gh_taken? =
      case System.find_executable("gh") do
        nil ->
          false

        gh ->
          System.cmd(gh, ["release", "view", tag], cd: @repo_root, stderr_to_stdout: true) |> elem(1) == 0
      end

    cond do
      tag_taken? -> {:error, "local tag #{tag} already exists — identity taken"}
      gh_taken? -> {:error, "GitHub release #{tag} already exists — identity taken"}
      true -> {:ok, "no #{tag} tag or GitHub release yet (identity free)"}
    end
  end

  defp do_publish(version, tar, opts) do
    authorized = System.get_env("REPLICANT_PUBLISH_AUTHORIZED")
    digest = sha256(File.read!(tar))
    expected = "#{version}:#{digest}"

    unless authorized == expected do
      abort(
        "--publish requires REPLICANT_PUBLISH_AUTHORIZED=#{version}:<sha256> matching the retained " <>
          "artifact; refusing to upload without explicit human authorization naming the exact version and digest"
      )
    end

    key = System.get_env("HEX_API_KEY") || abort("HEX_API_KEY not set")
    config = :mix_hex_core.default_config() |> Map.put(:api_key, key)

    case :mix_hex_api_release.publish(config, File.read!(tar), opts) do
      {:ok, {code, _, _}} when code in [200, 201] ->
        IO.puts("upload_candidate: published replicant #{version} (exact bytes, #{String.slice(digest, 0, 12)}…)")

      other ->
        abort("publish failed: #{inspect(other)}")
    end
  end

  defp sha256(bin), do: :crypto.hash(:sha256, bin) |> Base.encode16(case: :lower)

  defp abort(msg) do
    IO.puts(:stderr, "::error::upload_candidate: #{msg}")
    System.halt(1)
  end
end

UploadCandidate.run(System.argv())

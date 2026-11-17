# Guarded exact-tree docs uploader for a witnessed Replicant package candidate.
# The publish workflow uploads docs after the package checksum read-back.
# A release without HexDocs fails the published-identity check.
#
# Docs are BUILT FROM THE WITNESSED SOURCE COMMIT (git archive), never from the
# working tree, and the upload is gated on the same exact version:digest
# authorization as the package publish. Dry-run is the default and never reads a
# credential or sends bytes.
Mix.ensure_application!(:hex)

Code.require_file("package_witness.exs", __DIR__)

defmodule Replicant.UploadDocs do
  @moduledoc false

  @repo_root Path.expand("../..", __DIR__)
  @package "replicant"

  def run(argv) do
    {opts, rest, invalid} =
      OptionParser.parse(argv, strict: [publish: :boolean, commit: :string])

    if rest != [] or invalid != [] do
      abort("usage: upload_docs.exs [--publish] [--commit <sha>]")
    end

    publish? = opts[:publish] || false
    version = read_version()
    receipt = Path.join([@repo_root, ".kimosabe", "artifacts", "replicant-#{version}-receipt.txt"])

    artifact = Path.join([@repo_root, ".kimosabe", "artifacts", "replicant-#{version}.tar"])
    witness_ref = "refs/attestations/packages/replicant/#{version}"

    receipt_bytes = immutable!(receipt)
    artifact_bytes = immutable!(artifact)

    case Replicant.PackageWitness.verify_content(
           @repo_root,
           witness_ref,
           artifact_bytes,
           receipt_bytes
         ) do
      :ok -> :ok
      {:error, message} -> abort(message)
    end

    source_commit =
      case Replicant.PackageWitness.receipt_source_commit_content(receipt_bytes) do
        {:ok, commit} -> commit
        {:error, reason} -> abort("witnessed receipt unreadable: #{inspect(reason)}")
      end

    # --commit pins the witnessed source explicitly (the default resolves it from
    # the receipt); a mismatch is a hard stop. Docs must describe the exact
    # published tree.
    if opts[:commit] && opts[:commit] != source_commit do
      abort("witnessed source commit is #{source_commit}, not #{opts[:commit]}")
    end

    digest =
      receipt_bytes
      |> String.split("\n")
      |> Enum.find_value(&Regex.run(~r/^sha256: ([0-9a-f]{64})$/, &1))
      |> case do
        [_, d] -> d
        nil -> abort("witnessed receipt has no valid digest")
      end

    expected = "#{version}:#{digest}"

    if publish? and System.get_env("REPLICANT_PUBLISH_AUTHORIZED") != expected do
      abort("--publish requires exact version:digest authorization for the witnessed artifact")
    end

    with {:ok, tar_bytes} <- build_docs_tarball(source_commit, version) do
      if publish? do
        key = credential!()

        case Hex.API.ReleaseDocs.publish("hexpm", @package, version, tar_bytes, [key: key]) do
          {:ok, {status, headers, _body}} when status in 200..299 ->
            IO.puts("upload_docs: docs published for #{@package} #{version} (HTTP #{status})")

            location =
              case headers["location"] do
                loc when is_binary(loc) -> loc
                loc when is_list(loc) -> Enum.join(loc, "")
                _ -> "n/a"
              end

            IO.puts("upload_docs: location: #{location}")

          {:ok, {status, _headers, _body}} ->
            abort("docs publish failed with HTTP #{status}")

          {:error, _reason} ->
            abort("docs publish response was ambiguous; verify public state before retry")
        end
      else
        IO.puts("""
        upload_docs: DRY-RUN - docs tarball built from witnessed commit #{source_commit} (#{byte_size(tar_bytes)} bytes), nothing uploaded.
          would call: Hex.API.ReleaseDocs.publish("hexpm", #{@package}, #{version}, <#{byte_size(tar_bytes)} bytes>, [key: <credential>])
          credential: NOT read
          publication additionally requires --publish and exact version:digest authorization
        """)
      end
    else
      {:error, message} -> abort(message)
    end
  end

  defp build_docs_tarball(source_commit, version) do
    build_tree =
      Path.join(System.tmp_dir!(), "replicant-docs-#{System.unique_integer([:positive])}")

    File.mkdir_p!(build_tree)

    try do
      render_docs_tarball(build_tree, source_commit, version)
    rescue
      error -> {:error, Exception.message(error)}
    after
      File.rm_rf!(build_tree)
    end
  end

  defp render_docs_tarball(build_tree, source_commit, version) do
    source_tar = Path.join(build_tree, "source.tar")

    case System.cmd("git", ["archive", "--format=tar", "--output", source_tar, source_commit],
           cd: @repo_root,
           stderr_to_stdout: true
         ) do
      {_out, 0} -> :ok
      {_out, code} -> raise "git archive failed with exit #{code}"
    end

    case System.cmd("tar", ["-xf", source_tar, "-C", build_tree], stderr_to_stdout: true) do
      {_out, 0} -> :ok
      {_out, code} -> raise "tar extract failed with exit #{code}"
    end

    # Docs render from the exact witnessed tree, deps resolved from the same
    # tree's lockfile (never the working tree's).
    docs_env = [
      {"MIX_ENV", "dev"},
      {"MIX_BUILD_PATH", Path.join(build_tree, "_build")},
      {"MIX_DEPS_PATH", Path.join(build_tree, "deps")},
      {"HEX_API_KEY", nil},
      {"REPLICANT_PUBLISH_AUTHORIZED", nil}
    ]

    with_cmd(build_tree, docs_env, "mix", ["deps.get", "--check-locked"], "mix deps.get")
    with_cmd(build_tree, docs_env, "mix", ["docs", "--warnings-as-errors"], "mix docs")

    doc_dir = Path.join(build_tree, "doc")
    index = Path.join(doc_dir, "index.html")

    unless File.exists?(index) do
      raise "ex_doc produced no doc/index.html for #{version} at #{source_commit}"
    end

    files =
      Path.join(doc_dir, "**")
      |> Path.wildcard()
      |> Enum.reject(&File.dir?/1)
      |> Enum.map(fn path ->
        rel = path |> Path.relative_to(doc_dir) |> to_charlist()
        {rel, File.read!(path)}
      end)

    case :mix_hex_tarball.create_docs(files) do
      {:ok, data} ->
        {:ok, data}

      {:error, _reason} ->
        raise "docs tarball creation failed"
    end
  end

  # System.cmd inherits the environment; nil removes publish credentials from
  # child builds while retaining PATH and the isolated dependency/build paths.
  defp with_cmd(cwd, env, cmd, args, label) do
    case System.cmd(cmd, args, cd: cwd, env: env, stderr_to_stdout: false) do
      {_out, 0} -> :ok
      {_out, code} -> raise "#{label} failed with exit #{code}"
    end
  end

  defp credential! do
    key = System.get_env("HEX_API_KEY")

    if key in [nil, ""] do
      abort("HEX_API_KEY not set")
    end

    key
  end

  defp read_version do
    @repo_root
    |> Path.join("mix.exs")
    |> File.read!()
    |> then(&Regex.run(~r/@version "([^"]+)"/, &1))
    |> case do
      [_, version] -> version
      _ -> abort("could not read package version")
    end
  end

  defp immutable!(path) do
    case Replicant.PackageWitness.read_immutable(path) do
      {:ok, binary} -> binary
      {:error, message} -> abort(message)
    end
  end

  defp abort(message) do
    IO.puts(:stderr, "::error::upload_docs: #{message}")
    System.halt(1)
  end
end

Replicant.UploadDocs.run(System.argv())

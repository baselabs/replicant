# Guarded exact-tree docs uploader for a witnessed Replicant package candidate.
# The release scripts build and AUDIT docs (consume_candidate.sh runs `mix docs
# --warnings-as-errors`) but never UPLOADED them — a release published without
# hexdocs fails this repo's own check_build ("Hex release documentation is
# unavailable"), so the upload is part of publishing, not an optional extra.
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

    receipt_bytes = read!(receipt)

    source_commit =
      case Replicant.PackageWitness.receipt_source_commit_content(receipt_bytes) do
        {:ok, commit} -> commit
        {:error, reason} -> abort("witnessed receipt unreadable: #{inspect(reason)}")
      end

    # --commit pins the witnessed source explicitly (the default resolves it from
    # the receipt); a mismatch is a hard stop — docs must describe the exact
    # published tree.
    if opts[:commit] && opts[:commit] != source_commit do
      abort("witnessed source commit is #{source_commit}, not #{opts[:commit]}")
    end

    digest =
      receipt_bytes
      |> String.split("
")
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

          {:ok, {status, _headers, body}} ->
            abort("docs publish failed with HTTP #{status}: #{inspect(body)}")

          {:error, reason} ->
            abort("docs publish error: #{inspect(reason)}")
        end
      else
        IO.puts("""
        upload_docs: DRY-RUN — docs tarball built from witnessed commit #{source_commit} (#{byte_size(tar_bytes)} bytes), nothing uploaded.
          would call: Hex.API.ReleaseDocs.publish("hexpm", #{@package}, #{version}, <#{byte_size(tar_bytes)} bytes>, [key: <credential>])
          credential: NOT read
          publication additionally requires --publish and exact version:digest authorization
        """)
      end
    end
  end

  defp build_docs_tarball(source_commit, version) do
    build_tree =
      Path.join(System.tmp_dir!(), "replicant-docs-#{System.unique_integer([:positive])}")

    File.mkdir_p!(build_tree)
    source_tar = Path.join(build_tree, "source.tar")

    case System.cmd("git", ["archive", "--format=tar", "--output", source_tar, source_commit],
           cd: @repo_root,
           stderr_to_stdout: true
         ) do
      {_out, 0} -> :ok
      {out, code} -> abort("git archive #{source_commit} failed (#{code}): #{out}")
    end

    case System.cmd("tar", ["-xf", source_tar, "-C", build_tree], stderr_to_stdout: true) do
      {_out, 0} -> :ok
      {out, code} -> abort("tar extract failed (#{code}): #{out}")
    end

    # Docs render from the exact witnessed tree, deps resolved from the same
    # tree's lockfile (never the working tree's).
    docs_env = [
      {"MIX_ENV", "dev"},
      {"MIX_BUILD_PATH", Path.join(build_tree, "_build")},
      {"MIX_DEPS_PATH", Path.join(build_tree, "deps")}
    ]

    base = System.get_env() |> Enum.map(fn {k, v} -> {to_string(k), to_string(v)} end)
    with_cmd(build_tree, docs_env, "mix", ["deps.get"], "mix deps.get", base)
    with_cmd(build_tree, docs_env, "mix", ["docs", "--warnings-as-errors"], "mix docs", base)

    doc_dir = Path.join(build_tree, "doc")
    index = Path.join(doc_dir, "index.html")

    unless File.exists?(index) do
      abort("ex_doc produced no doc/index.html for #{version} at #{source_commit}")
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
        File.rm_rf!(build_tree)
        {:ok, data}

      {:error, reason} ->
        abort("docs tarball creation failed: #{inspect(reason)}")
    end
  end

  # System.cmd/4's :env REPLACES the environment wholesale; the docs build needs
  # PATH (mix, elixir) and MIX_HOME alongside the isolation vars, so the base
  # environment is carried over with the isolating overrides applied on top.
  defp with_cmd(cwd, overrides, cmd, args, label, base_env) do
    env =
      base_env
      |> Kernel.++(overrides)
      |> Enum.map(fn {k, v} -> {to_string(k), to_string(v)} end)

    case System.cmd(cmd, args, cd: cwd, env: env, stderr_to_stdout: false) do
      {_out, 0} -> :ok
      {out, code} -> abort("#{label} failed with exit #{code}: #{out}")
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

  defp read!(path) do
    case File.read(path) do
      {:ok, binary} -> binary
      {:error, reason} -> abort("could not read #{path}: #{inspect(reason)}")
    end
  end

  defp abort(message) do
    IO.puts(:stderr, "::error::upload_docs: #{message}")
    System.halt(1)
  end
end

Replicant.UploadDocs.run(System.argv())

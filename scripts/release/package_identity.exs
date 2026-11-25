Code.require_file("release_docs.exs", __DIR__)

defmodule Replicant.PackageIdentity do
  @moduledoc false

  @repo_root Path.expand("../..", __DIR__)
  @github_release_path "repos/baselabs/replicant/releases/tags/"
  @hex_package_url "https://hex.pm/api/packages/replicant"
  @hex_release_url "https://hex.pm/api/packages/replicant/releases/"
  @hex_docs_url "https://repo.hex.pm/docs/replicant-"

  def check_candidate(version, runner \\ &run/2) do
    tag = "v#{version}"

    with :ok <- existing_package(runner),
         :ok <- absent_local_tag(tag, runner),
         :ok <- absent_remote_tag(tag, runner),
         :ok <- absent_github(tag, runner),
         {:ok, _state} <- check_archive(version, nil, runner) do
      :ok
    end
  end

  def check_build(version, source_commit, published_digest, runner \\ &run/2) do
    tag = "v#{version}"

    case tag_release_state(tag, runner) do
      :absent -> check_candidate(version, runner)
      :present -> check_published_build(version, tag, source_commit, published_digest, runner)
      {:error, _message} = error -> error
    end
  end

  # The release state comes from ORIGIN, not the runner's clone: an ephemeral CI
  # checkout without fetched tags must not route a published version down the
  # candidate path (which fails on the existing remote tag).
  defp tag_release_state(tag, runner) do
    case local_tag_state(tag, runner) do
      :present -> :present
      :absent -> remote_tag_state(tag, runner)
      {:error, _message} = error -> error
    end
  end

  defp remote_tag_state(tag, runner) do
    case runner.("git", ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/#{tag}"]) do
      {_output, 2} -> :absent
      {_output, 0} -> :present
      {output, status} -> {:error, "remote tag check failed (exit #{status}): #{structural(output)}"}
    end
  end

  def verify_candidate!(version), do: check_candidate(version) |> unwrap!()

  # Verify runs the source and tag gates before this keyless pass. Plan from
  # the actual inspected bytes and confirm any already served docs provenance.
  def verify_publish_archive!(version, release_commit, archive, repository, branch) do
    digest = :crypto.hash(:sha256, File.read!(archive)) |> Base.encode16(case: :lower)
    :ok = absent_github("v#{version}", &run/2) |> unwrap!()
    {:ok, state} = check_archive(version, digest) |> unwrap!()

    package =
      case state do
        :fresh ->
          :upload

        {:held, _docs?} ->
          case read_served("https://repo.hex.pm/tarballs/replicant-#{version}.tar", &run/2) do
            {200, bytes} ->
              if Base.encode16(:crypto.hash(:sha256, bytes), case: :lower) != digest do
                raise "served Hex package differs from the inspected package; publication refused"
              end

            _other ->
              raise "served Hex package is unavailable; publication refused"
          end

          :skip
      end

    docs =
      case docs_plan(version, state, &run/2) |> unwrap!() do
        {:ok, :upload} ->
          :upload

        {:ok, {:keep, bytes}} ->
          published = Replicant.ReleaseDocs.provenance!(bytes, version)
          Replicant.ReleaseDocs.verify_history!(published, release_commit, repository, branch)
          :skip
      end

    IO.puts("#{release_commit} #{version} #{package} #{docs}")
  end

  def check_archive(version, digest, runner \\ &run/2) do
    case read_served(@hex_release_url <> version, runner) do
      {404, _body} -> {:ok, :fresh}
      {200, body} -> held_release(body, version, digest)
      _other -> {:error, "Hex release lookup failed; publication refused"}
    end
  end

  def docs_plan(version, state, runner) do
    response = read_served(@hex_docs_url <> version <> ".tar.gz", runner)

    case response do
      {200, bytes} ->
        Replicant.ReleaseDocs.files!(bytes)
        {:ok, {:keep, bytes}}

      {404, _body} when state != {:held, true} ->
        {:ok, :upload}

      {404, _body} ->
        {:error, "Hex docs metadata and repository disagree; inspect public state before retrying"}

      _other ->
        {:error, "Hex docs archive is unavailable; publication refused"}
    end
  rescue
    _error -> {:error, "Hex docs archive is malformed; publication refused"}
  end

  # Use a byte offset because the HTTP body may be an archive.
  defp read_served(url, runner) do
    args = [
      "--silent",
      "--show-error",
      "--connect-timeout",
      "10",
      "--max-time",
      "30",
      "--write-out",
      "\n%{http_code}",
      url
    ]

    case runner.("curl", args) do
      {response, 0} when is_binary(response) and byte_size(response) >= 4 ->
        size = byte_size(response) - 4

        case response do
          <<body::binary-size(^size), "\n200">> -> {200, body}
          <<body::binary-size(^size), "\n404">> -> {404, body}
          _other -> :error
        end

      _other ->
        :error
    end
  end

  defp held_release(body, version, digest) do
    # OTP's JSON decoder uses :null; Hex's Erlang API uses nil in the uploader.
    with {:ok, metadata} <- decode_json(body, "Hex release") do
      case metadata do
        %{
          "version" => ^version,
          "checksum" => checksum,
          "retirement" => :null,
          "has_docs" => docs?
        }
        when is_binary(checksum) and is_boolean(docs?) ->
          cond do
            not Regex.match?(~r/\A[0-9a-f]{64}\z/, checksum) ->
              {:error, "Hex release checksum is invalid"}

            digest != nil and checksum != digest ->
              {:error, "Hex release checksum differs from the built package; publication refused"}

            true ->
              {:ok, {:held, docs?}}
          end

        %{"retirement" => retirement} when retirement != :null ->
          {:error, "Hex release is retired; publication refused"}

        _other ->
          {:error, "Hex release metadata is invalid; publication refused"}
      end
    end
  end

  def verify_build!(version, source_commit, published_digest),
    do: check_build(version, source_commit, published_digest) |> unwrap!()

  defp check_published_build(version, tag, source_commit, published_digest, runner) do
    with :ok <- valid_published_digest(published_digest),
         {:ok, tag_commit} <- local_tag_commit(tag, runner),
         :ok <- matching_remote_tag(tag, tag_commit, runner),
         :ok <- ancestor(tag, tag_commit, source_commit, runner),
         :ok <- published_github(tag, tag_commit, runner),
         :ok <- published_hex(version, published_digest, runner) do
      :ok
    end
  end

  defp valid_published_digest(digest) when is_binary(digest) do
    if Regex.match?(~r/\A[0-9a-f]{64}\z/, digest),
      do: :ok,
      else: {:error, "published package digest is missing or invalid"}
  end

  defp valid_published_digest(_digest),
    do: {:error, "published package digest is missing or invalid"}

  defp local_tag_state(tag, runner) do
    case runner.("git", ["rev-parse", "-q", "--verify", "refs/tags/#{tag}"]) do
      {_, 1} ->
        :absent

      {_, 0} ->
        :present

      {output, status} ->
        {:error, "local tag check failed (exit #{status}): #{structural(output)}"}
    end
  end

  defp local_tag_commit(tag, runner) do
    case runner.("git", ["rev-parse", "refs/tags/#{tag}^{}"]) do
      {output, 0} ->
        case String.trim(output) do
          "" -> {:error, "local tag #{tag} returned no commit"}
          commit -> {:ok, commit}
        end

      {output, status} ->
        {:error, "local tag #{tag} is unavailable (exit #{status}): #{structural(output)}"}
    end
  end

  defp ancestor(tag, tag_commit, source_commit, runner) do
    case runner.("git", ["merge-base", "--is-ancestor", tag_commit, source_commit]) do
      {_, 0} ->
        :ok

      {_, 1} ->
        {:error, "published tag #{tag} is not an ancestor of current source"}

      {output, status} ->
        {:error, "tag ancestry check failed (exit #{status}): #{structural(output)}"}
    end
  end

  defp absent_local_tag(tag, runner) do
    case runner.("git", ["rev-parse", "-q", "--verify", "refs/tags/#{tag}"]) do
      {_, 1} ->
        :ok

      {_, 0} ->
        {:error, "local tag #{tag} already exists"}

      {output, status} ->
        {:error, "local tag check failed (exit #{status}): #{structural(output)}"}
    end
  end

  defp absent_remote_tag(tag, runner) do
    case runner.("git", ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/#{tag}"]) do
      {_, 2} ->
        :ok

      {_, 0} ->
        {:error, "remote tag #{tag} already exists on origin"}

      {output, status} ->
        {:error, "remote tag check failed (exit #{status}): #{structural(output)}"}
    end
  end

  defp matching_remote_tag(tag, commit, runner) do
    args = [
      "ls-remote",
      "--exit-code",
      "--tags",
      "origin",
      "refs/tags/#{tag}",
      "refs/tags/#{tag}^{}"
    ]

    case runner.("git", args) do
      {output, 0} ->
        resolved = remote_tag_commit(output, tag)

        if is_nil(resolved) do
          {:error, "remote tag #{tag} returned no parseable commit"}
        else
          compare_tag("remote", tag, resolved, commit)
        end

      {output, status} ->
        {:error, "remote tag #{tag} is unavailable (exit #{status}): #{structural(output)}"}
    end
  end

  defp remote_tag_commit(output, tag) do
    refs =
      output
      |> String.split("\n", trim: true)
      |> Enum.map(&String.split(&1, "\t", parts: 2))
      |> Map.new(fn [sha, ref] -> {ref, sha} end)

    refs["refs/tags/#{tag}^{}"] || refs["refs/tags/#{tag}"]
  end

  defp compare_tag(_location, _tag, commit, commit), do: :ok

  defp compare_tag(location, tag, actual, expected) do
    {:error, "#{location} tag #{tag} resolves to #{actual}, expected recorded source #{expected}"}
  end

  defp existing_package(runner) do
    case http_status(@hex_package_url, runner) do
      {"200", 0} ->
        :ok

      {"404", 0} ->
        {:error, "the name replicant is not on Hex; a first publication is the owner's decision"}

      _ ->
        {:error, "Hex package lookup failed; publication refused"}
    end
  end

  defp http_status(url, runner) do
    args = [
      "--silent",
      "--show-error",
      "--connect-timeout",
      "10",
      "--max-time",
      "30",
      "--output",
      "/dev/null",
      "--write-out",
      "%{http_code}",
      url
    ]

    runner.("curl", args)
  end

  defp absent_github(tag, runner) do
    {output, status} = runner.("gh", ["api", "-i", @github_release_path <> tag])

    # gh exits 1 for HTTP errors, including the only response proving absence.
    # Read the status line, never a status-looking string in the response body.
    case {Regex.run(~r/\AHTTP\/\d+(?:\.\d+)? (\d{3})(?:[ \r\n]|\z)/, output), status} do
      {[_, "404"], 1} ->
        :ok

      {[_, "200"], 0} ->
        {:error, "GitHub release already exists"}

      _other ->
        {:error, "GitHub release check failed (exit #{status}): #{structural(output)}"}
    end
  end

  defp published_github(tag, tag_commit, runner) do
    with {:ok, metadata} <- fetch_github_json(tag, runner) do
      cond do
        metadata["tag_name"] != tag ->
          {:error, "GitHub release tag does not match #{tag}"}

        metadata["target_commitish"] != tag_commit ->
          {:error, "GitHub release target does not match the published tag commit"}

        metadata["draft"] != false ->
          {:error, "GitHub release is still a draft"}

        metadata["prerelease"] != false ->
          {:error, "GitHub release is marked as a prerelease"}

        true ->
          :ok
      end
    end
  end

  defp fetch_github_json(tag, runner) do
    case runner.("gh", ["api", @github_release_path <> tag]) do
      {body, 0} ->
        decode_json(body, "GitHub release")

      {output, status} ->
        {:error, "GitHub release check failed (exit #{status}): #{structural(output)}"}
    end
  end

  defp published_hex(version, published_digest, runner) do
    with {:ok, metadata} <- fetch_json(@hex_release_url <> version, "Hex release", runner) do
      cond do
        metadata["version"] != version ->
          {:error, "Hex release version does not match #{version}"}

        metadata["checksum"] != published_digest ->
          {:error, "Hex release checksum does not match the tracked published digest"}

        Map.fetch(metadata, "retirement") != {:ok, :null} ->
          {:error, "Hex release is retired or retirement metadata is unavailable"}

        metadata["has_docs"] != true ->
          {:error, "Hex release documentation is unavailable"}

        true ->
          :ok
      end
    end
  end

  defp fetch_json(url, label, runner) do
    args = [
      "--silent",
      "--show-error",
      "--fail",
      "--connect-timeout",
      "10",
      "--max-time",
      "30",
      url
    ]

    case runner.("curl", args) do
      {body, 0} ->
        decode_json(body, label)

      {output, exit_status} ->
        {:error, "#{label} check failed (exit #{exit_status}): #{structural(output)}"}
    end
  end

  defp decode_json(body, label) do
    case :json.decode(body) do
      metadata when is_map(metadata) -> {:ok, metadata}
      _other -> {:error, "#{label} returned an invalid metadata shape"}
    end
  rescue
    _error -> {:error, "#{label} returned invalid JSON"}
  catch
    _kind, _reason -> {:error, "#{label} returned invalid JSON"}
  end

  @doc false
  def run(command, args, timeout_ms \\ 30_000) do
    case System.find_executable(command) do
      nil ->
        {"required command unavailable", 127}

      executable ->
        task =
          Task.async(fn ->
            System.cmd(executable, args, cd: @repo_root, stderr_to_stdout: true)
          end)

        case Task.yield(task, timeout_ms) do
          {:ok, result} ->
            result

          nil ->
            Task.shutdown(task, :brutal_kill)
            {"command timed out", 124}
        end
    end
  end

  defp structural(output) do
    if String.trim(output) == "", do: "no output", else: "command returned output"
  end

  defp unwrap!(:ok), do: :ok
  defp unwrap!({:ok, _state} = result), do: result
  defp unwrap!({:error, message}), do: raise(message)
end

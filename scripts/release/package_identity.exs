defmodule Replicant.PackageIdentity do
  @moduledoc false

  @repo_root Path.expand("../..", __DIR__)
  @github_release_url "https://api.github.com/repos/baselabs/replicant/releases/tags/"
  @hex_release_url "https://hex.pm/api/packages/replicant/releases/"

  def check_candidate(version, runner \\ &run/2) do
    tag = "v#{version}"

    with :ok <- absent_local_tag(tag, runner),
         :ok <- absent_remote_tag(tag, runner),
         :ok <- absent_http(@github_release_url <> tag, "GitHub release", runner),
         :ok <- absent_http(@hex_release_url <> version, "Hex release", runner) do
      :ok
    end
  end

  def check_publish(version, source_commit, runner \\ &run/2) do
    tag = "v#{version}"

    with :ok <- matching_local_tag(tag, source_commit, runner),
         :ok <- matching_remote_tag(tag, source_commit, runner),
         :ok <- absent_http(@github_release_url <> tag, "GitHub release", runner),
         :ok <- absent_http(@hex_release_url <> version, "Hex release", runner) do
      :ok
    end
  end

  def check_build(version, source_commit, published_digest, runner \\ &run/2) do
    tag = "v#{version}"

    case local_tag_state(tag, runner) do
      :absent -> check_candidate(version, runner)
      :present -> check_published_build(version, tag, source_commit, published_digest, runner)
      {:error, _message} = error -> error
    end
  end

  def verify_candidate!(version), do: check_candidate(version) |> unwrap!()

  def verify_publish!(version, source_commit),
    do: check_publish(version, source_commit) |> unwrap!()

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

  defp matching_local_tag(tag, commit, runner) do
    case runner.("git", ["rev-parse", "refs/tags/#{tag}^{}"]) do
      {output, 0} ->
        compare_tag("local", tag, String.trim(output), commit)

      {output, status} ->
        {:error, "local tag #{tag} is unavailable (exit #{status}): #{structural(output)}"}
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

  defp absent_http(url, label, runner) do
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

    case runner.("curl", args) do
      {"404", 0} ->
        :ok

      {"200", 0} ->
        {:error, "#{label} already exists"}

      {status, 0} ->
        {:error, "#{label} check returned HTTP #{String.trim(status)}"}

      {output, exit_status} ->
        {:error, "#{label} check failed (exit #{exit_status}): #{structural(output)}"}
    end
  end

  defp published_github(tag, tag_commit, runner) do
    with {:ok, metadata} <- fetch_json(@github_release_url <> tag, "GitHub release", runner) do
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

  defp published_hex(version, published_digest, runner) do
    with {:ok, metadata} <- fetch_json(@hex_release_url <> version, "Hex release", runner) do
      cond do
        metadata["version"] != version ->
          {:error, "Hex release version does not match #{version}"}

        metadata["checksum"] != published_digest ->
          {:error, "Hex release checksum does not match the tracked published digest"}

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
  defp unwrap!({:error, message}), do: raise(message)
end

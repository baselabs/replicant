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

  def verify_candidate!(version), do: check_candidate(version) |> unwrap!()

  def verify_publish!(version, source_commit),
    do: check_publish(version, source_commit) |> unwrap!()

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

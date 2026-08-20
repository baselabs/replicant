defmodule Replicant.PackageWitness do
  @moduledoc false

  @zero String.duplicate("0", 40)

  def create(repo, ref, source_commit, receipt_path) do
    receipt = File.read!(receipt_path)

    with :ok <- receipt_commit_matches(receipt, source_commit),
         :ok <- commit_exists(repo, source_commit),
         {:ok, witness} <- create_commit(repo, source_commit, receipt_path, receipt),
         :ok <- create_ref(repo, ref, witness),
         :ok <- verify(repo, ref, nil, receipt_path) do
      :ok
    end
  end

  def create!(repo, ref, source_commit, receipt_path) do
    case create(repo, ref, source_commit, receipt_path) do
      :ok -> :ok
      {:error, message} -> raise message
    end
  end

  def verify(repo, ref, artifact_path, receipt_path) do
    receipt = File.read!(receipt_path)

    with {:ok, witnessed} <- git_output(repo, ["show", "#{ref}:candidate-receipt.txt"]),
         :ok <- exact_receipt(witnessed, receipt),
         {:ok, parent} <- git_output(repo, ["rev-parse", "#{ref}^"]),
         {:ok, commit} <- receipt_value(receipt, "source_commit", ~r/^[0-9a-f]{40}$/),
         :ok <- exact_parent(String.trim(parent), commit),
         :ok <- verify_artifact(artifact_path, receipt) do
      :ok
    end
  end

  def verify!(repo, ref, artifact_path, receipt_path) do
    case verify(repo, ref, artifact_path, receipt_path) do
      :ok -> :ok
      {:error, message} -> raise message
    end
  end

  def verify_copies(paths, expected_digest) do
    Enum.reduce_while(paths, :ok, fn path, :ok ->
      cond do
        not File.regular?(path) ->
          {:halt, {:error, "retained copy missing or non-regular: #{path}"}}

        sha256(File.read!(path)) != expected_digest ->
          {:halt, {:error, "retained copy digest mismatch: #{path}"}}

        true ->
          {:cont, :ok}
      end
    end)
  end

  def verify_copies!(paths, expected_digest) do
    case verify_copies(paths, expected_digest) do
      :ok -> :ok
      {:error, message} -> raise message
    end
  end

  def retain_copies!(source, destinations) do
    bytes = File.read!(source)

    Enum.each(destinations, fn destination ->
      File.mkdir_p!(Path.dirname(destination))

      case File.open(destination, [:write, :binary, :exclusive]) do
        {:ok, io} ->
          try do
            IO.binwrite(io, bytes)
          after
            File.close(io)
          end

          File.chmod!(destination, 0o444)

        {:error, :eexist} ->
          raise "refusing to overwrite retained package copy: #{destination}"

        {:error, reason} ->
          raise "could not retain package copy #{destination}: #{inspect(reason)}"
      end
    end)
  end

  def receipt_source_commit(receipt_path) do
    receipt_path |> File.read!() |> receipt_value("source_commit", ~r/^[0-9a-f]{40}$/)
  end

  defp create_commit(repo, source_commit, receipt_path, _receipt) do
    scratch =
      Path.join(
        System.tmp_dir!(),
        "replicant-package-witness-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(scratch)

    try do
      tree_input = Path.join(scratch, "tree")
      message = Path.join(scratch, "message")
      File.write!(message, "Replicant package witness\n")

      with {:ok, blob} <- git_output(repo, ["hash-object", "-w", receipt_path]),
           :ok <-
             File.write(tree_input, "100644 blob #{String.trim(blob)}\tcandidate-receipt.txt\n"),
           {:ok, tree} <- git_mktree(repo, tree_input),
           {:ok, witness} <-
             git_output(repo, [
               "commit-tree",
               String.trim(tree),
               "-p",
               source_commit,
               "-F",
               message
             ]) do
        {:ok, String.trim(witness)}
      end
    after
      File.rm_rf!(scratch)
    end
  end

  defp create_ref(repo, ref, witness) do
    case System.cmd("git", ["update-ref", ref, witness, @zero], cd: repo, stderr_to_stdout: true) do
      {_, 0} -> :ok
      {_, _} -> {:error, "package witness ref already exists or could not be created: #{ref}"}
    end
  end

  defp commit_exists(repo, commit) do
    case System.cmd("git", ["cat-file", "-e", "#{commit}^{commit}"],
           cd: repo,
           stderr_to_stdout: true
         ) do
      {_, 0} -> :ok
      {_, _} -> {:error, "recorded source commit is not present"}
    end
  end

  defp receipt_commit_matches(receipt, commit) do
    case receipt_value(receipt, "source_commit", ~r/^[0-9a-f]{40}$/) do
      {:ok, ^commit} -> :ok
      {:ok, _} -> {:error, "receipt source commit does not match witness parent"}
      error -> error
    end
  end

  defp exact_receipt(receipt, receipt), do: :ok
  defp exact_receipt(_, _), do: {:error, "receipt does not match immutable witness"}

  defp exact_parent(commit, commit), do: :ok
  defp exact_parent(_, _), do: {:error, "witness parent does not match receipt source commit"}

  defp verify_artifact(nil, _receipt), do: :ok

  defp verify_artifact(path, receipt) do
    with {:ok, recorded} <- receipt_value(receipt, "sha256", ~r/^[0-9a-f]{64}$/) do
      cond do
        not File.regular?(path) -> {:error, "candidate artifact missing or non-regular"}
        sha256(File.read!(path)) == recorded -> :ok
        true -> {:error, "candidate artifact digest does not match witnessed receipt"}
      end
    end
  end

  defp receipt_value(receipt, key, format) do
    case Regex.run(~r/^#{Regex.escape(key)}:\s*(\S+)\s*$/m, receipt) do
      [_, value] ->
        if value =~ format, do: {:ok, value}, else: {:error, "receipt has malformed #{key}"}

      _ ->
        {:error, "receipt has no #{key}"}
    end
  end

  defp git_output(repo, args) do
    env = [
      {"GIT_AUTHOR_NAME", "Replicant Package Witness"},
      {"GIT_AUTHOR_EMAIL", "replicant@example.invalid"},
      {"GIT_COMMITTER_NAME", "Replicant Package Witness"},
      {"GIT_COMMITTER_EMAIL", "replicant@example.invalid"}
    ]

    case System.cmd("git", args, cd: repo, env: env, stderr_to_stdout: true) do
      {output, 0} -> {:ok, output}
      {_, _} -> {:error, "package witness is missing or unreadable"}
    end
  end

  defp git_mktree(repo, input_path) do
    case System.cmd("sh", ["-c", "git mktree < \"$1\"", "sh", input_path],
           cd: repo,
           stderr_to_stdout: true
         ) do
      {output, 0} -> {:ok, output}
      {_, _} -> {:error, "could not create package witness tree"}
    end
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end

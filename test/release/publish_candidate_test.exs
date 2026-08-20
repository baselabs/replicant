defmodule Replicant.PublishCandidateTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../..", __DIR__)
  @script Path.join(@repo_root, "scripts/release/publish_candidate.sh")
  @loader Path.join(@repo_root, "scripts/release/credential_loader.sh")

  test "wrapper rejects wrong authorization before reading the project credential" do
    version = Mix.Project.config()[:version]

    receipt =
      Path.join([@repo_root, ".kimosabe", "artifacts", "replicant-#{version}-receipt.txt"])

    File.mkdir_p!(Path.dirname(receipt))

    created? =
      case File.open(receipt, [:write, :exclusive]) do
        {:ok, io} ->
          IO.binwrite(io, "sha256: #{String.duplicate("0", 64)}\n")
          File.close(io)
          true

        {:error, :eexist} ->
          false
      end

    if created?, do: on_exit(fn -> File.rm!(receipt) end)

    {output, status} =
      System.cmd("bash", [@script],
        cd: @repo_root,
        env: [{"REPLICANT_PUBLISH_AUTHORIZED", "#{version}:wrong"}],
        stderr_to_stdout: true
      )

    assert status == 1
    assert output =~ "exact version:digest authorization"
    refute output =~ "HEX_API_KEY"
  end

  test "publish mode rejects artifact, receipt, and witness overrides" do
    {output, status} =
      System.cmd(
        "mix",
        [
          "run",
          "--no-start",
          "scripts/release/upload_candidate.exs",
          "--publish",
          "--artifact",
          "/tmp/not-the-witnessed-package.tar"
        ],
        cd: @repo_root,
        stderr_to_stdout: true
      )

    assert status == 1
    assert output =~ "publish mode does not accept path or witness overrides"
  end

  test "credential loader replaces an inherited key from a file without a final newline" do
    env_file =
      Path.join(System.tmp_dir!(), "replicant-credential-#{System.unique_integer([:positive])}")

    File.write!(env_file, "HEX_API_KEY=file-key", [:binary])
    on_exit(fn -> File.rm(env_file) end)

    command = ~S'''
      source "$1"
      export HEX_API_KEY=ambient-key
      replicant_load_hex_api_key "$2"
      test "$HEX_API_KEY" = file-key
    '''

    {_output, status} = System.cmd("bash", ["-c", command, "bash", @loader, env_file])

    assert status == 0
  end
end

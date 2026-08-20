defmodule Replicant.PublishCandidateTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../..", __DIR__)
  @script Path.join(@repo_root, "scripts/release/publish_candidate.sh")
  @loader Path.join(@repo_root, "scripts/release/credential_loader.sh")

  test "wrapper rejects wrong authorization before reading the project credential" do
    version = Mix.Project.config()[:version]
    {fixture_root, fixture_script} = isolated_publish_fixture!(version)

    {output, status} =
      System.cmd("bash", [fixture_script],
        cd: fixture_root,
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

  defp isolated_publish_fixture!(version) do
    fixture_root =
      Path.join(
        System.tmp_dir!(),
        "replicant-publish-candidate-#{System.unique_integer([:positive, :monotonic])}"
      )

    script_dir = Path.join([fixture_root, "scripts", "release"])
    fixture_script = Path.join(script_dir, "publish_candidate.sh")

    receipt =
      Path.join([fixture_root, ".kimosabe", "artifacts", "replicant-#{version}-receipt.txt"])

    File.mkdir_p!(script_dir)
    on_exit(fn -> File.rm_rf!(fixture_root) end)

    File.cp!(@script, fixture_script)
    File.write!(Path.join(fixture_root, "mix.exs"), ~s(@version "#{version}"\n))
    File.mkdir_p!(Path.dirname(receipt))
    File.write!(receipt, "sha256: #{String.duplicate("0", 64)}\n")

    File.write!(
      Path.join(script_dir, "credential_loader.sh"),
      "echo HEX_API_KEY >&2\nexit 99\n"
    )

    {fixture_root, fixture_script}
  end
end

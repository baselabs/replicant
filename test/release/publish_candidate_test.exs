defmodule Replicant.PublishCandidateTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../..", __DIR__)
  @script Path.join(@repo_root, "scripts/release/publish_candidate.sh")

  test "wrapper rejects wrong authorization before reading the project credential" do
    {output, status} =
      System.cmd("bash", [@script],
        cd: @repo_root,
        env: [{"REPLICANT_PUBLISH_AUTHORIZED", "1.2.0:wrong"}],
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
end

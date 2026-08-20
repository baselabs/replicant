defmodule Replicant.ConsumeCandidateTest do
  use ExUnit.Case, async: true

  @script Path.expand("../../scripts/release/consume_candidate.sh", __DIR__)

  test "rejects a candidate whose digest differs from the required witnessed digest" do
    tarball =
      Path.join(System.tmp_dir!(), "replicant-wrong-digest-#{System.unique_integer()}.tar")

    File.write!(tarball, "not the witnessed package")
    on_exit(fn -> File.rm(tarball) end)

    expected = String.duplicate("a", 64)

    {output, status} =
      System.cmd("bash", [@script, tarball, "", expected], stderr_to_stdout: true)

    assert status == 1
    assert output =~ "candidate digest mismatch"
    refute output =~ "HEX_API_KEY"
  end
end

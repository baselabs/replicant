Code.require_file("../../scripts/release/package_identity.exs", __DIR__)

defmodule Replicant.PackageIdentityTest do
  use ExUnit.Case, async: true

  alias Replicant.PackageIdentity

  @version "1.2.0"
  @commit String.duplicate("a", 40)

  test "candidate identity is free only when every authoritative check proves absence" do
    runner = fn
      "git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"] ->
        {"", 1}

      "git", ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0"] ->
        {"", 2}

      "curl", args when is_list(args) ->
        assert "--connect-timeout" in args
        assert "--max-time" in args

        assert List.last(args) in [
                 "https://api.github.com/repos/baselabs/replicant/releases/tags/v1.2.0",
                 "https://hex.pm/api/packages/replicant/releases/1.2.0"
               ]

        {"404", 0}
    end

    assert :ok == PackageIdentity.check_candidate(@version, runner)
  end

  test "remote tag lookup errors fail closed instead of becoming absence" do
    runner = fn
      "git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"] ->
        {"", 1}

      "git", ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0"] ->
        {"network unavailable", 128}
    end

    assert {:error, message} = PackageIdentity.check_candidate(@version, runner)
    assert message =~ "remote tag check failed"
  end

  test "ambiguous GitHub and Hex responses fail closed" do
    base = fn
      "git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"] ->
        {"", 1}

      "git", ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0"] ->
        {"", 2}

      "curl", args ->
        case List.last(args) do
          "https://api.github.com/repos/baselabs/replicant/releases/tags/v1.2.0" -> {"503", 0}
          "https://hex.pm/api/packages/replicant/releases/1.2.0" -> {"404", 0}
        end
    end

    assert {:error, message} = PackageIdentity.check_candidate(@version, base)
    assert message =~ "GitHub release check returned HTTP 503"
  end

  test "publish requires the local and remote tag to dereference to the recorded commit" do
    runner = fn
      "git", ["rev-parse", "refs/tags/v1.2.0^{}"] ->
        {@commit <> "\n", 0}

      "git",
      ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0", "refs/tags/v1.2.0^{}"] ->
        {String.duplicate("b", 40) <> "\trefs/tags/v1.2.0\n", 0}
    end

    assert {:error, message} = PackageIdentity.check_publish(@version, @commit, runner)
    assert message =~ "remote tag v1.2.0 resolves to"
  end

  test "command execution has a hard deadline" do
    started = System.monotonic_time(:millisecond)

    assert {"command timed out", 124} =
             PackageIdentity.run("sh", ["-c", "sleep 1"], 25)

    assert System.monotonic_time(:millisecond) - started < 500
  end
end

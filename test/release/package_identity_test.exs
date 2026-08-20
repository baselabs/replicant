Code.require_file("../../scripts/release/package_identity.exs", __DIR__)

defmodule Replicant.PackageIdentityTest do
  use ExUnit.Case, async: true

  alias Replicant.PackageIdentity

  @version "1.2.0"
  @commit String.duplicate("a", 40)
  @digest String.duplicate("d", 64)

  test "candidate identity is free only when every authoritative check proves absence" do
    runner = fn
      "git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"] ->
        {"", 1}

      "git", ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0"] ->
        {"", 2}

      "curl", args when is_list(args) ->
        assert Enum.chunk_every(args, 2, 1, :discard) |> Enum.member?(["--connect-timeout", "10"])
        assert Enum.chunk_every(args, 2, 1, :discard) |> Enum.member?(["--max-time", "30"])

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

  test "package check accepts a coherent published release behind the current main commit" do
    tag_commit = String.duplicate("b", 40)

    runner = fn
      "git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"] ->
        {String.duplicate("c", 40) <> "\n", 0}

      "git", ["rev-parse", "refs/tags/v1.2.0^{}"] ->
        {tag_commit <> "\n", 0}

      "git",
      ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0", "refs/tags/v1.2.0^{}"] ->
        {tag_commit <> "\trefs/tags/v1.2.0^{}\n", 0}

      "git", ["merge-base", "--is-ancestor", ^tag_commit, @commit] ->
        {"", 0}

      "curl", args ->
        case List.last(args) do
          "https://api.github.com/repos/baselabs/replicant/releases/tags/v1.2.0" ->
            {~s({"tag_name":"v1.2.0","target_commitish":"#{tag_commit}","draft":false,"prerelease":false}),
             0}

          "https://hex.pm/api/packages/replicant/releases/1.2.0" ->
            {~s({"version":"1.2.0","checksum":"#{@digest}","has_docs":true}), 0}
        end
    end

    assert :ok == PackageIdentity.check_build(@version, @commit, @digest, runner)
  end

  test "package check rejects a published Hex checksum that differs from the tracked digest" do
    tag_commit = String.duplicate("b", 40)

    runner = fn
      "git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"] ->
        {String.duplicate("c", 40) <> "\n", 0}

      "git", ["rev-parse", "refs/tags/v1.2.0^{}"] ->
        {tag_commit <> "\n", 0}

      "git",
      ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0", "refs/tags/v1.2.0^{}"] ->
        {tag_commit <> "\trefs/tags/v1.2.0^{}\n", 0}

      "git", ["merge-base", "--is-ancestor", ^tag_commit, @commit] ->
        {"", 0}

      "curl", args ->
        case List.last(args) do
          "https://api.github.com/repos/baselabs/replicant/releases/tags/v1.2.0" ->
            {~s({"tag_name":"v1.2.0","target_commitish":"#{tag_commit}","draft":false,"prerelease":false}),
             0}

          "https://hex.pm/api/packages/replicant/releases/1.2.0" ->
            {~s({"version":"1.2.0","checksum":"#{String.duplicate("e", 64)}","has_docs":true}), 0}
        end
    end

    assert {:error, message} = PackageIdentity.check_build(@version, @commit, @digest, runner)
    assert message =~ "Hex release checksum does not match"
  end

  test "package check rejects a published tag outside current main history" do
    tag_commit = String.duplicate("b", 40)

    runner = fn
      "git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"] ->
        {String.duplicate("c", 40) <> "\n", 0}

      "git", ["rev-parse", "refs/tags/v1.2.0^{}"] ->
        {tag_commit <> "\n", 0}

      "git",
      ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0", "refs/tags/v1.2.0^{}"] ->
        {tag_commit <> "\trefs/tags/v1.2.0^{}\n", 0}

      "git", ["merge-base", "--is-ancestor", ^tag_commit, @commit] ->
        {"", 1}
    end

    assert {:error, message} = PackageIdentity.check_build(@version, @commit, @digest, runner)
    assert message =~ "is not an ancestor"
  end

  test "package check without a local tag still requires the full namespace to be free" do
    runner = fn
      "git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"] ->
        {"", 1}

      "git", ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0"] ->
        {String.duplicate("b", 40) <> "\trefs/tags/v1.2.0\n", 0}
    end

    assert {:error, message} = PackageIdentity.check_build(@version, @commit, nil, runner)
    assert message =~ "remote tag v1.2.0 already exists"
  end

  test "command execution has a hard deadline" do
    started = System.monotonic_time(:millisecond)

    assert {"command timed out", 124} =
             PackageIdentity.run("sh", ["-c", "sleep 1"], 25)

    assert System.monotonic_time(:millisecond) - started < 500
  end
end

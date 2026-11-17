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

  test "publish requires clean exact main and a free namespace before the operator tags" do
    assert :ok == PackageIdentity.check_publish(@version, @commit, &publish_runner/2)
  end

  test "publish refuses moved main, a different HEAD, a dirty tree, and an unavailable origin" do
    for {command, args, response} <- [
          {"git", ["rev-parse", "HEAD"], {String.duplicate("b", 40), 0}},
          {"git", ["status", "--porcelain"], {" M README.md\n", 0}},
          {"git", ["ls-remote", "--exit-code", "origin", "refs/heads/main"],
           {String.duplicate("b", 40) <> "\trefs/heads/main\n", 0}},
          {"git", ["ls-remote", "--exit-code", "origin", "refs/heads/main"], {"", 128}}
        ] do
      runner = fn
        ^command, ^args -> response
        cmd, argv -> publish_runner(cmd, argv)
      end

      assert {:error, message} = PackageIdentity.check_publish(@version, @commit, runner)
      assert message =~ "clean checkout of the witnessed commit at live origin/main"
    end
  end

  test "publish refuses every occupied release identity and ambiguous release reads" do
    for {command, args, response} <- [
          {"git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"], {@commit, 0}},
          {"git", ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0"],
           {@commit <> "\trefs/tags/v1.2.0\n", 0}},
          {"curl", :github, {"200", 0}},
          {"curl", :hex, {"200", 0}},
          {"curl", :hex, {"503", 0}}
        ] do
      runner = fn cmd, argv ->
        target = if cmd == "curl", do: release_endpoint(argv), else: argv

        if cmd == command and target == args,
          do: response,
          else: publish_runner(cmd, argv)
      end

      assert {:error, _message} = PackageIdentity.check_publish(@version, @commit, runner)
    end
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

  test "package check takes the PUBLISHED path when the ephemeral runner has no local tags but origin has the release tag" do
    # A CI checkout without fetched tags must not decide release state from its partial
    # clone: with the version published (tag on origin), the check routes to the
    # published-identity path, not the candidate path that fails on the existing remote
    # tag (OBSERVED: the post-1.4.0 CI run took the candidate path and failed, 2026-10-01).
    tag_commit = String.duplicate("b", 40)

    runner = fn
      "git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"] ->
        {"", 1}

      "git", ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0"] ->
        {tag_commit <> "\trefs/tags/v1.2.0\n", 0}

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

  test "package check with the release tag on origin fails closed when the tracked published digest is missing" do
    # A remote tag means the release state is PUBLISHED (origin, not the runner's
    # partial clone, decides) — so the candidate path's namespace-free check no longer
    # applies here; the mint-time gate (verify_candidate!) owns that check instead.
    # A published version with no tracked digest is incoherent and fails closed.
    runner = fn
      "git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"] ->
        {"", 1}

      "git", ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0"] ->
        {String.duplicate("b", 40) <> "\trefs/tags/v1.2.0\n", 0}
    end

    assert {:error, message} = PackageIdentity.check_build(@version, @commit, nil, runner)
    assert message =~ "published package digest is missing or invalid"
  end

  test "command execution has a hard deadline" do
    started = System.monotonic_time(:millisecond)

    assert {"command timed out", 124} =
             PackageIdentity.run("sh", ["-c", "sleep 1"], 25)

    assert System.monotonic_time(:millisecond) - started < 500
  end

  defp publish_runner("git", ["rev-parse", "HEAD"]), do: {@commit <> "\n", 0}
  defp publish_runner("git", ["status", "--porcelain"]), do: {"", 0}

  defp publish_runner("git", ["ls-remote", "--exit-code", "origin", "refs/heads/main"]),
    do: {@commit <> "\trefs/heads/main\n", 0}

  defp publish_runner("git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"]),
    do: {"", 1}

  defp publish_runner(
         "git",
         ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0"]
       ),
       do: {"", 2}

  defp publish_runner("curl", _args), do: {"404", 0}

  defp release_endpoint(args) do
    if String.starts_with?(List.last(args), "https://api.github.com/"), do: :github, else: :hex
  end
end

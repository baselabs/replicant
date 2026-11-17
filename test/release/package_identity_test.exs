Code.require_file("../../scripts/release/package_identity.exs", __DIR__)

defmodule Replicant.PackageIdentityTest do
  use ExUnit.Case, async: true

  alias Replicant.PackageIdentity

  @version "1.2.0"
  @commit String.duplicate("a", 40)
  @digest String.duplicate("d", 64)
  @github_path "repos/baselabs/replicant/releases/tags/v1.2.0"
  @hex_url "https://hex.pm/api/packages/replicant/releases/1.2.0"
  @absent "HTTP/2.0 404 Not Found\r\nContent-Type: application/json\r\n\r\n{}"

  test "candidate identity is free only when every authoritative check proves absence" do
    runner = fn
      "git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"] ->
        {"", 1}

      "git", ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0"] ->
        {"", 2}

      "gh", ["api", "-i", @github_path] ->
        {@absent, 1}

      "curl", args when is_list(args) ->
        assert Enum.chunk_every(args, 2, 1, :discard) |> Enum.member?(["--connect-timeout", "10"])
        assert Enum.chunk_every(args, 2, 1, :discard) |> Enum.member?(["--max-time", "30"])
        assert List.last(args) == @hex_url
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

  test "only a GitHub 404 status line with its expected exit proves absence" do
    for protocol <- ["HTTP/2.0", "HTTP/1.1"] do
      runner = fn
        "gh", ["api", "-i", @github_path] ->
          {"#{protocol} 404 Not Found\r\n\r\n{\"message\":\"absent\"}", 1}

        command, args ->
          publish_runner(command, args)
      end

      assert :ok == PackageIdentity.check_candidate(@version, runner)
    end
  end

  test "GitHub absence refuses other statuses, body-only 404s, and command failures" do
    marker = "private-response-value"

    for response <- [
          {"HTTP/2.0 200 OK\r\n\r\n#{marker}", 0},
          {"HTTP/2.0 401 Unauthorized\r\n\r\n#{marker}", 1},
          {"HTTP/2.0 403 Forbidden\r\n\r\n#{marker}", 1},
          {"HTTP/2.0 429 Too Many Requests\r\n\r\n#{marker}", 1},
          {"HTTP/2.0 503 Service Unavailable\r\n\r\n#{marker}", 1},
          {"HTTP/2.0 503 Service Unavailable\r\n\r\nHTTP/2.0 404 Not Found", 1},
          {"HTTP/2.0 200 OK\r\n\r\nHTTP/2.0 404 Not Found", 1},
          {"HTTP/2.0 4040 invalid\n#{marker}", 1},
          {"#{marker}\nHTTP/2.0 404 Not Found", 1},
          {"404", 1},
          {"", 0},
          {marker, 1},
          {marker, 4},
          {marker, 124},
          {marker, 127},
          {@absent, 0},
          {@absent, 4},
          {@absent, 124}
        ] do
      runner = fn
        "gh", ["api", "-i", @github_path] -> response
        command, args -> publish_runner(command, args)
      end

      assert {:error, message} = PackageIdentity.check_candidate(@version, runner)
      assert message =~ "GitHub release"
      refute message =~ marker
      refute message =~ "HTTP/"
    end
  end

  test "Hex responses still require a definitive absence" do
    for response <- [{"200", 0}, {"503", 0}, {"private-response-value", 22}] do
      runner = fn
        "curl", _args -> response
        command, args -> publish_runner(command, args)
      end

      assert {:error, message} = PackageIdentity.check_candidate(@version, runner)
      assert message =~ "Hex release"
      refute message =~ "private-response-value"
    end
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
          {"gh", ["api", "-i", @github_path], {"HTTP/2.0 200 OK\r\n\r\n{}", 0}},
          {"curl", @hex_url, {"200", 0}},
          {"curl", @hex_url, {"503", 0}}
        ] do
      runner = fn cmd, argv ->
        target = if cmd == "curl", do: List.last(argv), else: argv

        if cmd == command and target == args,
          do: response,
          else: publish_runner(cmd, argv)
      end

      assert {:error, _message} = PackageIdentity.check_publish(@version, @commit, runner)
    end
  end

  test "package check accepts a coherent published release behind the current main commit" do
    runner = published_runner(String.duplicate("b", 40))
    assert :ok == PackageIdentity.check_build(@version, @commit, @digest, runner)
  end

  test "published GitHub metadata refuses command failures and invalid JSON without values" do
    marker = "private-response-value"
    tag_commit = String.duplicate("b", 40)

    metadata =
      ~s({"tag_name":"v1.2.0","target_commitish":"#{tag_commit}","draft":false,"prerelease":false})

    for response <- [
          {metadata, 1},
          {metadata, 4},
          {metadata, 124},
          {marker, 1},
          {marker, 4},
          {marker, 22},
          {marker, 127},
          {marker, 0},
          {"", 0},
          {"[]", 0},
          {"null", 0},
          {~s("#{marker}"), 0}
        ] do
      runner = fn
        "gh", ["api", @github_path] -> response
        command, args -> published_runner(tag_commit).(command, args)
      end

      assert {:error, message} = PackageIdentity.check_build(@version, @commit, @digest, runner)
      assert message =~ "GitHub release"
      refute message =~ marker
      refute message =~ metadata
    end
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

      "gh", ["api", @github_path] ->
        {~s({"tag_name":"v1.2.0","target_commitish":"#{tag_commit}","draft":false,"prerelease":false}),
         0}

      "curl", args ->
        case List.last(args) do
          @hex_url ->
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

      "gh", ["api", @github_path] ->
        {~s({"tag_name":"v1.2.0","target_commitish":"#{tag_commit}","draft":false,"prerelease":false}),
         0}

      "curl", args ->
        case List.last(args) do
          @hex_url ->
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

  defp publish_runner("gh", ["api", "-i", @github_path]), do: {@absent, 1}

  defp publish_runner("curl", args) do
    assert List.last(args) == @hex_url
    {"404", 0}
  end

  defp published_runner(tag_commit) do
    fn
      "git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"] ->
        {String.duplicate("c", 40) <> "\n", 0}

      "git", ["rev-parse", "refs/tags/v1.2.0^{}"] ->
        {tag_commit <> "\n", 0}

      "git",
      ["ls-remote", "--exit-code", "--tags", "origin", "refs/tags/v1.2.0", "refs/tags/v1.2.0^{}"] ->
        {tag_commit <> "\trefs/tags/v1.2.0^{}\n", 0}

      "git", ["merge-base", "--is-ancestor", ^tag_commit, @commit] ->
        {"", 0}

      "gh", ["api", @github_path] ->
        {~s({"tag_name":"v1.2.0","target_commitish":"#{tag_commit}","draft":false,"prerelease":false}),
         0}

      "curl", args ->
        case List.last(args) do
          @hex_url ->
            {~s({"version":"1.2.0","checksum":"#{@digest}","has_docs":true}), 0}
        end
    end
  end
end

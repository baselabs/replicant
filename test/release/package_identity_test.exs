Code.require_file("../../scripts/release/package_identity.exs", __DIR__)

defmodule Replicant.PackageIdentityTest do
  use ExUnit.Case, async: true

  alias Replicant.PackageIdentity

  @version "1.2.0"
  @commit String.duplicate("a", 40)
  @digest String.duplicate("d", 64)
  @github_path "repos/baselabs/replicant/releases/tags/v1.2.0"
  @package_url "https://hex.pm/api/packages/replicant"
  @hex_url "https://hex.pm/api/packages/replicant/releases/1.2.0"
  @docs_url "https://repo.hex.pm/docs/replicant-1.2.0.tar.gz"
  @docs_bytes <<0x1F, 0x8B, 0xFF, "inspected docs">>
  @docs_digest :crypto.hash(:sha256, @docs_bytes) |> Base.encode16(case: :lower)
  @absent "HTTP/2.0 404 Not Found\r\nContent-Type: application/json\r\n\r\n{}"

  test "fresh candidate requires authoritative absence checks and an existing package" do
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

        case List.last(args) do
          @docs_url -> {@docs_bytes <> "\n200", 0}
          @package_url -> {"200", 0}
          @hex_url -> {"\n404", 0}
        end
    end

    assert :ok == PackageIdentity.check_candidate(@version, runner)
  end

  test "remote tag lookup errors fail closed instead of becoming absence" do
    runner = fn
      "curl", args ->
        publish_runner("curl", args)

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

  test "Hex release lookup refuses malformed metadata and ambiguous statuses" do
    for response <- [
          {"200", 0},
          {"\n503", 0},
          {"private-response-value\n200", 0},
          {"{}\n200", 0},
          {"[]\n200", 0},
          {"null\n200", 0},
          {"\n404", 22},
          {"private-response-value", 22}
        ] do
      runner = fn
        "curl", args ->
          if List.last(args) == @hex_url, do: response, else: publish_runner("curl", args)

        command, args ->
          publish_runner(command, args)
      end

      assert {:error, message} = PackageIdentity.check_candidate(@version, runner)
      assert message =~ "Hex release"
      refute message =~ "private-response-value"
    end
  end

  test "publish requires clean exact main and an untagged identity before infra tags" do
    assert :ok == PackageIdentity.check_publish(@version, @commit, &publish_runner/2)
  end

  test "fresh and held package decisions bind built bytes while docs may be missing or served" do
    assert {:ok, :fresh} ==
             PackageIdentity.check_archive(@version, @digest, nil, &publish_runner/2)

    for docs? <- [false, true] do
      runner = hex_runner(hex_metadata(docs?))
      assert :ok == PackageIdentity.check_publish(@version, @commit, runner)

      assert {:ok, {:held, docs?}} ==
               PackageIdentity.check_archive(@version, @digest, nil, runner)

      assert {:error, message} =
               PackageIdentity.check_archive(@version, String.duplicate("e", 64), nil, runner)

      assert message =~ "checksum differs from the built package"
    end
  end

  test "held docs compare served bytes before accepting either archive" do
    for docs? <- [false, true] do
      runner = hex_runner(hex_metadata(docs?))

      assert {:ok, {:held, docs?}} ==
               PackageIdentity.check_archive(@version, @digest, @docs_digest, runner)

      for response <- [
            {<<0x1F, 0x8B, "different">> <> "\n200", 0},
            {"private-response-value\n503", 0},
            {@docs_bytes <> "\n200", 22}
          ] do
        refused = docs_runner(runner, response)

        assert {:error, message} =
                 PackageIdentity.check_archive(@version, @digest, @docs_digest, refused)

        refute message =~ "private-response-value"
      end

      absent = docs_runner(runner, {"\n404", 0})
      result = PackageIdentity.check_archive(@version, @digest, @docs_digest, absent)

      if docs? do
        assert {:error, _message} = result
      else
        assert result == {:ok, {:held, false}}
      end
    end
  end

  test "docs response requires gzip, exact checksum and authoritative absence" do
    assert :ok == PackageIdentity.docs_match({@docs_bytes <> "\n200", 0}, @docs_digest, true)
    assert :ok == PackageIdentity.docs_match({"\n404", 0}, @docs_digest, false)

    for response <- [
          {"\n404", 0},
          {"\n404", 22},
          {"not gzip\n200", 0},
          {"\n200", 0},
          {"", 0},
          {"private-response-value", 7},
          {<<0x1F, 0x8B, "different">> <> "\n200", 0}
        ] do
      assert {:error, message} = PackageIdentity.docs_match(response, @docs_digest, true)
      refute message =~ "private-response-value"
    end
  end

  test "held metadata refuses retirement, version drift and malformed identity fields" do
    metadata = hex_metadata(false)

    for mutated <- [
          Map.put(metadata, "retirement", %{"reason" => "other"}),
          Map.put(metadata, "retirement", false),
          Map.delete(metadata, "retirement"),
          Map.put(metadata, "version", "1.2.1"),
          Map.put(metadata, "checksum", "private-response-value"),
          Map.delete(metadata, "checksum"),
          Map.put(metadata, "has_docs", nil),
          Map.delete(metadata, "has_docs")
        ] do
      runner = hex_runner(mutated)
      assert {:error, message} = PackageIdentity.check_publish(@version, @commit, runner)
      refute message =~ "private-response-value"
      assert {:error, _message} = PackageIdentity.check_archive(@version, @digest, nil, runner)
    end

    assert {:error, message} =
             PackageIdentity.check_archive(
               @version,
               @digest,
               nil,
               hex_runner(Map.put(metadata, "retirement", %{"reason" => "other"}))
             )

    assert message =~ "retired"
  end

  test "post-build publication hashes the actual archive and repeats source and tag refusals" do
    archive =
      Path.join(System.tmp_dir!(), "replicant-publish-#{System.unique_integer([:positive])}.tar")

    docs = archive <> ".docs.tar.gz"
    File.write!(docs, @docs_bytes)

    on_exit(fn ->
      File.rm!(archive)
      File.rm!(docs)
    end)

    File.write!(archive, "inspected-package-bytes")
    digest = :crypto.hash(:sha256, File.read!(archive)) |> Base.encode16(case: :lower)
    runner = hex_runner(Map.put(hex_metadata(false), "checksum", digest))

    assert :ok ==
             PackageIdentity.verify_publish_archive!(@version, @commit, archive, docs, runner)

    File.write!(docs, <<0x1F, 0x8B, "changed docs">>)

    assert_raise RuntimeError, ~r/docs checksum differs/, fn ->
      PackageIdentity.verify_publish_archive!(@version, @commit, archive, docs, runner)
    end

    File.write!(docs, @docs_bytes)

    File.write!(archive, "changed-package-bytes")

    assert_raise RuntimeError, ~r/checksum differs from the built package/, fn ->
      PackageIdentity.verify_publish_archive!(@version, @commit, archive, docs, runner)
    end

    for {command, args, response} <- [
          {"git", ["status", "--porcelain"], {" M mix.exs", 0}},
          {"git", ["rev-parse", "-q", "--verify", "refs/tags/v1.2.0"], {@commit, 0}},
          {"gh", ["api", "-i", @github_path], {"HTTP/2.0 200 OK\r\n\r\n{}", 0}}
        ] do
      refused = fn
        ^command, ^args -> response
        cmd, argv -> runner.(cmd, argv)
      end

      assert_raise RuntimeError, fn ->
        PackageIdentity.verify_publish_archive!(@version, @commit, archive, docs, refused)
      end
    end
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
      assert message =~ "clean checkout of the source commit at live origin/main"
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

  test "published checks refuse retired releases and missing retirement metadata" do
    tag_commit = String.duplicate("b", 40)

    for metadata <- [
          Map.put(hex_metadata(true), "retirement", %{"reason" => "other"}),
          Map.delete(hex_metadata(true), "retirement")
        ] do
      runner = fn
        "curl", args ->
          assert List.last(args) == @hex_url
          {Jason.encode!(metadata), 0}

        command, args ->
          published_runner(tag_commit).(command, args)
      end

      assert {:error, message} = PackageIdentity.check_build(@version, @commit, @digest, runner)
      assert message =~ "retired or retirement metadata is unavailable"
    end
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
            {Jason.encode!(hex_metadata(true)), 0}
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
            {Jason.encode!(Map.put(hex_metadata(true), "checksum", String.duplicate("e", 64))), 0}
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
    case List.last(args) do
      @docs_url -> {@docs_bytes <> "\n200", 0}
      @package_url -> {"200", 0}
      @hex_url -> {"\n404", 0}
    end
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
            {Jason.encode!(hex_metadata(true)), 0}
        end
    end
  end

  defp docs_runner(runner, response) do
    fn
      "curl", args ->
        if List.last(args) == @docs_url, do: response, else: runner.("curl", args)

      command, args ->
        runner.(command, args)
    end
  end

  defp hex_metadata(docs?) do
    %{"version" => @version, "checksum" => @digest, "has_docs" => docs?, "retirement" => nil}
  end

  defp hex_runner(metadata) do
    fn
      "curl", args ->
        if List.last(args) == @hex_url,
          do: {Jason.encode!(metadata) <> "\n200", 0},
          else: publish_runner("curl", args)

      command, args ->
        publish_runner(command, args)
    end
  end
end

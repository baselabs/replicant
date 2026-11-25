# Load only the script's module, leaving its command-line entry point unexecuted.
# Mix prunes archive paths during compilation; restore the real installed Hex APIs.
Mix.Local.append_archives()

for api <- [Hex.API.Package, Hex.API.Release, Hex.API.ReleaseDocs, Hex.Repo, :mix_hex_tarball] do
  Code.ensure_loaded!(api)
end

source = File.read!(Path.expand("../../scripts/release/publish_hex_artifacts.exs", __DIR__))
Code.require_file("../../scripts/release/release_docs.exs", __DIR__)
{:__block__, _, forms} = Code.string_to_quoted!(source)
module = Enum.find(forms, &match?({:defmodule, _, _}, &1))
Code.compile_quoted(module)

defmodule Replicant.PublishHexArtifactsTest do
  use ExUnit.Case, async: true

  alias Replicant.PublishHexArtifacts

  @script Path.expand("../../scripts/release/publish_hex_artifacts.exs", __DIR__)
  @version "1.2.0"
  @digest String.duplicate("a", 64)
  metadata =
    :json.encode(%{
      "package" => "replicant",
      "version" => @version,
      "commit" => String.duplicate("c", 40)
    })

  {:ok, docs_bytes} =
    :mix_hex_tarball.create_docs([
      {~c"index.html", "served docs"},
      {~c"release.json", IO.iodata_to_binary(metadata)}
    ])

  @docs_bytes docs_bytes
  @commit String.duplicate("a", 40)

  test "only an existing package name permits publication" do
    assert :ok == PublishHexArtifacts.existing_package!(response(200, %{}))

    assert_raise RuntimeError, ~r/replicant.*first publication.*owner's decision/, fn ->
      PublishHexArtifacts.existing_package!(response(404, %{}))
    end

    for result <- failures() do
      error = assert_raise RuntimeError, fn -> PublishHexArtifacts.existing_package!(result) end
      refute Exception.message(error) =~ "private-response-value"
    end
  end

  test "fresh and held releases select only missing uploads" do
    assert :fresh == PublishHexArtifacts.release_state!(response(404, %{}), @version, @digest)

    for docs? <- [false, true] do
      result = response(200, metadata(docs?))

      assert {:held, docs?} ==
               PublishHexArtifacts.release_state!(result, @version, @digest)
    end
  end

  test "checksum mismatch, retirement and malformed metadata refuse held publication" do
    for body <- [
          Map.put(metadata(false), "checksum", String.duplicate("b", 64)),
          Map.put(metadata(false), "retirement", %{"reason" => "other"}),
          Map.put(metadata(false), "retirement", false),
          Map.put(metadata(false), "version", "1.2.1"),
          Map.put(metadata(false), "has_docs", nil),
          Map.delete(metadata(false), "retirement"),
          Map.delete(metadata(false), "checksum"),
          Map.delete(metadata(false), "has_docs"),
          %{},
          nil,
          "private-response-value"
        ] do
      error =
        assert_raise RuntimeError, fn ->
          PublishHexArtifacts.release_state!(response(200, body), @version, @digest)
        end

      refute Exception.message(error) =~ "private-response-value"
    end

    assert_raise RuntimeError, ~r/checksum is invalid/, fn ->
      body = Map.put(metadata(false), "checksum", "invalid")
      PublishHexArtifacts.release_state!(response(200, body), @version, "invalid")
    end

    for result <- failures() do
      assert_raise RuntimeError, fn ->
        PublishHexArtifacts.release_state!(result, @version, @digest)
      end
    end
  end

  test "the uploader refuses to run inside a Mix project before reading anything" do
    assert Mix.Project.get()

    assert_raise RuntimeError, ~r/without a Mix project/, fn ->
      PublishHexArtifacts.main([
        @version,
        @commit,
        "missing-package.tar",
        "missing-docs.tar.gz",
        "missing-plan"
      ])
    end
  end

  test "docs upload requires the served package archive's exact bytes" do
    package = "served package archive"
    digest = :crypto.hash(:sha256, package) |> Base.encode16(case: :lower)
    assert :ok == PublishHexArtifacts.verify_package_bytes!(response(200, package), digest)

    assert_raise RuntimeError, ~r/package archive checksum differs/, fn ->
      PublishHexArtifacts.verify_package_bytes!(response(200, "different archive"), digest)
    end

    for result <- [response(404, "private-response-value"), response(200, nil) | failures()] do
      error =
        assert_raise RuntimeError, fn ->
          PublishHexArtifacts.verify_package_bytes!(result, digest)
        end

      refute Exception.message(error) =~ "private-response-value"
    end
  end

  test "served docs are kept by presence, while authoritative absence uploads" do
    for docs? <- [false, true] do
      assert :skip == PublishHexArtifacts.docs_action!(response(200, @docs_bytes), docs?)
    end

    assert :upload == PublishHexArtifacts.docs_action!(response(404, %{}), false)

    {:ok, legacy} = :mix_hex_tarball.create_docs([{~c"index.html", "legacy docs"}])
    assert :skip == PublishHexArtifacts.docs_action!(response(200, legacy), true)

    malformed = [nil, "", "not gzip", <<0x1F, 0x8B, "not a tarball">>, :zlib.gzip("not tar")]

    for result <- Enum.map(malformed, &response(200, &1)) ++ failures() do
      for docs? <- [false, true] do
        error =
          assert_raise RuntimeError, fn -> PublishHexArtifacts.docs_action!(result, docs?) end

        refute Exception.message(error) =~ "private-response-value"
      end
    end
  end

  test "a repository 404 cannot replace docs that release metadata says exist" do
    error =
      assert_raise RuntimeError, ~r/docs metadata and repository disagree/, fn ->
        PublishHexArtifacts.docs_action!(response(404, "private-response-value"), true)
      end

    refute Exception.message(error) =~ "private-response-value"
  end

  test "uploads refuse failures, ambiguity and approval holds without response values" do
    for result <- [response(403, "private-response-value"), {:error, :timeout}, nil] do
      error = assert_raise RuntimeError, fn -> PublishHexArtifacts.accepted!(result, "Hex") end
      refute Exception.message(error) =~ "private-response-value"
    end

    ExUnit.CaptureIO.capture_io(fn ->
      assert :published == PublishHexArtifacts.accepted!(response(201, %{}), "Hex")
      assert :published == PublishHexArtifacts.accepted!(response(201, ""), "Hex")

      assert :staged ==
               PublishHexArtifacts.accepted!(response(202, %{"status" => "staged"}), "Hex")
    end)

    assert :ok == PublishHexArtifacts.approved!(:published)

    assert_raise RuntimeError, ~r/approval pending/, fn ->
      PublishHexArtifacts.approved!(:staged)
    end
  end

  test "the isolated uploader orders package and absence checks before its docs write" do
    source = File.read!(@script)

    required = [
      ~s|case String.split(File.read!(plan))|,
      ~s|existing_package!(Hex.API.Package.get_anonymous(nil, "replicant"))|,
      ~s|token = exchange!(config)|,
      ~s|Hex.API.Release.publish(nil, package, auth, fn _ -> :ok end, replace: false)|,
      ~s|if docs_plan == "upload" do|,
      ~S|verify_package_bytes!(repo_archive("tarballs/replicant-#{version}.tar"), digest)|,
      ~s|case served_release!(config, version, digest) do|,
      ~S|docs_action!(repo_archive("docs/replicant-#{version}.tar.gz"), docs?) == :upload|,
      ~s|Hex.API.ReleaseDocs.publish(nil, "replicant", version, docs, auth)|,
      "after\n      revoke!(config)"
    ]

    offsets =
      for checkpoint <- required do
        assert {offset, _size} = :binary.match(source, checkpoint)
        offset
      end

    assert offsets == Enum.sort(offsets)
    refute source =~ ~r/Mix\.Task|System\.cmd|deps\.get|mix docs|hex\.build/
  end

  test "the grant's token is kept before metadata validation and removed after revocation" do
    source = File.read!(@script)
    assert source =~ ~s|%{"access_token" => token} = grant|

    assert {stored, _} =
             :binary.match(source, "File.write(token_path(), token, [:binary, :exclusive])")

    assert {validated, _} = :binary.match(source, "case {status, grant} do")
    assert stored < validated
    assert source =~ "revoke_token!(config, token)\n        File.rm!(token_path())"
    assert source =~ ~s|:mix_hex_api.post(config, "oauth/revoke", %{"token" => token})|
  end

  defp metadata(docs?) do
    %{"version" => @version, "checksum" => @digest, "retirement" => nil, "has_docs" => docs?}
  end

  defp response(status, body), do: {:ok, {status, [], body}}

  defp failures do
    statuses =
      for status <- [201, 204, 301, 401, 403, 429, 500, 503] do
        response(status, "private-response-value")
      end

    statuses ++ [{:error, :timeout}, nil, {:ok, {"200", [], %{}}}]
  end
end

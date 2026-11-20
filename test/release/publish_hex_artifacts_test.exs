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
  @docs_read ~s|docs_read_back!(Hex.Repo.get_docs("hexpm", "replicant", version), version)|
  @docs_action ~s|docs_action!(Hex.Repo.get_docs("hexpm", "replicant", version), docs?)|
  @package_read ~s|package_read_back!(Hex.Repo.get_tarball("hexpm", "replicant", version), digest)|
  @read ~s|release_state!(Hex.API.Release.get(nil, "replicant", version), version, digest)|
  @read_back ~s|read_back!(Hex.API.Release.get(nil, "replicant", version), version, digest)|
  @exists ~s|existing_package!(Hex.API.Package.get_anonymous(nil, "replicant"))|
  @package ~s|Hex.API.Release.publish(nil, package_bytes, auth)|
  @docs ~s|Hex.API.ReleaseDocs.publish(nil, "replicant", version, docs_bytes, auth)|
  @guard ~s|if Mix.Project.get(), do: raise("publication must run without a Mix project")|

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
      PublishHexArtifacts.main(["0.0.0", "missing-package.tar", "missing-docs.tar.gz"])
    end
  end

  test "read-back requires the exact live package, no retirement and served docs" do
    assert :ok == PublishHexArtifacts.read_back!(response(200, metadata(true)), @version, @digest)

    incomplete = [
      response(404, %{}),
      response(200, metadata(false)),
      response(200, Map.put(metadata(true), "checksum", String.duplicate("b", 64))),
      response(200, Map.put(metadata(true), "retirement", %{"reason" => "other"}))
    ]

    for result <- incomplete ++ failures() do
      assert_raise RuntimeError, fn ->
        PublishHexArtifacts.read_back!(result, @version, @digest)
      end
    end
  end

  test "package read-back requires the served archive's exact bytes" do
    package = "served package archive"
    digest = :crypto.hash(:sha256, package) |> Base.encode16(case: :lower)
    assert :ok == PublishHexArtifacts.package_read_back!(response(200, package), digest)

    assert_raise RuntimeError, ~r/package archive checksum differs/, fn ->
      PublishHexArtifacts.package_read_back!(response(200, "different archive"), digest)
    end

    for result <- [response(404, "private-response-value"), response(200, nil) | failures()] do
      error =
        assert_raise RuntimeError, fn ->
          PublishHexArtifacts.package_read_back!(result, digest)
        end

      refute Exception.message(error) =~ "private-response-value"
    end
  end

  test "served docs are kept by presence, while authoritative absence uploads" do
    for docs? <- [false, true] do
      assert :skip == PublishHexArtifacts.docs_action!(response(200, @docs_bytes), docs?)
    end

    assert :upload == PublishHexArtifacts.docs_action!(response(404, %{}), false)

    assert String.duplicate("c", 40) ==
             PublishHexArtifacts.docs_read_back!(response(200, @docs_bytes), @version)

    {:ok, legacy} = :mix_hex_tarball.create_docs([{~c"index.html", "legacy docs"}])
    assert :skip == PublishHexArtifacts.docs_action!(response(200, legacy), true)

    assert_raise RuntimeError, ~r/no release.json/, fn ->
      PublishHexArtifacts.docs_read_back!(response(200, legacy), @version)
    end

    malformed = [nil, "", "not gzip", <<0x1F, 0x8B, "not a tarball">>, :zlib.gzip("not tar")]

    for result <- Enum.map(malformed, &response(200, &1)) ++ failures() do
      for docs? <- [false, true] do
        error = assert_raise RuntimeError, fn -> PublishHexArtifacts.docs_action!(result, docs?) end
        refute Exception.message(error) =~ "private-response-value"
      end
    end

    for result <- [response(404, %{}) | Enum.map(malformed, &response(200, &1))] ++ failures() do
      error =
        assert_raise RuntimeError, fn ->
          PublishHexArtifacts.docs_read_back!(result, @version)
        end

      refute Exception.message(error) =~ "private-response-value"
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

  test "the isolated uploader gates both writes and reads both served artifacts back" do
    source = File.read!(@script)
    assert_isolated_upload!(source)

    for {before, after_change} <- [
          {@exists, ":ok"},
          {@read, ":fresh"},
          {@package, "mix hex.publish"},
          {@docs, "mix docs"},
          {"{:held, _docs?} ->", ":fresh ->"},
          {":upload ->", ":skip ->"},
          {@docs_action, ":skip"},
          {"Replicant.ReleaseDocs.files!(bytes)", ":ok"},
          {"Replicant.ReleaseDocs.provenance!(bytes, version)", ":ok"},
          {"|> approved!()", "|> then(fn _ -> :ok end)"},
          {@read_back, ":ok"},
          {@package_read, ":ok"},
          {@docs_read, ":ok"},
          {"!= digest do", "== digest do"},
          {~s|"checksum" => ^digest|, ~s|"checksum" => _checksum|},
          {~s|"retirement" => nil|, ~s|"retirement" => _retirement|}
        ] do
      mutated = String.replace(source, before, after_change)
      refute mutated == source
      assert_raise ExUnit.AssertionError, fn -> assert_isolated_upload!(mutated) end
    end

    commands = ["System.cmd", "Code.require_file", "Mix.Task.run", "Mix.Project", "File.write!"]

    for command <- commands do
      mutated = source <> "\n" <> command
      assert_raise ExUnit.AssertionError, fn -> assert_isolated_upload!(mutated) end
    end
  end

  defp assert_isolated_upload!(source) do
    assert source =~ @guard

    without_guard =
      source
      |> String.replace(@guard, "")
      |> String.replace(~s|Code.require_file("release_docs.exs", __DIR__)|, "")

    refute without_guard =~ ~r/System\.cmd|Code\.|Mix\.|File\.write|deps\.get|mix docs|hex\.build/
    assert source =~ "package_bytes = File.read!(archive)"
    assert source =~ "docs_bytes = File.read!(docs)"
    assert source =~ ~s|"checksum" => ^digest|
    assert source =~ ~s|"retirement" => nil|
    parts = String.split(source, "case " <> @read <> " do")
    assert length(parts) == 3
    [before_package, package_case, docs_case] = parts
    assert before_package =~ @exists
    assert package_case =~ ":fresh ->\n        " <> @package
    assert package_case =~ "{:held, _docs?} ->\n        :ok"
    assert package_case =~ "|> accepted!(\"Hex package\")\n        |> approved!()"
    assert docs_case =~ "{:held, docs?} ->"
    assert docs_case =~ "case " <> @docs_action <> " do"
    assert docs_case =~ ":upload ->\n            " <> @docs
    assert docs_case =~ ":skip ->\n            :ok"
    assert docs_case =~ "|> accepted!(\"Hex docs\")\n            |> approved!()"

    assert docs_case =~ @read_back
    assert docs_case =~ @package_read
    assert docs_case =~ @docs_read
    assert source =~ "Replicant.ReleaseDocs.files!(bytes)"
    assert source =~ "Replicant.ReleaseDocs.provenance!(bytes, version)"
    refute source =~ "docs_digest"
    assert source =~ "Base.encode16(:crypto.hash(:sha256, bytes), case: :lower) != digest do"
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

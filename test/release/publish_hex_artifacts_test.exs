# Load only the script's module, leaving its command-line entry point unexecuted.
source = File.read!(Path.expand("../../scripts/release/publish_hex_artifacts.exs", __DIR__))
{:__block__, _, [module, _entry_point]} = Code.string_to_quoted!(source)
Code.compile_quoted(module)

defmodule Replicant.PublishHexArtifactsTest do
  use ExUnit.Case, async: true

  alias Replicant.PublishHexArtifacts

  @script Path.expand("../../scripts/release/publish_hex_artifacts.exs", __DIR__)
  @version "1.2.0"
  @digest String.duplicate("a", 64)
  @read ~s|release_state!(Hex.API.Release.get("hexpm", "replicant", version), version, digest)|
  @package ~s|Hex.API.Release.publish("hexpm", package_bytes, auth)|
  @docs ~s|Hex.API.ReleaseDocs.publish("hexpm", "replicant", version, docs_bytes, auth)|
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

    # Docs are presence-only: different rendered bytes are legitimate on a retry.
    for archive <- [<<0x1F, 0x8B, "first">>, <<0x1F, 0x8B, "different">>] do
      assert :ok == PublishHexArtifacts.docs_present!(response(200, archive))
    end

    missing = [response(404, %{}), response(200, ""), response(200, "not gzip") | failures()]

    for result <- missing do
      assert_raise RuntimeError, fn -> PublishHexArtifacts.docs_present!(result) end
    end
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
          {~s|existing_package!(Hex.API.Package.get("hexpm", "replicant"))|, ":ok"},
          {@read, ":fresh"},
          {@package, "mix hex.publish"},
          {@docs, "mix docs"},
          {"{:held, _docs?} ->", ":fresh ->"},
          {"{:held, false} ->", "{:held, true} ->"},
          {"|> approved!()", "|> then(fn _ -> :ok end)"},
          {~s|read_back!(Hex.API.Release.get("hexpm", "replicant", version), version, digest)|,
           ":ok"},
          {~s|docs_present!(Hex.API.ReleaseDocs.get("hexpm", "replicant", version))|, ":ok"},
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
    without_guard = String.replace(source, @guard, "")
    refute without_guard =~ ~r/System\.cmd|Code\.|Mix\.|File\.write|deps\.get|mix docs|hex\.build/
    assert source =~ "package_bytes = File.read!(archive)"
    assert source =~ "docs_bytes = File.read!(docs)"
    assert source =~ ~s|"checksum" => ^digest|
    assert source =~ ~s|"retirement" => nil|
    parts = String.split(source, "case " <> @read <> " do")
    assert length(parts) == 3
    [before_package, package_case, docs_case] = parts
    assert before_package =~ ~s|existing_package!(Hex.API.Package.get("hexpm", "replicant"))|
    assert package_case =~ ":fresh ->\n        " <> @package
    assert package_case =~ "{:held, _docs?} ->\n        :ok"
    assert package_case =~ "|> accepted!(\"Hex package\")\n        |> approved!()"
    assert docs_case =~ "{:held, false} ->\n        " <> @docs
    assert docs_case =~ "{:held, true} ->\n        :ok"
    assert docs_case =~ "|> accepted!(\"Hex docs\")\n        |> approved!()"

    assert docs_case =~
             ~s|read_back!(Hex.API.Release.get("hexpm", "replicant", version), version, digest)|

    assert docs_case =~ ~s|docs_present!(Hex.API.ReleaseDocs.get("hexpm", "replicant", version))|
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

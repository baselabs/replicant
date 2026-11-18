# Load only the script's module, leaving its command-line entry point unexecuted.
source = File.read!(Path.expand("../../scripts/release/publish_hex_artifacts.exs", __DIR__))
{:__block__, _, [module, _entry_point]} = Code.string_to_quoted!(source)
Code.compile_quoted(module)

defmodule Replicant.PublishHexArtifactsTest do
  use ExUnit.Case, async: true

  alias Replicant.PublishHexArtifacts

  @script Path.expand("../../scripts/release/publish_hex_artifacts.exs", __DIR__)
  @digest String.duplicate("a", 64)

  test "only package 200 and release 404 permit publication" do
    assert :ok == PublishHexArtifacts.existing_package!({:ok, {200, [], %{}}})
    assert :ok == PublishHexArtifacts.absent_release!({:ok, {404, [], %{}}})

    for status <- [201, 204, 301, 401, 403, 429, 500, 503] do
      response = {:ok, {status, [], "private-response-value"}}
      assert_raise RuntimeError, fn -> PublishHexArtifacts.existing_package!(response) end
      assert_raise RuntimeError, fn -> PublishHexArtifacts.absent_release!(response) end
    end

    assert_raise RuntimeError, ~r/replicant.*first publication.*owner's decision/, fn ->
      PublishHexArtifacts.existing_package!({:ok, {404, [], %{}}})
    end

    assert_raise RuntimeError, ~r/already has/, fn ->
      PublishHexArtifacts.absent_release!({:ok, {200, [], %{}}})
    end

    for response <- [{:error, :timeout}, nil, {:ok, {"200", [], %{}}}] do
      assert_raise RuntimeError, fn -> PublishHexArtifacts.existing_package!(response) end
      assert_raise RuntimeError, fn -> PublishHexArtifacts.absent_release!(response) end
    end
  end

  test "the uploader refuses to run inside a Mix project before reading anything" do
    assert Mix.Project.get()

    assert_raise RuntimeError, ~r/without a Mix project/, fn ->
      PublishHexArtifacts.main(["0.0.0", "missing-package.tar", "missing-docs.tar.gz"])
    end
  end

  test "checksum read-back accepts only the digest of the uploaded bytes" do
    assert :ok ==
             PublishHexArtifacts.checksum!({:ok, {200, [], %{"checksum" => @digest}}}, @digest)

    for response <- [
          {:ok, {200, [], %{"checksum" => String.duplicate("b", 64)}}},
          {:ok, {200, [], %{}}},
          {:ok, {503, [], %{}}},
          {:error, :timeout}
        ] do
      assert_raise RuntimeError, fn -> PublishHexArtifacts.checksum!(response, @digest) end
    end
  end

  test "uploads refuse failures and ambiguous responses without response values" do
    for response <- [{:ok, {403, [], "private-response-value"}}, {:error, :timeout}] do
      error = assert_raise RuntimeError, fn -> PublishHexArtifacts.accepted!(response, "Hex") end
      refute Exception.message(error) =~ "private-response-value"
    end

    ExUnit.CaptureIO.capture_io(fn ->
      assert :published == PublishHexArtifacts.accepted!({:ok, {201, [], %{}}}, "Hex")

      assert :staged ==
               PublishHexArtifacts.accepted!({:ok, {202, [], %{"status" => "staged"}}}, "Hex")
    end)
  end

  test "the uploader rechecks the name and version immediately before sending the read bytes" do
    source = File.read!(@script)
    assert_isolated_upload!(source)

    for {before, after_change} <- [
          {~s|existing_package!(Hex.API.Package.get("hexpm", "replicant"))|, ":ok"},
          {~s|absent_release!(Hex.API.Release.get("hexpm", "replicant", version))|, ":ok"},
          {"Hex.API.Release.publish(\"hexpm\", package_bytes, auth)", "mix hex.publish"},
          {"Hex.API.ReleaseDocs.publish(\"hexpm\", \"replicant\", version, docs_bytes, auth)",
           "mix docs"}
        ] do
      mutated = String.replace(source, before, after_change)
      refute mutated == source
      assert_raise ExUnit.AssertionError, fn -> assert_isolated_upload!(mutated) end
    end

    for command <- [
          "System.cmd",
          "Code.require_file",
          "Mix.Task.run",
          "Mix.Project",
          "File.write!"
        ] do
      mutated = source <> "\n" <> command
      assert_raise ExUnit.AssertionError, fn -> assert_isolated_upload!(mutated) end
    end
  end

  defp assert_isolated_upload!(source) do
    refute source =~ ~r/System\.cmd|Code\.|Mix\.|File\.write|deps\.get|mix docs|hex\.build/
    assert source =~ "package_bytes = File.read!(archive)"
    assert source =~ "docs_bytes = File.read!(docs)"

    guards =
      ~s|existing_package!(Hex.API.Package.get("hexpm", "replicant"))| <>
        "\n    " <> ~s|absent_release!(Hex.API.Release.get("hexpm", "replicant", version))|

    assert source =~ guards
    [before_upload, _rest] = String.split(source, "Hex.API.Release.publish", parts: 2)
    assert before_upload =~ guards
    assert source =~ ~s|Hex.API.Release.publish("hexpm", package_bytes, auth)|

    assert source =~
             ~s|Hex.API.ReleaseDocs.publish("hexpm", "replicant", version, docs_bytes, auth)|
  end
end

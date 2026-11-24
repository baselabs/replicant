defmodule Replicant.DocsArchiveTest do
  use ExUnit.Case, async: true

  @builder Path.expand("../../scripts/release/build_docs_archive.exs", __DIR__)
  @checker Path.expand("../../scripts/release/check_docs_archive.py", __DIR__)
  @version "1.6.1"
  @commit String.duplicate("c", 40)

  setup do
    scratch = Path.join(System.tmp_dir!(), "replicant-docs-#{System.unique_integer([:positive])}")
    File.mkdir_p!(scratch)
    on_exit(fn -> File.rm_rf!(scratch) end)
    %{scratch: scratch}
  end

  test "Hex archives inspect every rendered byte without comparing docs builds", %{scratch: dir} do
    first = Path.join(dir, "first")
    second = Path.join(dir, "second")

    for {tree, time} <- [{first, 1_000_000_000}, {second, 1_700_000_000}] do
      File.mkdir_p!(Path.join(tree, "assets"))
      File.mkdir_p!(Path.join(tree, ".build"))
      File.write!(Path.join(tree, ".build/html"), tree)

      for name <- ["index.html", "readme.html", "readme.md", "assets/code.js"] do
        path = Path.join(tree, name)
        File.write!(path, "rendered #{name}")
        File.touch!(path, time)
      end
    end

    one = build!(first, dir)
    two = build!(second, dir)
    assert {output, 0} = inspect_archive(one, first)
    assert output =~ "5 regular files; all bytes match the source tree"
    assert {_, 0} = inspect_archive(two, second)

    File.write!(Path.join(first, "readme.html"), "mutated rendered bytes")
    assert {output, 1} = inspect_archive(one, first)
    assert output =~ "bytes differ from source"

    File.write!(Path.join(second, "missing.md"), "not archived")
    assert {output, 1} = inspect_archive(two, second)
    assert output =~ "omits source files"
  end

  test "EPUB is refused even when its bytes match the rendered tree", %{scratch: dir} do
    tree = Path.join(dir, "docs")
    File.mkdir_p!(tree)
    File.write!(Path.join(tree, "index.html"), "index")
    File.write!(Path.join(tree, "readme.html"), "readme")
    File.write!(Path.join(tree, "replicant.epub"), "timestamped EPUB")
    archive = build!(tree, dir)
    assert {output, 1} = inspect_archive(archive, tree)
    assert output =~ "EPUB embeds build time"

    checker = File.read!(@checker)
    mutated = String.replace(checker, ~s|path.suffix == ".epub"|, "False")
    refute mutated == checker

    # This exact mutation must admit the otherwise valid EPUB archive.
    assert {_output, 0} =
             System.cmd("python3", ["-I", "-c", mutated, archive, tree, @version, @commit],
               stderr_to_stdout: true
             )
  end

  test "inspection requires the exact release provenance fields", %{scratch: dir} do
    Mix.Local.append_archives()
    Code.ensure_loaded!(:mix_hex_tarball)
    tree = Path.join(dir, "docs")
    File.mkdir!(tree)
    File.write!(Path.join(tree, "index.html"), "index")
    File.write!(Path.join(tree, "readme.html"), "readme")
    archive = build!(tree, dir)
    assert {_, 0} = inspect_archive(archive, tree)

    for metadata <- [
          %{"package" => "other", "version" => @version, "commit" => @commit},
          %{"package" => "replicant", "version" => "0.0.0", "commit" => @commit},
          %{"package" => "replicant", "version" => @version, "commit" => "HEAD"},
          %{"package" => "replicant", "version" => @version, "commit" => @commit, "extra" => true}
        ] do
      File.write!(Path.join(tree, "release.json"), :json.encode(metadata))
      repack!(tree, archive)
      assert {output, 1} = inspect_archive(archive, tree)
      assert output =~ "release.json must name"
    end

    File.rm!(Path.join(tree, "release.json"))
    repack!(tree, archive)
    assert {output, 1} = inspect_archive(archive, tree)
    assert output =~ "requires top-level"
  end

  defp build!(tree, scratch) do
    archive = tree <> ".tar.gz"

    assert {_output, 0} =
             System.cmd(
               "mix",
               ["run", "--no-mix-exs", @builder, tree, archive, @version, @commit],
               cd: scratch,
               stderr_to_stdout: true
             )

    archive
  end

  defp inspect_archive(archive, tree) do
    System.cmd("python3", ["-I", @checker, archive, tree, @version, @commit],
      stderr_to_stdout: true
    )
  end

  defp repack!(tree, archive) do
    files =
      tree
      |> Path.join("**/*")
      |> Path.wildcard()
      |> Enum.filter(&File.regular?/1)
      |> Enum.map(&{String.to_charlist(Path.relative_to(&1, tree)), File.read!(&1)})

    {:ok, bytes} = :mix_hex_tarball.create_docs(files)
    File.write!(archive, bytes)
  end
end

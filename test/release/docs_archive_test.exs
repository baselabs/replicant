defmodule Replicant.DocsArchiveTest do
  use ExUnit.Case, async: true

  @builder Path.expand("../../scripts/release/build_docs_archive.exs", __DIR__)
  @checker Path.expand("../../scripts/release/check_docs_archive.py", __DIR__)

  setup do
    scratch = Path.join(System.tmp_dir!(), "replicant-docs-#{System.unique_integer([:positive])}")
    File.mkdir_p!(scratch)
    on_exit(fn -> File.rm_rf!(scratch) end)
    %{scratch: scratch}
  end

  test "Hex archives ignore source times and inspect every rendered byte", %{scratch: dir} do
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
    assert File.read!(one) == File.read!(two)
    assert {output, 0} = inspect_archive(one, first)
    assert output =~ "4 regular files; all bytes match the source tree"

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
             System.cmd("python3", ["-I", "-c", mutated, archive, tree], stderr_to_stdout: true)
  end

  defp build!(tree, scratch) do
    archive = tree <> ".tar.gz"

    assert {_output, 0} =
             System.cmd("mix", ["run", "--no-mix-exs", @builder, tree, archive],
               cd: scratch,
               stderr_to_stdout: true
             )

    archive
  end

  defp inspect_archive(archive, tree) do
    System.cmd("python3", ["-I", @checker, archive, tree], stderr_to_stdout: true)
  end
end

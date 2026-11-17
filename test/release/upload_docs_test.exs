Code.require_file("../../scripts/release/package_witness.exs", __DIR__)

defmodule Replicant.UploadDocsTest do
  use ExUnit.Case, async: true

  alias Replicant.PackageWitness

  @repo_root Path.expand("../..", __DIR__)

  setup do
    root = Path.join(System.tmp_dir!(), "replicant-docs-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    scripts = Path.join(root, "scripts/release")
    artifacts = Path.join(root, ".kimosabe/artifacts")
    File.mkdir_p!(scripts)
    File.mkdir_p!(artifacts)

    for name <- ["upload_docs.exs", "package_witness.exs"] do
      File.cp!(Path.join([@repo_root, "scripts", "release", name]), Path.join(scripts, name))
    end

    File.write!(Path.join(root, "mix.exs"), """
    defmodule DocsFixture.MixProject do
      use Mix.Project
      @version "1.2.0"
      def project, do: [app: :docs_fixture, version: @version]
    end
    """)

    File.write!(Path.join(root, ".gitignore"), ".kimosabe/\n_build/\n")
    git!(root, ["init", "-q"])
    git!(root, ["config", "user.email", "test@example.invalid"])
    git!(root, ["config", "user.name", "Replicant Test"])
    git!(root, ["add", "mix.exs", ".gitignore", "scripts/release/upload_docs.exs"])
    git!(root, ["commit", "-qm", "docs source"])
    commit = git!(root, ["rev-parse", "HEAD"])
    artifact = Path.join(artifacts, "replicant-1.2.0.tar")
    File.write!(artifact, "synthetic artifact bytes")
    digest = :crypto.hash(:sha256, File.read!(artifact)) |> Base.encode16(case: :lower)
    receipt = Path.join(artifacts, "replicant-1.2.0-receipt.txt")
    File.write!(receipt, "version: 1.2.0\nsource_commit: #{commit}\nsha256: #{digest}\n")
    ref = "refs/attestations/packages/replicant/1.2.0"
    PackageWitness.create!(root, ref, commit, receipt)
    assert :ok == PackageWitness.verify(root, ref, artifact, receipt)
    File.chmod!(artifact, 0o444)
    File.chmod!(receipt, 0o444)
    %{root: root, artifact: artifact, receipt: receipt}
  end

  test "docs refuse a rewritten receipt before rendering or credential access", ctx do
    File.chmod!(ctx.receipt, 0o644)
    File.write!(ctx.receipt, "source_commit: #{String.duplicate("a", 40)}\n")
    File.chmod!(ctx.receipt, 0o444)
    {output, status} = upload_docs(ctx.root)
    assert status == 1
    assert output =~ "receipt does not match immutable witness"
    refute output =~ "HEX_API_KEY"
  end

  test "docs refuse artifact tampering against the same witness", ctx do
    File.chmod!(ctx.artifact, 0o644)
    File.write!(ctx.artifact, "changed artifact bytes")
    File.chmod!(ctx.artifact, 0o444)
    {output, status} = upload_docs(ctx.root)
    assert status == 1
    assert output =~ "candidate artifact digest does not match witnessed receipt"
    refute output =~ "HEX_API_KEY"
  end

  defp upload_docs(root) do
    System.cmd("mix", ["run", "--no-start", "scripts/release/upload_docs.exs", "--publish"],
      cd: root,
      env: [{"HEX_API_KEY", nil}, {"REPLICANT_PUBLISH_AUTHORIZED", nil}],
      stderr_to_stdout: true
    )
  end

  defp git!(root, args) do
    {output, 0} = System.cmd("git", args, cd: root)
    String.trim(output)
  end
end

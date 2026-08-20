Code.require_file("../../scripts/release/package_witness.exs", __DIR__)

defmodule Replicant.PackageWitnessTest do
  use ExUnit.Case, async: false

  alias Replicant.PackageWitness

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "replicant-package-witness-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    git!(root, ["init", "-q"])
    git!(root, ["config", "user.email", "test@example.invalid"])
    git!(root, ["config", "user.name", "Replicant Test"])

    File.write!(Path.join(root, "source"), "one\n")
    git!(root, ["add", "source"])
    git!(root, ["commit", "-q", "-m", "one"])
    first = git!(root, ["rev-parse", "HEAD"])

    File.write!(Path.join(root, "source"), "two\n")
    git!(root, ["commit", "-q", "-am", "two"])
    second = git!(root, ["rev-parse", "HEAD"])

    artifact = Path.join(root, "artifact.tar")
    File.write!(artifact, "synthetic artifact bytes")
    digest = sha256(File.read!(artifact))
    receipt = Path.join(root, "receipt.txt")
    File.write!(receipt, receipt_body(first, digest))

    on_exit(fn -> File.rm_rf!(root) end)

    %{
      root: root,
      first: first,
      second: second,
      artifact: artifact,
      digest: digest,
      receipt: receipt
    }
  end

  test "witness binds the exact receipt and source commit", ctx do
    ref = "refs/attestations/packages/replicant/1.2.0"
    assert :ok == PackageWitness.create(ctx.root, ref, ctx.first, ctx.receipt)
    assert :ok == PackageWitness.verify(ctx.root, ref, ctx.artifact, ctx.receipt)

    File.write!(ctx.receipt, receipt_body(ctx.second, ctx.digest))

    assert {:error, message} = PackageWitness.verify(ctx.root, ref, ctx.artifact, ctx.receipt)
    assert message =~ "receipt does not match immutable witness"
  end

  test "every retained copy must match the candidate digest", ctx do
    backup = Path.join(ctx.root, "backup.tar")
    by_digest = Path.join(ctx.root, "by-digest.tar")
    File.cp!(ctx.artifact, backup)
    File.cp!(ctx.artifact, by_digest)
    File.write!(backup, "corrupt")

    assert {:error, message} =
             PackageWitness.verify_copies([ctx.artifact, backup, by_digest], ctx.digest)

    assert message =~ "retained copy digest mismatch"
    assert message =~ backup
  end

  test "retained copies refuse to overwrite an existing destination", ctx do
    fresh = Path.join(ctx.root, "fresh.tar")
    destination = Path.join(ctx.root, "retained.tar")
    File.write!(destination, "existing")

    assert_raise RuntimeError, ~r/refusing to overwrite retained package copy/, fn ->
      PackageWitness.retain_copies!(ctx.artifact, [fresh, destination])
    end

    refute File.exists?(fresh)
    assert File.read!(destination) == "existing"
  end

  test "witness creation rejects a receipt for a different source commit", ctx do
    File.write!(ctx.receipt, receipt_body(ctx.second, ctx.digest))

    assert {:error, "receipt source commit does not match witness parent"} =
             PackageWitness.create(
               ctx.root,
               "refs/attestations/packages/replicant/1.2.0",
               ctx.first,
               ctx.receipt
             )
  end

  test "uploader inputs must be read-only regular files, never symlinks", ctx do
    link = Path.join(ctx.root, "artifact-link.tar")
    File.ln_s!(ctx.artifact, link)

    assert {:error, message} = PackageWitness.read_immutable(link)
    assert message =~ "regular file"

    assert {:error, message} = PackageWitness.read_immutable(ctx.artifact)
    assert message =~ "read-only"

    File.chmod!(ctx.artifact, 0o444)
    assert {:ok, "synthetic artifact bytes"} = PackageWitness.read_immutable(ctx.artifact)
  end

  defp git!(root, args) do
    {output, 0} = System.cmd("git", args, cd: root)
    String.trim(output)
  end

  defp receipt_body(commit, digest) do
    "version: 1.2.0\nsource_commit: #{commit}\nsha256: #{digest}\n"
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end

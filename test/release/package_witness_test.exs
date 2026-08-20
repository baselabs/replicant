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
    System.cmd("git", ["init", "-q"], cd: root)
    System.cmd("git", ["config", "user.email", "test@example.invalid"], cd: root)
    System.cmd("git", ["config", "user.name", "Replicant Test"], cd: root)

    File.write!(Path.join(root, "source"), "one\n")
    System.cmd("git", ["add", "source"], cd: root)
    System.cmd("git", ["commit", "-q", "-m", "one"], cd: root)
    first = git!(root, ["rev-parse", "HEAD"])

    File.write!(Path.join(root, "source"), "two\n")
    System.cmd("git", ["commit", "-q", "-am", "two"], cd: root)
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

  defp git!(root, args) do
    {output, 0} = System.cmd("git", args, cd: root)
    String.trim(output)
  end

  defp receipt_body(commit, digest) do
    "version: 1.2.0\nsource_commit: #{commit}\nsha256: #{digest}\n"
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end

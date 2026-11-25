# Keyless read-back after upload: retain only served archives and their provenance.
Code.require_file("release_docs.exs", __DIR__)

defmodule Replicant.ReleaseAssets do
  @moduledoc false

  def main([version, commit, digest, repository, branch, output]) do
    if Mix.Project.get(), do: raise("release read-back must run without a Mix project")
    Hex.start()

    package = served!(Hex.Repo.get_tarball("hexpm", "replicant", version))
    if sha256(package) != digest, do: raise("served package differs from inspected package")

    docs = served!(Hex.Repo.get_docs("hexpm", "replicant", version))
    published = retain!(package, docs, version, output, commit, repository, branch)
    File.write!(System.fetch_env!("GITHUB_OUTPUT"), "published_commit=#{published}\n", [:append])
    IO.puts("release read-back: release_commit=#{commit} published_commit=#{published}")
  end

  def retain!(package, docs, version, output, release_commit, repository, branch) do
    published = Replicant.ReleaseDocs.provenance!(docs, version)
    Replicant.ReleaseDocs.verify_history!(published, release_commit, repository, branch)
    package_digest = sha256(package)
    docs_digest = sha256(docs)

    # Nothing is retained until the package, docs, and history all pass.
    File.mkdir!(output)
    package_name = "replicant-#{version}.tar"
    docs_name = "replicant-#{version}-docs.tar.gz"
    File.write!(Path.join(output, package_name), package)
    File.write!(Path.join(output, docs_name), docs)

    File.write!(
      Path.join(output, "SHA256SUMS"),
      "#{package_digest}  #{package_name}\n#{docs_digest}  #{docs_name}\n"
    )

    File.write!(Path.join(output, "published_commit"), published <> "\n")
    published
  end

  defp served!({:ok, {200, _headers, bytes}}) when is_binary(bytes), do: bytes
  defp served!(_response), do: raise("served release archive is unavailable; read-back refused")
  defp sha256(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
end

Replicant.ReleaseAssets.main(System.argv())

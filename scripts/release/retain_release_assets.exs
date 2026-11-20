# Keyless read-back after upload: retain only served archives and their provenance.
Code.require_file("release_docs.exs", __DIR__)

defmodule Replicant.ReleaseAssets do
  @moduledoc false

  def main([version, archive, output]) do
    if Mix.Project.get(), do: raise("release read-back must run without a Mix project")
    if System.get_env("HEX_API_KEY"), do: raise("release read-back must run without a key")
    Hex.start()

    package = served!(Hex.Repo.get_tarball("hexpm", "replicant", version))
    digest = sha256(File.read!(archive))
    if sha256(package) != digest, do: raise("served package differs from inspected package")

    case Hex.API.Release.get(nil, "replicant", version) do
      {:ok, {200, _headers, %{"version" => ^version, "checksum" => ^digest,
                            "retirement" => nil, "has_docs" => true}}} ->
        :ok

      _other ->
        raise "served release identity is unavailable, retired, or different; read-back refused"
    end

    docs = served!(Hex.Repo.get_docs("hexpm", "replicant", version))
    published = retain!(package, docs, version, output,
                        System.fetch_env!("GITHUB_SHA"), System.fetch_env!("GITHUB_WORKSPACE"))
    File.write!(System.fetch_env!("GITHUB_OUTPUT"), "published_commit=#{published}\n", [:append])
    IO.puts("release read-back: published_commit=#{published}")
  end

  def retain!(package, docs, version, output, dispatched, repository) do
    published = Replicant.ReleaseDocs.provenance!(docs, version)
    verify_history!(published, dispatched, repository)

    # Nothing is retained until the package, docs, and history all pass read-back.
    File.mkdir!(output)
    package_name = "replicant-#{version}.tar"
    docs_name = "replicant-#{version}-docs.tar.gz"
    File.write!(Path.join(output, package_name), package)
    File.write!(Path.join(output, docs_name), docs)

    File.write!(
      Path.join(output, "SHA256SUMS"),
      "#{sha256(package)}  #{package_name}\n#{sha256(docs)}  #{docs_name}\n"
    )

    File.write!(Path.join(output, "published_commit"), published <> "\n")
    published
  end

  def verify_history!(published, dispatched, repository) do
    valid? = Enum.all?([published, dispatched], &Regex.match?(~r/\A[0-9a-f]{40}\z/, &1))

    with true <- valid?,
         {"false\n", 0} <- git(repository, ["rev-parse", "--is-shallow-repository"]),
         {_output, 0} <-
           git(repository, ["merge-base", "--is-ancestor", dispatched, "refs/remotes/origin/main"]),
         {_output, 0} <- git(repository, ["merge-base", "--is-ancestor", published, dispatched]) do
      :ok
    else
      _other -> raise "docs commit is not on the dispatched default-branch history; read-back refused"
    end
  end

  defp git(repository, args), do: System.cmd("git", ["-C", repository | args], stderr_to_stdout: true)

  defp served!({:ok, {200, _headers, bytes}}) when is_binary(bytes), do: bytes
  defp served!(_response), do: raise("served release archive is unavailable; read-back refused")
  defp sha256(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
end

Replicant.ReleaseAssets.main(System.argv())

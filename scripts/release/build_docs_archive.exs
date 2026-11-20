unless is_nil(Mix.Project.get()) do
  Mix.raise("Docs archives must be built outside a Mix project")
end

[directory, output, version, commit] = System.argv()
directory = Path.expand(directory)

Code.require_file("release_docs.exs", __DIR__)
metadata = :json.encode(%{"package" => "replicant", "version" => version, "commit" => commit})
Replicant.ReleaseDocs.fields!(IO.iodata_to_binary(metadata), version)
# Render first, then bind this archive to the CI source before any inspection.
File.write!(Path.join(directory, "release.json"), metadata)

unless File.regular?(Path.join(directory, "index.html")) do
  Mix.raise("Documentation must contain a top-level index.html")
end

Hex.start()
Code.ensure_loaded!(:mix_hex_tarball)

# The same listing mix hex.publish makes: Path.wildcard/1 matches no file or
# directory whose name starts with a dot, so ExDoc's .build manifests stay out.
files =
  directory
  |> Path.join("**")
  |> Path.wildcard()
  |> Enum.sort()
  |> Enum.filter(&File.regular?/1)
  |> Enum.map(fn path ->
    name = Path.relative_to(path, directory)
    top_level = name |> Path.split() |> List.first()

    if match?({:ok, _}, Version.parse(top_level)) do
      Mix.raise("Top-level documentation filenames cannot be semantic versions")
    end

    {String.to_charlist(name), File.read!(path)}
  end)

# Hex 2.7.0 sorts entries, fixes times to 2000-01-01 and uid/gid to 0,
# leaves owner/group names empty, and emits gzip without a filename or mtime.
{:ok, data} = :mix_hex_tarball.create_docs(files)
File.write!(output, data)
IO.puts("Docs archive: #{output}")

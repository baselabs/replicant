# Shared by the pre-build refusal and keyless read-back. Archives stay in memory.
defmodule Replicant.ReleaseNotes do
  @moduledoc false

  def verify_changelog!(mixfile, changelog) do
    case Regex.scan(~r/^  @version "([^"]+)"$/m, File.read!(mixfile)) do
      [[_, version]] -> section!(File.read!(changelog), version)
      _other -> raise "expected exactly one Mix project version; publication refused"
    end
  end

  def section!(changelog, version) do
    pattern = ~r/^## \[#{Regex.escape(version)}\][^\n]*(?:\n|\z).*?(?=^## |\z)/ms

    case Regex.scan(pattern, changelog) do
      [[section]] ->
        [_heading | body] = String.split(section, "\n", parts: 2)
        body = Regex.replace(~r/^\s*#+[^\n]*$/m, Enum.join(body, "\n"), "")

        if String.trim(body) == "", do: invalid_section!(version)

        String.trim(section)

      _other ->
        invalid_section!(version)
    end
  end

  def package_section!(package, version) do
    package
    |> member!(~c"contents.tar.gz", [])
    |> member!(~c"CHANGELOG.md", [:compressed])
    |> section!(version)
  end

  defp member!(archive, name, options) do
    with {:ok, names} <- :erl_tar.table({:binary, archive}, options),
         1 <- Enum.count(names, &(&1 == name)),
         {:ok, [{^name, bytes}]} <-
           :erl_tar.extract({:binary, archive}, [:memory, {:files, [name]} | options]) do
      bytes
    else
      _other -> raise "Hex package changelog is missing or malformed; publication refused"
    end
  rescue
    _error -> raise "Hex package changelog is missing or malformed; publication refused"
  end

  defp invalid_section!(version) do
    raise "the changelog section for #{version} is missing, duplicated or empty; publication refused"
  end
end

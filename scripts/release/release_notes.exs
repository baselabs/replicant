# Refuse an absent, duplicated or empty version section before building.
defmodule Replicant.ReleaseNotes do
  @moduledoc false

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

  defp invalid_section!(version) do
    raise "the changelog section for #{version} is missing, duplicated or empty; publication refused"
  end
end

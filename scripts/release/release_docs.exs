# Standalone archive inspection shared by the keyless checks and isolated uploader.
# Uses only OTP; it loads no Mix project or package code and extracts only to memory.
defmodule Replicant.ReleaseDocs do
  @moduledoc false

  def files!(<<0x1F, 0x8B, _rest::binary>> = bytes) do
    with {:ok, names} <- :erl_tar.table({:binary, bytes}, [:compressed]),
         true <- Enum.all?(names, &safe_name?/1),
         true <- length(names) == MapSet.size(MapSet.new(names)),
         {:ok, files} <- :erl_tar.extract({:binary, bytes}, [:compressed, :memory]),
         true <- length(files) == length(names),
         true <- List.keymember?(files, ~c"index.html", 0) do
      files
    else
      _other -> raise "Hex docs archive is malformed; publication refused"
    end
  rescue
    _error -> raise "Hex docs archive is malformed; publication refused"
  end

  def files!(_bytes), do: raise("Hex docs archive is malformed; publication refused")

  def provenance!(bytes, version) do
    files = files!(bytes)

    case List.keyfind(files, ~c"release.json", 0) do
      {_name, metadata} -> fields!(metadata, version)
      nil -> raise "Hex docs archive has no release.json; publication refused"
    end
  end

  def fields!(json, version) do
    case :json.decode(json) do
      %{"package" => "replicant", "version" => ^version, "commit" => commit} = fields
      when map_size(fields) == 3 and is_binary(commit) ->
        if Regex.match?(~r/\A[0-9a-f]{40}\z/, commit), do: commit, else: invalid_fields!()

      _other ->
        invalid_fields!()
    end
  rescue
    _error -> invalid_fields!()
  end

  defp invalid_fields!, do: raise("Hex docs release.json is invalid; publication refused")

  defp safe_name?(name) do
    name = to_string(name)

    Path.type(name) == :relative and not String.contains?(name, "\\") and
      Enum.all?(String.split(name, "/"), &(&1 not in ["", ".", ".."]))
  end
end

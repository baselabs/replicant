defmodule Replicant.Telemetry do
  @moduledoc """
  Value-free `:telemetry.span/3` wrapper. Owns the metadata allowlist — the single
  enforcement point for "no row values in telemetry" (Critical Rule 1). LSNs,
  table names, counts, durations, and error classes are permitted; an
  off-allowlist key raises rather than shipping a value downstream. Mirrors
  `arcadic`'s allowlist pattern.
  """

  @allowed_meta_keys ~w(commit_lsn change_count byte_size lag_ms duration table reason error_class kind slot_name)a

  @doc "The permitted span metadata keys."
  @spec allowed_meta_keys() :: [atom()]
  def allowed_meta_keys, do: @allowed_meta_keys

  @spec span(atom(), map(), (-> {term(), map()})) :: term()
  def span(op, start_meta, fun) when is_atom(op) and is_map(start_meta) and is_function(fun, 0) do
    :telemetry.span([:replicant, op], validate!(start_meta), fn ->
      {result, stop_meta} = fun.()
      {result, validate!(Map.merge(start_meta, stop_meta))}
    end)
  end

  @spec event([atom(), ...], map(), map()) :: :ok
  def event(name, measurements, meta)
      when is_list(name) and is_map(measurements) and is_map(meta) do
    :telemetry.execute(name, measurements, validate!(meta))
  end

  @doc false
  @spec validate!(map()) :: map()
  def validate!(meta) when is_map(meta) do
    case Map.keys(meta) -- @allowed_meta_keys do
      [] ->
        meta

      bad ->
        raise ArgumentError,
              "telemetry metadata keys #{inspect(bad)} are not in the value-free allowlist " <>
                "#{inspect(@allowed_meta_keys)} (no row values / column values in telemetry)"
    end
  end
end

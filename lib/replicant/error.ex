defmodule Replicant.Error do
  @moduledoc """
  A typed, value-free error.

  `reason` is a stable atom; `table`, `lsn`, and `shape` carry structure only.
  No row value, parameter, or decoded byte ever reaches this struct — the decode
  boundary (`Replicant.Decoder.decode/1`) and the identifier validator
  (`Replicant.Identifier`) are the enforcement points. `message/1` and the
  derived `Inspect` render only structural fields (Critical Rule 1).
  """

  @type reason ::
          :decode_failure
          | :unsupported_message
          | :invalid_identifier
          | :sink_failed
          | :schema_change_destructive
          | :slot_invalidated
          | :config_invalid
          | :snapshot_failed
          | :checkpoint_store_failed
          | :checkpoint_store_schema_mismatch

  @type t :: %__MODULE__{
          reason: reason(),
          table: String.t() | nil,
          lsn: Replicant.lsn() | nil,
          shape: String.t() | nil,
          message: String.t() | nil
        }

  defexception [:reason, :table, :lsn, :shape, :message]

  @doc """
  Build a value-free `:decode_failure` from any exception the vendored parser
  raised. The original message (which may embed raw bytes) is **discarded**; only
  the exception's module name is kept on `:shape` for triage.
  """
  @spec decode_failure(Exception.t()) :: t()
  def decode_failure(%{__struct__: mod} = exception) when is_exception(exception) do
    %__MODULE__{reason: :decode_failure, shape: inspect(mod)}
  end

  @impl true
  @spec message(t()) :: String.t()
  def message(%__MODULE__{reason: reason, table: table, lsn: lsn, shape: shape}) do
    parts =
      [
        "replicant error",
        "reason=#{reason}",
        table && "table=#{table}",
        lsn && "lsn=#{Replicant.lsn_to_string(lsn)}",
        shape && "shape=#{shape}"
      ]
      |> Enum.reject(&is_nil/1)

    Enum.join(parts, " ")
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(err, opts) do
      # Render ONLY structural fields; `message` (which a careless caller could
      # load with a value) is deliberately omitted from the default inspect.
      safe = Map.take(err, [:reason, :table, :lsn, :shape])
      concat(["#Replicant.Error<", to_doc(safe, opts), ">"])
    end
  end
end

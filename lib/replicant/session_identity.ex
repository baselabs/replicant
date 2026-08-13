defmodule Replicant.SessionIdentity do
  @moduledoc """
  Identity reported by PostgreSQL's `IDENTIFY_SYSTEM` command on the exact
  replication connection that will issue `START_REPLICATION`.

  This is the authoritative source boundary for a sink that binds durable
  checkpoint state to a PostgreSQL system and database. It is discovered again
  on every reconnect. Do not substitute values read from a separate connection.
  """

  @enforce_keys [:system_identifier, :timeline_id, :current_lsn, :database]
  defstruct [:system_identifier, :timeline_id, :current_lsn, :database]

  @type t :: %__MODULE__{
          system_identifier: String.t(),
          timeline_id: non_neg_integer(),
          current_lsn: Replicant.lsn(),
          database: String.t()
        }

  @max_uint32 0xFFFFFFFF
  @max_int64 0x7FFFFFFFFFFFFFFF
  @max_uint64 0xFFFFFFFFFFFFFFFF

  @doc false
  @spec from_result(Postgrex.Result.t()) :: {:ok, t()} | {:error, :invalid_session_identity}
  def from_result(%Postgrex.Result{
        rows: [[system_identifier, timeline, current_lsn, database]]
      })
      when is_binary(system_identifier) and is_binary(database) and database != "" do
    with {:ok, _system_id} <- parse_bounded_integer(system_identifier, 10, @max_uint64),
         {:ok, timeline_id} <- parse_bounded_integer(timeline, 10, @max_int64),
         {:ok, parsed_lsn} <- parse_lsn(current_lsn) do
      {:ok,
       %__MODULE__{
         system_identifier: system_identifier,
         timeline_id: timeline_id,
         current_lsn: parsed_lsn,
         database: database
       }}
    else
      _ -> {:error, :invalid_session_identity}
    end
  end

  def from_result(_result), do: {:error, :invalid_session_identity}

  defp parse_bounded_integer(value, _base, max)
       when is_integer(value) and value >= 0 and value <= max,
       do: {:ok, value}

  defp parse_bounded_integer(value, base, max) when is_binary(value) do
    case Integer.parse(value, base) do
      {parsed, ""} when parsed >= 0 and parsed <= max -> {:ok, parsed}
      _ -> :error
    end
  end

  defp parse_bounded_integer(_value, _base, _max), do: :error

  defp parse_lsn(value) when is_binary(value) do
    with [file, offset] <- String.split(value, "/", parts: 2),
         {:ok, file_number} <- parse_bounded_integer(file, 16, @max_uint32),
         {:ok, offset_number} <- parse_bounded_integer(offset, 16, @max_uint32) do
      {:ok, Bitwise.bsl(file_number, 32) + offset_number}
    else
      _ -> :error
    end
  end

  defp parse_lsn(_value), do: :error
end

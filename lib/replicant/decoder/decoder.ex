# This file steals liberally from https://github.com/supabase/realtime,
# which in turn draws on https://github.com/cainophile/pgoutput_decoder/blob/master/lib/pgoutput_decoder.ex

defmodule Replicant.Decoder do
  @moduledoc """
  Binary decoder for the Postgres logical-replication `pgoutput` stream.

  The byte parser (`decode_message/1`) is vendored from `walex` (MIT; see NOTICE),
  itself derived from `cainophile` and `supabase-realtime`. It raises on malformed
  input and produces `%Unsupported{data: binary}` (carrying raw bytes) for unknown
  message types.

  **Callers MUST use `decode/1`, not `decode_message/1`.** `decode/1` is the
  value-free-error boundary (Critical Rule 1): it catches every raise, scrubs the
  raw bytes, and normalises `%Unsupported{}` into a value-free
  `{:error, %Replicant.Error{reason: :unsupported_message}}`. No row value or raw
  WAL byte ever escapes the decode boundary.
  """

  alias Replicant.Error

  alias Replicant.Decoder.Messages.{
    Begin,
    Commit,
    Delete,
    Insert,
    Origin,
    Relation,
    Truncate,
    Type,
    Unsupported,
    Update
  }

  alias Replicant.Decoder.Messages.Relation.Column

  alias Replicant.Decoder.OidDatabase

  @pg_epoch DateTime.from_iso8601("2000-01-01T00:00:00Z")

  @doc """
  Decode one pgoutput message, value-free on failure.

  Returns `{:ok, message_struct}` on success, or
  `{:error, %Error{reason: :decode_failure | :unsupported_message}}` — never
  raises, never surfaces raw bytes.
  """
  @spec decode(binary()) :: {:ok, struct()} | {:error, Error.t()}
  def decode(binary) when is_binary(binary) do
    case decode_message(binary) do
      %Unsupported{} -> {:error, %Error{reason: :unsupported_message}}
      message -> {:ok, message}
    end
  rescue
    exception -> {:error, Error.decode_failure(exception)}
  end

  # The raw vendored parser. Private: product code and tests reach pgoutput
  # exclusively through `decode/1` (the value-free boundary). Keeping this private
  # is part of the boundary — there is no way to bypass the scrub.
  defp decode_message(message) when is_binary(message) do
    decode_message_impl(message)
  end

  defp decode_message_impl(<<"B", lsn::binary-8, timestamp::integer-64, xid::integer-32>>) do
    %Begin{
      final_lsn: decode_lsn(lsn),
      commit_timestamp: pgtimestamp_to_timestamp(timestamp),
      xid: xid
    }
  end

  defp decode_message_impl(
         <<"C", _flags::binary-1, lsn::binary-8, end_lsn::binary-8, timestamp::integer-64>>
       ) do
    %Commit{
      flags: [],
      lsn: decode_lsn(lsn),
      end_lsn: decode_lsn(end_lsn),
      commit_timestamp: pgtimestamp_to_timestamp(timestamp)
    }
  end

  defp decode_message_impl(<<"O", lsn::binary-8, name::binary>>) do
    %Origin{
      origin_commit_lsn: decode_lsn(lsn),
      name: name
    }
  end

  defp decode_message_impl(<<"R", id::integer-32, rest::binary>>) do
    [
      namespace
      | [name | [<<replica_identity::binary-1, _number_of_columns::integer-16, columns::binary>>]]
    ] = String.split(rest, <<0>>, parts: 3)

    friendly_replica_identity =
      case replica_identity do
        "d" -> :default
        "n" -> :nothing
        "f" -> :all_columns
        "i" -> :index
      end

    %Relation{
      id: id,
      namespace: namespace,
      name: name,
      replica_identity: friendly_replica_identity,
      columns: decode_columns(columns)
    }
  end

  defp decode_message_impl(
         <<"I", relation_id::integer-32, "N", number_of_columns::integer-16, tuple_data::binary>>
       ) do
    {<<>>, decoded_tuple_data} = decode_tuple_data(tuple_data, number_of_columns)

    %Insert{
      relation_id: relation_id,
      tuple_data: decoded_tuple_data
    }
  end

  defp decode_message_impl(
         <<"U", relation_id::integer-32, "N", number_of_columns::integer-16, tuple_data::binary>>
       ) do
    {<<>>, decoded_tuple_data} = decode_tuple_data(tuple_data, number_of_columns)

    %Update{
      relation_id: relation_id,
      tuple_data: decoded_tuple_data
    }
  end

  defp decode_message_impl(
         <<"U", relation_id::integer-32, key_or_old::binary-1, number_of_columns::integer-16,
           tuple_data::binary>>
       )
       when key_or_old == "O" or key_or_old == "K" do
    {<<"N", new_number_of_columns::integer-16, new_tuple_binary::binary>>, old_decoded_tuple_data} =
      decode_tuple_data(tuple_data, number_of_columns)

    {<<>>, decoded_tuple_data} = decode_tuple_data(new_tuple_binary, new_number_of_columns)

    base_update_msg = %Update{
      relation_id: relation_id,
      tuple_data: decoded_tuple_data
    }

    case key_or_old do
      "K" -> Map.put(base_update_msg, :changed_key_tuple_data, old_decoded_tuple_data)
      "O" -> Map.put(base_update_msg, :old_tuple_data, old_decoded_tuple_data)
    end
  end

  defp decode_message_impl(
         <<"D", relation_id::integer-32, key_or_old::binary-1, number_of_columns::integer-16,
           tuple_data::binary>>
       )
       when key_or_old == "K" or key_or_old == "O" do
    {<<>>, decoded_tuple_data} = decode_tuple_data(tuple_data, number_of_columns)

    base_delete_msg = %Delete{
      relation_id: relation_id
    }

    case key_or_old do
      "K" -> Map.put(base_delete_msg, :changed_key_tuple_data, decoded_tuple_data)
      "O" -> Map.put(base_delete_msg, :old_tuple_data, decoded_tuple_data)
    end
  end

  defp decode_message_impl(
         <<"T", number_of_relations::integer-32, options::integer-8, column_ids::binary>>
       ) do
    truncated_relations =
      for relation_id_bin <- column_ids |> :binary.bin_to_list() |> Enum.chunk_every(4),
          do: relation_id_bin |> :binary.list_to_bin() |> :binary.decode_unsigned()

    decoded_options =
      case options do
        0 -> []
        1 -> [:cascade]
        2 -> [:restart_identity]
        3 -> [:cascade, :restart_identity]
      end

    %Truncate{
      number_of_relations: number_of_relations,
      options: decoded_options,
      truncated_relations: truncated_relations
    }
  end

  defp decode_message_impl(<<"Y", data_type_id::integer-32, namespace_and_name::binary>>) do
    [namespace, name_with_null] = :binary.split(namespace_and_name, <<0>>)
    name = String.slice(name_with_null, 0..-2//1)

    %Type{
      id: data_type_id,
      namespace: namespace,
      name: name
    }
  end

  defp decode_message_impl(binary), do: %Unsupported{data: binary}

  defp decode_tuple_data(binary, columns_remaining, accumulator \\ [])

  defp decode_tuple_data(remaining_binary, 0, accumulator) when is_binary(remaining_binary),
    do: {remaining_binary, accumulator |> Enum.reverse() |> List.to_tuple()}

  defp decode_tuple_data(<<"n", rest::binary>>, columns_remaining, accumulator),
    do: decode_tuple_data(rest, columns_remaining - 1, [nil | accumulator])

  defp decode_tuple_data(<<"u", rest::binary>>, columns_remaining, accumulator),
    do: decode_tuple_data(rest, columns_remaining - 1, [:unchanged_toast | accumulator])

  defp decode_tuple_data(
         <<"t", column_length::integer-32, rest::binary>>,
         columns_remaining,
         accumulator
       ),
       do:
         decode_tuple_data(
           :erlang.binary_part(rest, {byte_size(rest), -(byte_size(rest) - column_length)}),
           columns_remaining - 1,
           [:erlang.binary_part(rest, {0, column_length}) | accumulator]
         )

  defp decode_columns(binary, accumulator \\ [])
  defp decode_columns(<<>>, accumulator), do: Enum.reverse(accumulator)

  defp decode_columns(<<flags::integer-8, rest::binary>>, accumulator) do
    [name | [<<data_type_id::integer-32, type_modifier::integer-32, columns::binary>>]] =
      String.split(rest, <<0>>, parts: 2)

    decoded_flags =
      case flags do
        1 -> [:key]
        _ -> []
      end

    decode_columns(columns, [
      %Column{
        name: name,
        flags: decoded_flags,
        type: OidDatabase.name_for_type_id(data_type_id),
        type_modifier: type_modifier
      }
      | accumulator
    ])
  end

  defp pgtimestamp_to_timestamp(microsecond_offset) when is_integer(microsecond_offset) do
    {:ok, epoch, 0} = @pg_epoch

    DateTime.add(epoch, microsecond_offset, :microsecond)
  end

  defp decode_lsn(<<xlog_file::integer-32, xlog_offset::integer-32>>) do
    Bitwise.bsl(xlog_file, 32) + xlog_offset
  end
end

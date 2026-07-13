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
    Message,
    Origin,
    Relation,
    StreamAbort,
    StreamCommit,
    StreamStart,
    StreamStop,
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
  @spec decode(binary(), keyword()) :: {:ok, struct()} | {:error, Error.t()}
  def decode(binary, opts \\ []) when is_binary(binary) do
    streaming? = Keyword.get(opts, :streaming, false)

    case decode_message(binary, streaming?) do
      %Unsupported{} -> {:error, %Error{reason: :unsupported_message}}
      message -> {:ok, message}
    end
  rescue
    exception -> {:error, Error.decode_failure(exception)}
  end

  # Stream-control messages (spec §5) — unambiguous by type byte, decoded regardless
  # of the streaming flag (they only occur mid-stream anyway).
  defp decode_message(<<"S", xid::integer-32, first::integer-8>>, _streaming?),
    do: %StreamStart{xid: xid, first_segment: first == 1}

  defp decode_message(<<"E">>, _streaming?), do: %StreamStop{}

  defp decode_message(
         <<"c", xid::integer-32, _flags::integer-8, commit_lsn::binary-8, end_lsn::binary-8,
           timestamp::integer-64>>,
         _streaming?
       ) do
    %StreamCommit{
      xid: xid,
      commit_lsn: decode_lsn(commit_lsn),
      end_lsn: decode_lsn(end_lsn),
      commit_timestamp: pgtimestamp_to_timestamp(timestamp)
    }
  end

  defp decode_message(<<"A", xid::integer-32, subxid::integer-32>>, _streaming?),
    do: %StreamAbort{xid: xid, subxid: subxid}

  # A STREAMED change message carries an Int32 (sub)xid before the v1 body (spec §5).
  # Strip it, decode the remainder through the v1 parser, and re-attach the xid — so the
  # tuple/column parsing stays in ONE place. Only the xid-prefixed types delegate here;
  # this clause is guarded on streaming?: true and precedes the v1 change clauses.
  defp decode_message(<<type::integer-8, xid::integer-32, rest::binary>>, true)
       when type in [?I, ?U, ?D, ?T, ?R, ?Y, ?M] do
    put_xid(decode_message_impl(<<type::integer-8, rest::binary>>), xid)
  end

  # Non-streamed (v1) and all remaining types delegate to the shipped parser unchanged.
  defp decode_message(binary, _streaming?), do: decode_message_impl(binary)

  defp put_xid(%mod{} = msg, xid)
       when mod in [Insert, Update, Delete, Truncate, Relation, Type, Message],
       do: %{msg | xid: xid}

  defp put_xid(msg, _xid), do: msg

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

  # 'M' — a logical-decoding message (spec §6 / A2). flags::8 (1 = transactional, 0 = non-txn),
  # lsn::64, prefix (null-terminated String), length::32, content::bytes(length). A malformed
  # frame (truncated content) raises inside split/binary-part → caught by decode/1's boundary.
  defp decode_message_impl(<<"M", flags::8, lsn::binary-8, rest::binary>>) do
    [prefix, rest_after_null] = :binary.split(rest, <<0>>)
    <<length::integer-32, content::binary-size(length)>> = rest_after_null

    %Message{
      transactional?: flags == 1,
      lsn: decode_lsn(lsn),
      prefix: prefix,
      content: content
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

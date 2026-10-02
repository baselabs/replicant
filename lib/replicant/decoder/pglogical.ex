defmodule Replicant.Decoder.Pglogical do
  @moduledoc """
  The `pglogical_output` binary-protocol decoder (pglogical 2.x, ADR-0009 §3).

  Wire grammar (OBSERVED from the pglogical 2.4.8 sources; anchors in
  `.kimosabe/intents/pglogical-wal2json-decoders.md`): a startup message `S` (version
  byte 1 + NUL-terminated key/value pairs) precedes the first transaction; `B`/`C`
  bracket transactions (flags u8, LSNs u64, commit time µs-since-2000-epoch, xid u32);
  `R` relation metadata carries names and identity-key flags but NO types and NO
  replica-identity field (both come from the connect-time catalog read in the cache);
  row messages `I`/`U`/`D` carry flags u8, relid u32 and `K`/`N` tuple markers; a tuple
  is `T`, natts u16, then per attribute `n` null, `u` unchanged TOAST, or `t` length
  u32 (NUL-INCLUSIVE) + bytes.

  The decoder requests text-format values only (no `binary.want_*`), so the casting
  layer receives the same server text output pgoutput delivers. pglogical emits no
  TRUNCATE and no logical-decoding message (it registers no such callbacks), which
  `capabilities/0` expresses.
  """

  @behaviour Replicant.Decoder.Plugin

  import Bitwise

  alias Replicant.Decoder.Messages

  @protocol_min 1
  @protocol_max 1

  @pg_epoch (case DateTime.from_iso8601("2000-01-01T00:00:00Z") do
               {:ok, epoch, 0} -> epoch
             end)

  @impl true
  @spec slot_plugin() :: String.t()
  def slot_plugin, do: "pglogical_output"

  @impl true
  @spec capabilities() :: []
  def capabilities, do: []

  @impl true
  @spec start_options(keyword()) :: [{String.t(), String.t()}]
  def start_options(opts) do
    sets = Keyword.fetch!(opts, :replication_sets)

    [
      {"startup_params_format", "1"},
      {"min_proto_version", Integer.to_string(@protocol_min)},
      {"max_proto_version", Integer.to_string(@protocol_max)},
      {"proto_format", "native"},
      # The dotted option name is a DOUBLE-QUOTED identifier in the walsender's option
      # grammar — exactly how pglogical's own worker spells it (pglogical.c).
      {"\"pglogical.replication_set_names\"", Enum.join(sets, ",")}
    ]
  end

  @impl true
  @spec init_cache(keyword()) :: Replicant.Decoder.Plugin.cache()
  def init_cache(opts) do
    %{
      # {namespace, name} => %{column => {type, type_modifier}} from the connect-time
      # catalog read (pglogical's wire metadata carries no types)
      column_types: Keyword.get(opts, :column_types, %{}),
      # {namespace, name} => replica-identity atom from the same read
      replica_identity: Keyword.get(opts, :replica_identity, %{}),
      # relid => {namespace, name}, grown as `R` messages arrive
      relids: %{}
    }
  end

  @impl true
  @spec decode(binary(), Replicant.Decoder.Plugin.cache(), keyword()) ::
          {:ok, [struct()], Replicant.Decoder.Plugin.cache()}
          | {:error, Replicant.Error.t()}
  def decode(<<?S, rest::binary>>, cache, _opts), do: decode_startup(rest, cache)
  def decode(<<?B, rest::binary>>, cache, _opts), do: decode_txn_frame(?B, rest, cache)

  def decode(<<type::integer-8, rest::binary>>, cache, _opts)
      when type in [?C, ?O, ?R, ?I, ?U, ?D],
      do: decode_frame(type, rest, cache)

  def decode(_other, _cache, _opts),
    do: {:error, %Replicant.Error{reason: :unsupported_message}}

  defp decode_txn_frame(?B, <<_flags::8, final_lsn::64, commit_time::64, xid::32>>, cache),
    do: decode_begin(final_lsn, commit_time, xid, cache)

  defp decode_frame(?C, <<_flags::8, commit_lsn::64, end_lsn::64, commit_time::64>>, cache),
    do: decode_commit(commit_lsn, end_lsn, commit_time, cache)

  defp decode_frame(
         ?O,
         <<_flags::8, origin_lsn::64, len::8, origin::binary-size(len), _rest::binary>>,
         cache
       ),
       do: decode_origin(origin_lsn, origin, cache)

  defp decode_frame(?R, <<_flags::8, relid::32, rest::binary>>, cache),
    do: decode_relation(relid, rest, cache)

  defp decode_frame(?I, <<_flags::8, relid::32, ?N, rest::binary>>, cache),
    do: decode_insert(relid, rest, cache)

  defp decode_frame(?U, <<_flags::8, relid::32, ?K, rest::binary>>, cache),
    do: decode_key_update(relid, rest, cache)

  defp decode_frame(?U, <<_flags::8, relid::32, ?N, rest::binary>>, cache),
    do: decode_plain_update(relid, rest, cache)

  defp decode_frame(?D, <<_flags::8, relid::32, ?K, rest::binary>>, cache),
    do: decode_delete(relid, rest, cache)

  defp decode_startup(<<1, rest::binary>>, cache) do
    case startup_params(rest, %{}) do
      {:ok, params} -> check_startup_protocol(params, cache)
      {:error, _} = err -> err
    end
  end

  # A startup-message version byte we were not built for (value-free: only the shape
  # fact, never the bytes)
  defp decode_startup(_other_version, _cache),
    do: {:error, %Replicant.Error{reason: :decoder_protocol_unsupported}}

  defp decode_begin(final_lsn, commit_time, xid, cache) do
    message(
      %Messages.Begin{
        final_lsn: final_lsn,
        commit_timestamp: pg_timestamp(commit_time),
        xid: xid
      },
      cache
    )
  end

  defp decode_commit(commit_lsn, end_lsn, commit_time, cache) do
    message(
      %Messages.Commit{
        flags: [],
        lsn: commit_lsn,
        end_lsn: end_lsn,
        commit_timestamp: pg_timestamp(commit_time)
      },
      cache
    )
  end

  defp decode_origin(origin_lsn, origin, cache) do
    message(%Messages.Origin{origin_commit_lsn: origin_lsn, name: trim_nul(origin)}, cache)
  end

  defp decode_insert(relid, rest, cache) do
    {tuple, _rest} = decode_tuple(rest)
    message(%Messages.Insert{relation_id: relid, tuple_data: tuple}, cache)
  end

  defp decode_key_update(relid, rest, cache) do
    {old, <<?N, new_rest::binary>>} = decode_tuple(rest)
    {new, _} = decode_tuple(new_rest)
    message(old_key_update(relid, old, new, cache), cache)
  end

  defp decode_plain_update(relid, rest, cache) do
    {new, _} = decode_tuple(rest)
    message(%Messages.Update{relation_id: relid, tuple_data: new}, cache)
  end

  defp decode_delete(relid, rest, cache) do
    {old, _} = decode_tuple(rest)
    message(old_key_delete(relid, old, cache), cache)
  end

  # ---- startup ----

  defp startup_params(<<>>, acc), do: {:ok, acc}

  defp startup_params(bin, acc) do
    with [key, rest] <- nul_split(bin),
         [value, rest2] <- nul_split(rest) do
      startup_params(rest2, Map.put(acc, key, value))
    else
      _ -> {:error, %Replicant.Error{reason: :decode_failure}}
    end
  end

  defp nul_split(bin) do
    case :binary.split(bin, <<0>>) do
      [a, b] -> [a, b]
      [_] -> nil
    end
  end

  # ADR-0009 §3: a startup message whose reported protocol range does not overlap the
  # range this decoder was built for halts :decoder_protocol_unsupported.
  defp check_startup_protocol(params, cache) do
    with {:ok, max_v} <- fetch_int(params, "max_proto_version"),
         {:ok, min_v} <- fetch_int(params, "min_proto_version") do
      if max_v >= @protocol_min and min_v <= @protocol_max do
        {:ok, [], cache}
      else
        {:error, %Replicant.Error{reason: :decoder_protocol_unsupported}}
      end
    else
      _ -> {:error, %Replicant.Error{reason: :decode_failure}}
    end
  end

  defp fetch_int(params, key) do
    case Map.fetch(params, key) do
      {:ok, value} ->
        case Integer.parse(value) do
          {int, ""} -> {:ok, int}
          _ -> :error
        end

      :error ->
        :error
    end
  end

  # ---- relation ----

  # nspnamelen u8 + nspname (incl NUL), relnamelen u8 + relname (incl NUL), 'A',
  # natts u16, then per column: 'C' + flags u8, 'N' + namelen u16 (incl NUL) + name.
  defp decode_relation(relid, rest, cache) do
    with <<nsp_len::8, nsp::binary-size(nsp_len), rest2::binary>> <- rest,
         <<rel_len::8, rel::binary-size(rel_len), rest3::binary>> <- rest2,
         <<?A, _natts::16, cols_bin::binary>> <- rest3,
         {:ok, columns} <- decode_columns(cols_bin, []) do
      namespace = trim_nul(nsp)
      name = trim_nul(rel)
      key = {namespace, name}

      relation = %Messages.Relation{
        id: relid,
        namespace: namespace,
        name: name,
        replica_identity: Map.get(cache.replica_identity, key),
        columns:
          columns
          |> merge_types(Map.get(cache.column_types, key))
          |> merge_key_flags(Map.get(cache.replica_identity, key))
      }

      {:ok, [relation], %{cache | relids: Map.put(cache.relids, relid, key)}}
    else
      _ -> {:error, %Replicant.Error{reason: :decode_failure}}
    end
  end

  # Wire columns carry name + key flag; types come from the catalog cache. A wire column
  # the catalog read has not seen (mid-stream DDL ahead of the connect-time snapshot)
  # keeps type nil — the casting layer's documented lenient fallback delivers the raw
  # server text for an unknown type, and the column-set change itself is classified by
  # the assembler's schema-change logic.
  #
  # KEY FLAGS under REPLICA IDENTITY FULL: pglogical's wire flags come solely from the
  # identity-index bitmap (pglogical_proto_native.c: RelationGetIndexAttrBitmap with
  # INDEX_ATTR_BITMAP_IDENTITY_KEY), which is EMPTY under FULL — there is no identity
  # index. pgoutput flags every column [:key] there, so the delivered Change.columns
  # metadata must match: the connect-time catalog read (which does know `relreplident`)
  # widens the flags to the whole row under FULL.
  defp merge_key_flags(columns, :all_columns),
    do: Enum.map(columns, fn col -> %{col | flags: [:key]} end)

  defp merge_key_flags(columns, _default_or_index_or_unknown), do: columns

  defp merge_types(columns, nil), do: columns

  defp merge_types(columns, types) do
    Enum.map(columns, fn col ->
      case Map.get(types, col.name) do
        nil -> col
        {type, type_modifier} -> %{col | type: type, type_modifier: type_modifier}
      end
    end)
  end

  defp decode_columns(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_columns(
         <<?C, flags::8, ?N, len::16, name::binary-size(len), rest::binary>>,
         acc
       ) do
    decode_columns(rest, [
      %Messages.Relation.Column{
        name: trim_nul(name),
        flags: if(band(flags, 1) == 1, do: [:key], else: []),
        type: nil,
        type_modifier: -1
      }
      | acc
    ])
  end

  defp decode_columns(_other, _acc), do: {:error, %Replicant.Error{reason: :decode_failure}}

  # ---- tuples ----

  # `T` + natts u16 + per attribute: n | u | t len u32 (NUL-inclusive) + bytes. `i`/`b`
  # only ever arrive when binary transfer was negotiated, which this decoder never
  # requests — receiving one is a protocol violation (the malformed clause throws into
  # the Decoder.decode/2 boundary, value-free).
  defp decode_tuple(<<?T, natts::16, rest::binary>>), do: decode_tuple_values(rest, natts, [])

  defp decode_tuple(_other), do: throw(:malformed_tuple)

  defp decode_tuple_values(rest, 0, acc), do: {List.to_tuple(Enum.reverse(acc)), rest}

  defp decode_tuple_values(<<?n, rest::binary>>, n, acc),
    do: decode_tuple_values(rest, n - 1, [nil | acc])

  defp decode_tuple_values(<<?u, rest::binary>>, n, acc),
    do: decode_tuple_values(rest, n - 1, [:unchanged_toast | acc])

  defp decode_tuple_values(<<?t, len::32, value::binary-size(len), rest::binary>>, n, acc)
       when len > 0,
       do: decode_tuple_values(rest, n - 1, [trim_nul(value) | acc])

  defp decode_tuple_values(_other, _n, _acc), do: throw(:malformed_tuple)

  defp trim_nul(bin) when byte_size(bin) > 0, do: binary_part(bin, 0, byte_size(bin) - 1)

  # ---- old-key classification ----

  # pglogical marks every old tuple `K`; whether it is a key-only or a full old tuple
  # depends on the table's replica identity, which the wire does not carry — the
  # connect-time catalog read decides. Under `f` (FULL) the logged old tuple is the
  # whole row. A row for a relid no `R` message introduced is a protocol violation.
  defp old_key_update(relid, old, new, cache) do
    case relation_key(cache, relid) do
      {:ok, key} ->
        case Map.get(cache.replica_identity, key) do
          :all_columns ->
            %Messages.Update{relation_id: relid, tuple_data: new, old_tuple_data: old}

          _key_only_or_unknown ->
            %Messages.Update{
              relation_id: relid,
              tuple_data: new,
              changed_key_tuple_data: old
            }
        end

      :error ->
        throw(:unknown_relation)
    end
  end

  defp old_key_delete(relid, old, cache) do
    case relation_key(cache, relid) do
      {:ok, key} ->
        case Map.get(cache.replica_identity, key) do
          :all_columns ->
            %Messages.Delete{relation_id: relid, old_tuple_data: old}

          _key_only_or_unknown ->
            %Messages.Delete{relation_id: relid, changed_key_tuple_data: old}
        end

      :error ->
        throw(:unknown_relation)
    end
  end

  defp relation_key(cache, relid) do
    case Map.fetch(cache.relids, relid) do
      {:ok, key} -> {:ok, key}
      :error -> :error
    end
  end

  # ---- helpers ----

  defp message(msg, cache), do: {:ok, [msg], cache}

  defp pg_timestamp(microsecond_offset) when is_integer(microsecond_offset),
    do: DateTime.add(@pg_epoch, microsecond_offset, :microsecond)
end

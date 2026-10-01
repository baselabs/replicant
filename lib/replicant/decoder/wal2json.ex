defmodule Replicant.Decoder.Wal2json do
  @moduledoc """
  The `wal2json` decoder — JSON format version 2 (ADR-0009 §4).

  One JSON document per action (`B`, `C`, `I`, `U`, `D`, `T`, `M`; anchors in
  `.kimosabe/intents/pglogical-wal2json-decoders.md`). The decoder requests
  `include-transaction`, `include-lsn`, `include-xids`, `include-timestamp`,
  `include-types`, `include-type-oids`, `include-pk` and
  `numeric-data-types-as-string` (wal2json ≥ 2.6): numerics arrive as JSON strings so
  no trailing digit is lost, and type names come from the type OIDs through the same
  `OidDatabase` call pgoutput's decoder uses. `add-tables` carries the configured
  tables.

  wal2json sends no relation message, so the decoder synthesizes a
  `Replicant.Decoder.Messages.Relation` from the first change it sees for a table and
  re-emits it ahead of any change whose column list differs (ADR-0009 §5) — the
  existing schema-change classification then runs unchanged. A column ABSENT from a
  change's `columns` list is wal2json's unchanged-TOAST sentinel (the plugin simply
  skips it), which this decoder surfaces as the first-class `:unchanged_toast` marker.
  The relation's `replica_identity` and stable `id` come from the connect-time catalog
  read in the cache.

  ## Documented divergences from the pgoutput contract

  wal2json's format 2 CANNOT express a dropped column: the plugin omits a column only
  when it is unchanged-TOASTed, so a column that vanishes from the change stream is
  indistinguishable from an untouched TOASTed column and surfaces as the `unchanged`
  sentinel indefinitely — the assembler's destructive-drop classification (Critical
  Rule 4) never fires under this decoder. A dropped column is therefore caught at the
  NEXT reconnect's catalog read (the synthesized relation shrinks) rather than in
  stream. A column ADD is detected and classified as usual (the append-only drift
  merge above).
  """

  @behaviour Replicant.Decoder.Plugin

  import Bitwise

  alias Replicant.Decoder.Messages
  alias Replicant.Decoder.OidDatabase

  @impl true
  @spec slot_plugin() :: String.t()
  def slot_plugin, do: "wal2json"

  @impl true
  @spec capabilities() :: [:messages]
  def capabilities, do: [:messages]

  @impl true
  @spec start_options(keyword()) :: [{String.t(), String.t()}]
  def start_options(opts) do
    tables = Keyword.fetch!(opts, :tables)
    add_tables = Enum.map_join(tables, ",", fn {schema, table} -> "#{schema}.#{table}" end)

    # The hyphenated option names are DOUBLE-QUOTED identifiers in the walsender's
    # option grammar (a bare IDENT cannot carry `-`; the same rule pglogical's dotted
    # `"pglogical.replication_set_names"` follows).
    [
      {"\"format-version\"", "2"},
      {"\"include-transaction\"", "true"},
      {"\"include-lsn\"", "true"},
      {"\"include-xids\"", "true"},
      {"\"include-timestamp\"", "true"},
      {"\"include-types\"", "true"},
      {"\"include-type-oids\"", "true"},
      {"\"include-pk\"", "true"},
      {"\"numeric-data-types-as-string\"", "true"},
      {"\"add-tables\"", add_tables}
    ]
  end

  @impl true
  @spec init_cache(keyword()) :: Replicant.Decoder.Plugin.cache()
  def init_cache(opts) do
    %{
      # {schema, table} => %Relation{} synthesized on first sight (plus the catalog seed
      # from the connect-time read, so a `T` action for a quiet table already resolves)
      relations: Keyword.get(opts, :relations, %{}),
      # {schema, table} => replica-identity atom from the connect-time catalog read
      replica_identity: Keyword.get(opts, :replica_identity, %{}),
      # {schema, table} => relation id (the pg_class oid from the same read; the
      # assembler keys relations by id, so it only needs to be unique and stable)
      relids: Keyword.get(opts, :relids, %{}),
      # {schema, table} keys whose Relation has already been announced to the
      # assembler this connection. The assembler's own relation cache is fed ONLY by
      # %Relation{} messages (the pgoutput contract), so a pre-seeded or synthesized
      # relation MUST be emitted once ahead of its first change — otherwise the
      # assembler halts "row for uncached relation".
      announced: MapSet.new()
    }
  end

  @impl true
  @spec decode(binary(), Replicant.Decoder.Plugin.cache(), keyword()) ::
          {:ok, [struct()], Replicant.Decoder.Plugin.cache()}
          | {:error, Replicant.Error.t()}
  def decode(payload, cache, _opts) do
    case Jason.decode(payload) do
      {:ok, %{"action" => action} = doc} ->
        decode_action(action, doc, cache)

      {:ok, _other_shape} ->
        {:error, %Replicant.Error{reason: :decode_failure}}

      # The Jason error struct (and its message) can embed payload bytes — discard it
      # entirely; only the shape fact survives (Critical Rule 1).
      {:error, _discarded} ->
        {:error, %Replicant.Error{reason: :decode_failure}}
    end
  end

  defp decode_action("B", doc, cache) do
    message(
      %Messages.Begin{
        final_lsn: doc_lsn(doc),
        commit_timestamp: doc_timestamp(doc),
        xid: doc["xid"]
      },
      cache
    )
  end

  defp decode_action("C", doc, cache) do
    # ADR-0009 §7: the commit LSN is never fabricated — a commit document without one
    # halts :decoder_lsn_missing.
    case doc_lsn(doc) do
      nil ->
        {:error, %Replicant.Error{reason: :decoder_lsn_missing}}

      commit_lsn ->
        message(
          %Messages.Commit{
            flags: [],
            lsn: commit_lsn,
            end_lsn: doc_lsn(doc, "nextlsn"),
            commit_timestamp: doc_timestamp(doc)
          },
          cache
        )
    end
  end

  defp decode_action("I", doc, cache), do: decode_change(doc, cache, :insert)

  defp decode_action("U", doc, cache), do: decode_change(doc, cache, :update)

  defp decode_action("D", doc, cache), do: decode_change(doc, cache, :delete)

  defp decode_action("T", doc, cache) do
    key = table_key(doc)

    case Map.fetch(cache.relations, key) do
      {:ok, relation} ->
        # wal2json's T document carries no CASCADE / RESTART IDENTITY flags — options
        # stay the explicit empty marker (documented difference, ADR-0009 §6). The
        # relation is ANNOUNCED first if the assembler has not seen it this
        # connection (a TRUNCATE can be a table's first action; the assembler halts
        # "truncate for uncached relation" without the announcement).
        if MapSet.member?(cache.announced, key) do
          message(
            %Messages.Truncate{
              number_of_relations: 1,
              options: [],
              truncated_relations: [relation.id]
            },
            cache
          )
        else
          cache = %{cache | announced: MapSet.put(cache.announced, key)}

          message(
            %Messages.Truncate{
              number_of_relations: 1,
              options: [],
              truncated_relations: [relation.id]
            },
            cache,
            [relation]
          )
        end

      :error ->
        {:error, %Replicant.Error{reason: :decode_failure}}
    end
  end

  defp decode_action("M", doc, cache) do
    message(
      %Messages.Message{
        transactional?: doc["transactional"] == true,
        lsn: doc_lsn(doc),
        prefix: doc["prefix"],
        content: doc["content"]
      },
      cache
    )
  end

  defp decode_action(_unknown, _doc, _cache),
    do: {:error, %Replicant.Error{reason: :unsupported_message}}

  # ---- change decoding + relation synthesis ----

  defp decode_change(doc, cache, op) do
    key = table_key(doc)
    columns = change_columns(doc)

    case Map.fetch(cache.relations, key) do
      :error ->
        first_sight(doc, key, columns, cache, op)

      {:ok, relation} ->
        decode_known_change(op, doc, key, columns, cache, relation)
    end
  end

  # A change for a table the cache knows. No drift → announce-and-emit; drift →
  # MERGE the new columns into the cached relation (append-only, never rebuilt: a
  # wal2json change OMITS unchanged-TOAST columns, so a rebuild would drop them and
  # the assembler would classify a legitimate ADD COLUMN as a destructive DROP —
  # OBSERVED in review) and emit the updated relation AHEAD of the change so the
  # schema-change classification runs (ADR-0009 §5).
  defp decode_known_change(op, doc, key, columns, cache, relation) do
    case column_drift(columns, relation.columns || []) do
      :none ->
        case dropped_columns(op, columns, relation) do
          [] ->
            announce_once(op, doc, relation, columns, cache)

          dropped ->
            # A DROPPED column, distinguished from the unchanged-TOAST sentinel by the
            # two absence rules (see dropped_columns/3): re-emit the relation WITHOUT
            # it ahead of the change — the assembler's shipped column_dropped
            # classification halts :destructive fail-closed, exactly like a pgoutput
            # Relation re-emit after the same DDL.
            updated = %{relation | columns: Enum.reject(relation.columns, &(&1.name in dropped))}

            cache = %{
              cache
              | relations: Map.put(cache.relations, key, updated),
                announced: MapSet.put(cache.announced, key)
            }

            change_message(op, doc, updated, columns, cache, [updated])
        end

      {:new_columns, new_names} ->
        identity_names = identity_column_names(doc)

        new_columns =
          Enum.map(new_names, fn name ->
            col = Enum.find(columns, &(&1["name"] == name))
            synthesize_column(col, identity_names)
          end)

        updated = %{relation | columns: (relation.columns || []) ++ new_columns}

        cache = %{
          cache
          | relations: Map.put(cache.relations, key, updated),
            announced: MapSet.put(cache.announced, key)
        }

        change_message(op, doc, updated, columns, cache, [updated])
    end
  end

  # The pre-seeded relation exists in THIS decoder's cache but the assembler only
  # learns a relation from a %Relation{} message — announce it once per connection
  # ahead of the table's first change (mirrors pgoutput's per-stream Relation).
  defp announce_once(op, doc, relation, columns, cache) do
    key = {relation.namespace, relation.name}

    if MapSet.member?(cache.announced, key) do
      change_message(op, doc, relation, columns, cache, [])
    else
      cache = %{cache | announced: MapSet.put(cache.announced, key)}
      change_message(op, doc, relation, columns, cache, [relation])
    end
  end

  # The first change for a table builds the relation: names/types/typeoids from the
  # change, replica identity and a stable id from the connect-time catalog read.
  defp first_sight(doc, key, columns, cache, op) do
    relation = synthesize_relation(doc, key, columns, cache)

    cache = %{
      cache
      | relations: Map.put(cache.relations, key, relation),
        announced: MapSet.put(cache.announced, key)
    }

    change_message(op, doc, relation, columns, cache, [relation])
  end

  defp synthesize_relation(doc, key, columns, cache) do
    identity_names = identity_column_names(doc)

    %Messages.Relation{
      id: Map.get(cache.relids, key) || default_relation_id(key),
      namespace: elem(key, 0),
      name: elem(key, 1),
      replica_identity: Map.get(cache.replica_identity, key),
      columns: Enum.map(columns, &synthesize_column(&1, identity_names))
    }
  end

  defp synthesize_column(col, identity_names) do
    %Messages.Relation.Column{
      name: col["name"],
      flags: if(col["name"] in identity_names, do: [:key], else: []),
      type: OidDatabase.name_for_type_id(col["typeoid"]),
      type_modifier: type_modifier(col["type"] || "")
    }
  end

  defp change_message(op, doc, relation, columns, cache, prefix) do
    new_values = if op == :delete, do: nil, else: values_by_name(columns)

    new_tuple =
      case op do
        :delete ->
          nil

        _ ->
          new_values |> tuple_against(relation, :unchanged_toast)
      end

    {old_kind, old} = old_tuple(doc, relation, new_values)

    message =
      case op do
        :insert ->
          %Messages.Insert{relation_id: relation.id, tuple_data: new_tuple}

        :update ->
          update_message(relation.id, new_tuple, old_kind, old)

        :delete ->
          delete_message(relation.id, old_kind, old)
      end

    {:ok, prefix ++ [message], cache}
  end

  defp update_message(relid, new_tuple, :key, old),
    do: %Messages.Update{relation_id: relid, tuple_data: new_tuple, changed_key_tuple_data: old}

  defp update_message(relid, new_tuple, :old, old),
    do: %Messages.Update{relation_id: relid, tuple_data: new_tuple, old_tuple_data: old}

  defp update_message(relid, new_tuple, nil, _old),
    do: %Messages.Update{relation_id: relid, tuple_data: new_tuple}

  defp delete_message(relid, :key, old),
    do: %Messages.Delete{relation_id: relid, changed_key_tuple_data: old}

  defp delete_message(relid, :old, old),
    do: %Messages.Delete{relation_id: relid, old_tuple_data: old}

  defp delete_message(relid, nil, _old),
    do: %Messages.Delete{relation_id: relid}

  # identity[] carries the old key (or the whole old row under REPLICA IDENTITY FULL).
  # Expanded to the relation's full width with nil for non-identity columns — the exact
  # shape pgoutput's K/O tuples and pglogical's K tuple use, which the assembler zips
  # against the full column list.
  #
  # SYNTHESIS DISCRIMINATION (OBSERVED): for an UPDATE whose old tuple was not logged
  # (key columns unchanged), wal2json synthesizes identity[] FROM THE NEW TUPLE when a
  # PK or replica-identity index exists — pgoutput and pglogical deliver NO old data
  # there. A logged old tuple is distinguishable by value: PG logs it only when a key
  # column CHANGED, so a real old key differs from the new tuple at some identity
  # column; the synthesized one equals it everywhere. Under FULL the identity is the
  # whole old row and is always real. This keeps the old_record contract byte-identical
  # across the three decoders.
  defp old_tuple(doc, relation, new_values) do
    case identity_map(doc) do
      nil ->
        {nil, nil}

      identity ->
        cond do
          relation.replica_identity == :all_columns ->
            {:old, values_by_name(identity) |> tuple_against(relation, nil)}

          synthesized_from_new?(identity, new_values) ->
            {nil, nil}

          true ->
            {:key, values_by_name(identity) |> tuple_against(relation, nil)}
        end
    end
  end

  defp synthesized_from_new?(identity, new_values) when is_map(new_values) do
    Enum.all?(identity, fn col ->
      case Map.fetch(new_values, col["name"]) do
        {:ok, value} -> value == col["value"]
        :error -> true
      end
    end)
  end

  defp synthesized_from_new?(_identity, _new_values), do: false

  defp identity_column_names(doc) do
    case doc["identity"] do
      nil -> []
      identity -> Enum.map(identity, & &1["name"])
    end
  end

  defp identity_map(doc) do
    case doc["identity"] do
      nil -> nil
      identity -> identity
    end
  end

  defp change_columns(doc) do
    case doc["columns"] do
      nil -> []
      columns -> columns
    end
  end

  # wal2json overloads ABSENCE: a dropped column and an untouched out-of-line
  # (TOASTed) column both omit from a change's columns list. Two rules separate them
  # WITHOUT a catalog round-trip: (1) an INSERT carries every live column — a cached
  # column absent from an insert can only have been dropped; (2) a FIXED-WIDTH type
  # (never varlena, so never stored out-of-line — no column STORAGE setting can
  # externalize it) absent from an UPDATE can only have been dropped. Everything else
  # absent on an update stays the :unchanged_toast sentinel (a false destructive halt
  # on a lawful TOAST omission would be a worse failure than a late detection); the
  # periodic schema guard owns that residual window.
  @never_toastable ~w(int2 int4 int8 float4 float8 bool date time timetz timestamp
                     timestamptz interval uuid oid xid cid)

  defp dropped_columns(:insert, columns, relation) do
    present = MapSet.new(columns, & &1["name"])
    for col <- relation.columns || [], not MapSet.member?(present, col.name), do: col.name
  end

  defp dropped_columns(:update, columns, relation) do
    present = MapSet.new(columns, & &1["name"])

    for col <- relation.columns || [],
        not MapSet.member?(present, col.name),
        col.type in @never_toastable,
        do: col.name
  end

  defp dropped_columns(_delete_and_friends, _columns, _relation), do: []

  # A change column the cached relation does not know is schema drift the plugin did
  # not announce (ADR-0009 §5 re-emit). A change column the relation knows but the
  # change omits is wal2json's unchanged-TOAST sentinel — NOT drift.
  defp column_drift(columns, relation_columns) do
    known = MapSet.new(relation_columns, & &1.name)
    new_names = for col <- columns, not MapSet.member?(known, col["name"]), do: col["name"]

    if new_names == [], do: :none, else: {:new_columns, new_names}
  end

  defp values_by_name(columns) do
    Map.new(columns, fn col -> {col["name"], col["value"]} end)
  end

  # One tuple element per relation column, in relation order. A column absent from the
  # change is wal2json's unchanged-TOAST sentinel when the change carries values
  # (`:unchanged_toast`) — or an explicit NULL placeholder on the identity expansion
  # (`nil`), per `missing`.
  defp tuple_against(values, relation, missing) do
    relation.columns
    |> Enum.map(fn col ->
      case Map.fetch(values, col.name) do
        {:ok, value} -> normalize_value(value, col)
        :error -> missing
      end
    end)
    |> List.to_tuple()
  end

  # JSON-typed values back to the server's text form — the casting layer (ADR-0008)
  # receives exactly the text pgoutput delivers. With numeric-data-types-as-string on,
  # numbers already arrive as JSON strings; bool is a JSON boolean; bytea arrives as
  # bare hex without the `\x` prefix.
  defp normalize_value(nil, _col), do: nil

  defp normalize_value(value, %Messages.Relation.Column{type: "boolean"})
       when is_boolean(value),
       do: if(value == true, do: "t", else: "f")

  defp normalize_value(value, %Messages.Relation.Column{type: "bytea"})
       when is_binary(value),
       do: "\\x" <> value

  defp normalize_value(true, _col), do: "t"
  defp normalize_value(false, _col), do: "f"
  defp normalize_value(value, _col) when is_binary(value), do: value
  defp normalize_value(value, _col) when is_integer(value), do: Integer.to_string(value)
  defp normalize_value(value, _col) when is_float(value), do: Float.to_string(value)
  defp normalize_value(_other, _col), do: throw(:malformed_value)

  # ---- value/type helpers ----

  defp doc_lsn(doc, key \\ "lsn") do
    case doc[key] do
      nil -> nil
      lsn -> Replicant.lsn_from_string(lsn) |> lsn_or_throw()
    end
  end

  defp lsn_or_throw({:ok, lsn}), do: lsn
  defp lsn_or_throw({:error, _}), do: throw(:malformed_lsn)

  defp doc_timestamp(doc) do
    case doc["timestamp"] do
      nil -> nil
      ts -> parse_timestamp(ts)
    end
  end

  defp parse_timestamp(ts) do
    case DateTime.from_iso8601(ts) do
      {:ok, dt, _offset} -> dt
      _ -> nil
    end
  end

  defp table_key(doc), do: {doc["schema"], doc["table"]}

  defp message(msg, cache), do: {:ok, [msg], cache}
  defp message(msg, cache, prefix), do: {:ok, prefix ++ [msg], cache}

  # A stable non-catalog relation id (only used when the connect-time read did not
  # cover the table — e.g. a table added to the stream by drift). Deterministic so a
  # reconnect re-derives the same id for the same table.
  defp default_relation_id({schema, table}), do: :erlang.phash2({schema, table})

  # wal2json's format-2 `type` string embeds the typmod the way `format_type` prints it
  # (`character varying(10)`, `numeric(10,2)`, `timestamp(3) without time zone`); this
  # parses it back to the raw signed atttypmod pgoutput delivers so a synthesized
  # relation's `type_modifier` is byte-identical. An array column carries no column-level
  # typmod (atttypmod = -1) even when its element prints one; no parenthetical is -1.
  @varlena_typmod ~r/\A(character varying|character|numeric)\((\d+)(?:,(\d+))?\)\z/
  @plain_typmod ~r/\A(.*?[a-z])\((\d+)\)/

  @doc false
  def type_modifier(type_string) do
    cond do
      String.contains?(type_string, "[]") -> -1
      not String.contains?(type_string, "(") -> -1
      true -> typmod_of(type_string)
    end
  end

  defp typmod_of(type_string) do
    case Regex.run(@varlena_typmod, type_string) do
      # an unmatched TRAILING optional capture yields a 3-element run (no scale)
      [_all, family, primary] -> varlena_typmod(family, primary, nil)
      [_all, family, primary, scale] -> varlena_typmod(family, primary, scale)
      nil -> plain_typmod(type_string)
    end
  end

  defp varlena_typmod("numeric", primary, scale),
    do: (String.to_integer(primary) <<< 16) + String.to_integer(scale || "0") + 4

  defp varlena_typmod(_varchar_or_bpchar, primary, _scale),
    do: String.to_integer(primary) + 4

  defp plain_typmod(type_string) do
    case Regex.run(@plain_typmod, type_string) do
      [_all, _family, primary] -> String.to_integer(primary)
      nil -> -1
    end
  end
end

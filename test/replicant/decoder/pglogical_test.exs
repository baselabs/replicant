defmodule Replicant.Decoder.PglogicalTest do
  @moduledoc """
  Hand-built pglogical_output frames from the OBSERVED native wire grammar (pglogical
  2.4.8 sources; anchors in `.kimosabe/intents/pglogical-wal2json-decoders.md`): `'S'`
  startup, `'B`/`'C'` txn brackets (flags u8, LSN u64, commit-time µs, xid u32), `'R'`
  relation metadata (NUL-inclusive name lengths, NO types / NO replica identity on the
  wire), `'I'`/`'U'`/`'D'` rows with `'K'`/`'N'` tuple markers, and the `'T'`-tuple
  body whose `'t'`-kind value length is NUL-INCLUSIVE.
  """

  use ExUnit.Case, async: true

  alias Replicant.Decoder
  alias Replicant.Decoder.Messages
  alias Replicant.Decoder.Pglogical

  defp cache do
    Pglogical.init_cache(
      column_types: %{
        {"public", "users"} => %{"id" => {"int8", -1}, "name" => {"text", -1}}
      },
      replica_identity: %{{"public", "users"} => :default}
    )
  end

  defp decode(payload, cache \\ cache(), opts \\ []) do
    Decoder.decode(payload, [decoder: :pglogical, cache: cache] ++ opts)
  end

  # 'R' for public.users(id int8 key, name text) — relation id 16384.
  defp relation_frame(relid \\ 16_384) do
    <<
      ?R,
      # flags
      0::8,
      relid::32,
      # nspname (NUL-inclusive)
      byte_size("public\0")::8,
      "public\0",
      byte_size("users\0")::8,
      "users\0",
      ?A,
      2::16,
      # column 1: key flag 1, name "id"
      ?C,
      1::8,
      ?N,
      byte_size("id\0")::16,
      "id\0",
      # column 2: no key flag, name "name"
      ?C,
      0::8,
      ?N,
      byte_size("name\0")::16,
      "name\0"
    >>
  end

  # 'T' tuple, 2 atts: text "1" and NULL.
  defp tuple_int_null do
    <<?T, 2::16, ?t, 2::32, "1\0", ?n>>
  end

  describe "REPLICA IDENTITY FULL key-flag parity (pgoutput flags the whole row)" do
    # pglogical's wire key flags come only from the identity-index bitmap, which is
    # EMPTY under FULL (no identity index) — the catalog read must widen the flags to
    # every column, or the delivered Change.columns metadata contradicts pgoutput for
    # the same table (fresh-review finding F4; OBSERVED in pglogical_proto_native.c).
    test "an R message for a catalog-known FULL table flags EVERY column [:key]" do
      full_cache =
        Pglogical.init_cache(
          column_types: %{
            {"public", "users"} => %{"id" => {"int8", -1}, "name" => {"text", -1}}
          },
          replica_identity: %{{"public", "users"} => :all_columns}
        )

      assert {:ok, [%Messages.Relation{} = rel], _} = decode(relation_frame(), full_cache)

      assert Enum.map(rel.columns, & &1.flags) == [[:key], [:key]]
    end

    test "a DEFAULT-identity table keeps the wire flags verbatim (id key, name not)" do
      assert {:ok, [%Messages.Relation{} = rel], _} = decode(relation_frame())

      assert Enum.map(rel.columns, & &1.flags) == [[:key], []]
    end
  end

  describe "startup message" do
    test "version-1 startup with an overlapping protocol range decodes to no messages" do
      startup =
        <<?S, 1>> <>
          paired("max_proto_version", "1") <> paired("min_proto_version", "1")

      assert {:ok, [], cache} = decode(startup)
      assert is_map(cache)
    end

    test "a protocol range outside the decoder's halts :decoder_protocol_unsupported" do
      startup =
        <<?S, 1>> <> paired("max_proto_version", "2") <> paired("min_proto_version", "2")

      assert {:error, err} = decode(startup)
      assert err.reason == :decoder_protocol_unsupported
    end

    test "an unknown startup-message version byte halts :decoder_protocol_unsupported" do
      assert {:error, err} = decode(<<?S, 2, "anything", 0>>)
      assert err.reason == :decoder_protocol_unsupported
    end
  end

  describe "transaction brackets" do
    test "begin: final_lsn, commit timestamp (µs since the PG epoch), xid" do
      frame = <<?B, 0::8, 0x16E3778::64, 1234::64, 42::32>>

      assert {:ok, [%Messages.Begin{} = begin_msg], _cache} = decode(frame)
      assert begin_msg.final_lsn == 0x16E3778
      assert begin_msg.xid == 42

      assert DateTime.compare(
               begin_msg.commit_timestamp,
               DateTime.add(~U[2000-01-01 00:00:00Z], 1234, :microsecond)
             ) == :eq
    end

    test "commit: commit LSN, end LSN, timestamp (the watermark source, ADR-0009 §7)" do
      frame = <<?C, 0::8, 0x20::64, 0x28::64, 99::64>>

      assert {:ok, [%Messages.Commit{} = commit], _cache} = decode(frame)
      assert commit.lsn == 0x20
      assert commit.end_lsn == 0x28
    end
  end

  describe "relation metadata (no wire types or replica identity)" do
    test "decodes names, identity-key flags; types + replica identity come from the cache" do
      assert {:ok, [%Messages.Relation{} = rel], cache} = decode(relation_frame())

      assert rel.id == 16_384
      assert rel.namespace == "public"
      assert rel.name == "users"
      assert rel.replica_identity == :default

      assert [%Messages.Relation.Column{} = id_col, %Messages.Relation.Column{} = name_col] =
               rel.columns

      assert id_col.name == "id"
      assert id_col.flags == [:key]
      # from the connect-time catalog cache, not the wire
      assert id_col.type == "int8"
      assert id_col.type_modifier == -1
      assert name_col.name == "name"
      assert name_col.flags == []
      assert name_col.type == "text"

      # the relid mapping is cached for later old-key classification
      assert %{relids: %{16_384 => {"public", "users"}}} = cache
    end

    test "a column the catalog has not seen keeps type nil (lenient casting fallback)" do
      cache = Pglogical.init_cache(column_types: %{}, replica_identity: %{})

      assert {:ok, [%Messages.Relation{} = rel], _} = decode(relation_frame(), cache)
      assert Enum.all?(rel.columns, &is_nil(&1.type))
      assert Enum.all?(rel.columns, &(&1.type_modifier == -1))
    end

    test "a truncated relation frame is a value-free :decode_failure" do
      truncated = binary_part(relation_frame(), 0, 12)

      assert {:error, err} = decode(truncated)
      assert err.reason == :decode_failure

      inspected = inspect(err) <> Exception.message(err)
      refute inspected =~ "public"
      refute inspected =~ "users"
    end
  end

  describe "row messages and tuple markers" do
    test "insert: 'N' tuple with text (NUL trimmed), null" do
      frame = <<?I, 0::8, 16_384::32, ?N>> <> tuple_int_null()

      assert {:ok, [%Messages.Insert{} = insert], _} = decode(frame)
      assert insert.relation_id == 16_384
      assert insert.tuple_data == {"1", nil}
    end

    test "update with 'K': key-only old tuple under a DEFAULT identity" do
      {:ok, _, cache} = decode(relation_frame())

      old = <<?T, 2::16, ?t, 2::32, "1\0", ?n>>
      new = <<?T, 2::16, ?t, 2::32, "1\0", ?t, 4::32, "bob\0">>
      frame = <<?U, 0::8, 16_384::32, ?K>> <> old <> <<?N>> <> new

      assert {:ok, [%Messages.Update{} = update], _} = decode(frame, cache)
      assert update.tuple_data == {"1", "bob"}
      assert update.changed_key_tuple_data == {"1", nil}
      assert update.old_tuple_data == nil
    end

    test "update with 'K' under a FULL identity is the whole old tuple" do
      cache =
        Pglogical.init_cache(
          column_types: %{},
          replica_identity: %{{"public", "users"} => :all_columns}
        )

      # the relation must be seen first so the relid maps to the table
      {:ok, _, cache} = decode(relation_frame(), cache)

      old = <<?T, 2::16, ?t, 2::32, "1\0", ?t, 6::32, "alice\0">>
      new = <<?T, 2::16, ?t, 2::32, "1\0", ?t, 7::32, "alicia\0">>
      frame = <<?U, 0::8, 16_384::32, ?K>> <> old <> <<?N>> <> new

      assert {:ok, [%Messages.Update{} = update], _} = decode(frame, cache)
      assert update.old_tuple_data == {"1", "alice"}
      assert update.changed_key_tuple_data == nil
    end

    test "update WITHOUT 'K' (a keyless update, e.g. REPLICA IDENTITY NOTHING) delivers the new tuple" do
      new = <<?T, 2::16, ?t, 2::32, "1\0", ?n>>
      frame = <<?U, 0::8, 16_384::32, ?N>> <> new

      assert {:ok, [%Messages.Update{} = update], _} = decode(frame)
      assert update.tuple_data == {"1", nil}
      assert update.changed_key_tuple_data == nil
      assert update.old_tuple_data == nil
    end

    test "delete with 'K': key-only old tuple under DEFAULT" do
      {:ok, _, cache} = decode(relation_frame())
      old = <<?T, 2::16, ?t, 2::32, "1\0", ?n>>
      frame = <<?D, 0::8, 16_384::32, ?K>> <> old

      assert {:ok, [%Messages.Delete{} = delete], _} = decode(frame, cache)
      assert delete.changed_key_tuple_data == {"1", nil}
    end

    test "an unchanged-TOAST 'u' marker surfaces as the sentinel" do
      tuple = <<?T, 2::16, ?t, 2::32, "1\0", ?u>>
      frame = <<?I, 0::8, 16_384::32, ?N>> <> tuple

      assert {:ok, [%Messages.Insert{tuple_data: {"1", :unchanged_toast}}], _} =
               decode(frame)
    end

    test "a negotiated-binary 'i' value kind is a protocol violation (never requested)" do
      tuple = <<?T, 1::16, ?i, 8::32, 1::64>>
      frame = <<?I, 0::8, 16_384::32, ?N>> <> tuple

      assert {:error, err} = decode(frame)
      assert err.reason == :decode_failure
      refute inspect(err) <> Exception.message(err) =~ "16384"
    end

    test "a row for a relid no relation introduced is a value-free :decode_failure" do
      old = <<?T, 1::16, ?t, 2::32, "1\0">>
      frame = <<?D, 0::8, 999::32, ?K>> <> old

      assert {:error, err} = decode(frame)
      assert err.reason == :decode_failure
    end
  end

  describe "origin and unsupported frames" do
    test "origin: lsn + NUL-terminated name" do
      name = "bdr_1234\0"
      frame = <<?O, 0::8, 0x30::64, byte_size(name)::8, name::binary>>

      assert {:ok, [%Messages.Origin{origin_commit_lsn: 0x30, name: "bdr_1234"}], _} =
               decode(frame)
    end

    test "an unknown message type is :unsupported_message with no payload leakage" do
      assert {:error, err} = decode(<<?Z, "secret-payload">>)
      assert err.reason == :unsupported_message
      refute inspect(err) =~ "secret-payload"
    end

    test "a malformed tuple throws into the boundary and is scrubbed value-free" do
      frame = <<?I, 0::8, 16_384::32, ?N, ?T, 2::16, ?t, 99::32, "short">>

      assert {:error, err} = decode(frame)
      assert err.reason == :decode_failure
      refute inspect(err) <> Exception.message(err) =~ "short"
    end
  end

  defp paired(key, value), do: key <> <<0>> <> value <> <<0>>
end

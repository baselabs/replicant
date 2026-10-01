defmodule Replicant.Decoder.Wal2jsonTest do
  @moduledoc """
  Hand-built wal2json format-version-2 documents from the OBSERVED shapes (wal2json
  sources; anchors in `.kimosabe/intents/pglogical-wal2json-decoders.md`): `B`/`C`
  carry `lsn`/`nextlsn`/`xid`/`timestamp`; `I`/`U`/`D` carry `columns`/`identity`/`pk`
  arrays of `{name, type, typeoid, value}` objects; a column ABSENT from `columns` is
  the unchanged-TOAST sentinel; `M`/`T` mirror their actions. Values arrive under
  `numeric-data-types-as-string` (numbers as JSON strings, bool as JSON boolean, bytea
  as bare hex without the `\\x` prefix).
  """

  use ExUnit.Case, async: true

  import Bitwise

  alias Replicant.Decoder
  alias Replicant.Decoder.Messages
  alias Replicant.Decoder.Wal2json

  defp decode(payload, cache \\ Wal2json.init_cache([]), opts \\ []) do
    Decoder.decode(payload, [decoder: :wal2json, cache: cache] ++ opts)
  end

  defp doc(map) do
    map |> Jason.encode!() |> then(& &1)
  end

  describe "transaction brackets" do
    test "B: lsn → final_lsn (integer), xid, timestamp" do
      payload =
        doc(%{"action" => "B", "xid" => 42, "lsn" => "0/16E3778", "nextlsn" => "0/16E3790"})

      assert {:ok, [%Messages.Begin{} = b], _cache} = decode(payload)
      assert b.final_lsn == 0x16E3778
      assert b.xid == 42
      assert b.commit_timestamp == nil
    end

    test "B with a timestamp parses it" do
      payload =
        doc(%{
          "action" => "B",
          "timestamp" => "2026-09-29 12:00:00+00",
          "lsn" => "0/1"
        })

      assert {:ok, [%Messages.Begin{commit_timestamp: %DateTime{}}], _} = decode(payload)
    end

    test "C: commit LSN is the watermark (ADR-0009 §7)" do
      payload = doc(%{"action" => "C", "lsn" => "0/20", "nextlsn" => "0/28"})

      assert {:ok, [%Messages.Commit{} = c], _} = decode(payload)
      assert c.lsn == 0x20
      assert c.end_lsn == 0x28
    end

    test "C WITHOUT a commit LSN halts :decoder_lsn_missing (never fabricated)" do
      assert {:error, err} = decode(doc(%{"action" => "C", "nextlsn" => "0/28"}))
      assert err.reason == :decoder_lsn_missing
    end
  end

  describe "relation synthesis and cache seeding (ADR-0009 §5)" do
    test "first sight emits the synthesized Relation AHEAD of the change" do
      payload =
        doc(%{
          "action" => "I",
          "schema" => "public",
          "table" => "users",
          "columns" => [
            %{"name" => "id", "typeoid" => 20, "value" => "1", "type" => "bigint"},
            %{"name" => "name", "typeoid" => 25, "value" => "bob", "type" => "text"}
          ]
        })

      assert {:ok, [rel, ins], cache} = decode(payload)

      assert %Messages.Relation{} = rel
      assert rel.namespace == "public"
      assert rel.name == "users"
      assert [%Messages.Relation.Column{}, %Messages.Relation.Column{}] = rel.columns
      # type names resolve through the same OidDatabase call the pgoutput decoder uses
      assert Enum.map(rel.columns, & &1.type) == ["int8", "text"]
      assert rel.id == :erlang.phash2({"public", "users"})

      assert %Messages.Insert{} = ins
      assert ins.relation_id == rel.id
      assert ins.tuple_data == {"1", "bob"}

      # a second change for the same table no longer emits the relation
      payload2 =
        doc(%{
          "action" => "I",
          "schema" => "public",
          "table" => "users",
          "columns" => [
            %{"name" => "id", "typeoid" => 20, "value" => "2", "type" => "bigint"},
            %{"name" => "name", "typeoid" => 25, "value" => "amy", "type" => "text"}
          ]
        })

      assert {:ok, [%Messages.Insert{tuple_data: {"2", "amy"}}], ^cache} = decode(payload2, cache)
    end

    test "a pre-seeded cache (the connect-time catalog read) announces the relation ONCE" do
      cache =
        Wal2json.init_cache(
          relations: %{
            {"public", "users"} => %Messages.Relation{
              id: 16_384,
              namespace: "public",
              name: "users",
              replica_identity: :default,
              columns: [
                %Messages.Relation.Column{
                  name: "id",
                  flags: [:key],
                  type: "int8",
                  type_modifier: -1
                },
                %Messages.Relation.Column{
                  name: "name",
                  flags: [],
                  type: "text",
                  type_modifier: -1
                }
              ]
            }
          },
          replica_identity: %{{"public", "users"} => :default},
          relids: %{{"public", "users"} => 16_384}
        )

      payload =
        doc(%{
          "action" => "I",
          "schema" => "public",
          "table" => "users",
          "columns" => [
            %{"name" => "id", "typeoid" => 20, "value" => "1"},
            %{"name" => "name", "typeoid" => 25, "value" => "bob"}
          ]
        })

      assert {:ok,
              [
                %Messages.Relation{id: 16_384},
                %Messages.Insert{relation_id: 16_384, tuple_data: {"1", "bob"}}
              ], _} =
               decode(payload, cache)
    end

    test "a NEW column (schema drift) re-emits the relation ahead of the change" do
      base =
        doc(%{
          "action" => "I",
          "schema" => "public",
          "table" => "users",
          "columns" => [%{"name" => "id", "typeoid" => 20, "value" => "1"}]
        })

      {:ok, _, cache} = decode(base)

      drifted =
        doc(%{
          "action" => "I",
          "schema" => "public",
          "table" => "users",
          "columns" => [
            %{"name" => "id", "typeoid" => 20, "value" => "2"},
            %{"name" => "email", "typeoid" => 25, "value" => "x@y"}
          ]
        })

      assert {:ok, [rel, %Messages.Insert{}], _} = decode(drifted, cache)
      assert Enum.map(rel.columns, & &1.name) == ["id", "email"]
    end

    test "a column ABSENT from the change is the unchanged-TOAST sentinel" do
      cache =
        Wal2json.init_cache(
          relations: %{
            {"public", "users"} => %Messages.Relation{
              id: 1,
              namespace: "public",
              name: "users",
              replica_identity: :default,
              columns: [
                %Messages.Relation.Column{
                  name: "id",
                  flags: [:key],
                  type: "int8",
                  type_modifier: -1
                },
                %Messages.Relation.Column{
                  name: "blob",
                  flags: [],
                  type: "text",
                  type_modifier: -1
                }
              ]
            }
          },
          replica_identity: %{{"public", "users"} => :default}
        )

      payload =
        doc(%{
          "action" => "U",
          "schema" => "public",
          "table" => "users",
          "columns" => [%{"name" => "id", "typeoid" => 20, "value" => "1"}],
          "identity" => [%{"name" => "id", "typeoid" => 20, "value" => "1"}]
        })

      assert {:ok, [%Messages.Relation{}, %Messages.Update{tuple_data: {"1", :unchanged_toast}}],
              _} =
               decode(payload, cache)
    end
  end

  describe "dropped-column detection (absence disambiguation — ADR-0009 divergence resolution)" do
    # wal2json overloads ABSENCE: a dropped column and an untouched-TOAST column both
    # omit from the change. Two rules split them without a catalog round-trip: an
    # INSERT carries every live column (any cached column absent = dropped); a
    # FIXED-WIDTH column (int/float/bool/date/time/timestamp/interval/uuid) can never
    # be stored out-of-line, so its absence on an UPDATE = dropped. The re-emitted
    # subset Relation rides the assembler's shipped column_dropped classification.
    defp seeded_cols(cols) do
      Wal2json.init_cache(
        relations: %{
          {"public", "users"} => %Messages.Relation{
            id: 1,
            namespace: "public",
            name: "users",
            replica_identity: :default,
            columns: cols
          }
        },
        replica_identity: %{{"public", "users"} => :default}
      )
    end

    test "an INSERT missing a cached column re-emits the relation WITHOUT it (inserts carry every live column)" do
      cache =
        seeded_cols([
          %Messages.Relation.Column{name: "id", flags: [:key], type: "int8", type_modifier: -1},
          %Messages.Relation.Column{name: "blob", flags: [], type: "text", type_modifier: -1}
        ])

      payload =
        doc(%{
          "action" => "I",
          "schema" => "public",
          "table" => "users",
          "columns" => [%{"name" => "id", "typeoid" => 20, "value" => "1"}]
        })

      assert {:ok, [rel, %Messages.Insert{}], _} = decode(payload, cache)
      assert Enum.map(rel.columns, & &1.name) == ["id"]
    end

    test "an UPDATE missing a FIXED-WIDTH column re-emits the relation WITHOUT it (a fixed-width value is never TOAST-omitted)" do
      cache =
        seeded_cols([
          %Messages.Relation.Column{name: "id", flags: [:key], type: "int8", type_modifier: -1},
          %Messages.Relation.Column{name: "n", flags: [], type: "int4", type_modifier: -1}
        ])

      payload =
        doc(%{
          "action" => "U",
          "schema" => "public",
          "table" => "users",
          "columns" => [%{"name" => "id", "typeoid" => 20, "value" => "1"}],
          "identity" => [%{"name" => "id", "typeoid" => 20, "value" => "1"}]
        })

      assert {:ok, [rel, %Messages.Update{}], _} = decode(payload, cache)
      assert Enum.map(rel.columns, & &1.name) == ["id"]
    end

    test "an UPDATE missing a VARLENA column keeps the sentinel relation INTACT (no false destructive)" do
      cache =
        seeded_cols([
          %Messages.Relation.Column{name: "id", flags: [:key], type: "int8", type_modifier: -1},
          %Messages.Relation.Column{name: "blob", flags: [], type: "text", type_modifier: -1}
        ])

      payload =
        doc(%{
          "action" => "U",
          "schema" => "public",
          "table" => "users",
          "columns" => [%{"name" => "id", "typeoid" => 20, "value" => "1"}],
          "identity" => [%{"name" => "id", "typeoid" => 20, "value" => "1"}]
        })

      assert {:ok, [rel, %Messages.Update{tuple_data: {"1", :unchanged_toast}}], _} =
               decode(payload, cache)

      assert Enum.map(rel.columns, & &1.name) == ["id", "blob"]
    end
  end

  describe "identity → old-tuple classification" do
    defp seeded(replident) do
      Wal2json.init_cache(
        relations: %{
          {"public", "users"} => %Messages.Relation{
            id: 1,
            namespace: "public",
            name: "users",
            replica_identity: replident,
            columns: [
              %Messages.Relation.Column{
                name: "id",
                flags: [:key],
                type: "int8",
                type_modifier: -1
              },
              %Messages.Relation.Column{name: "name", flags: [], type: "text", type_modifier: -1}
            ]
          }
        },
        replica_identity: %{{"public", "users"} => replident}
      )
    end

    test "identity under a key identity → changed_key_tuple_data, nil-filled to full width" do
      payload =
        doc(%{
          "action" => "D",
          "schema" => "public",
          "table" => "users",
          "identity" => [%{"name" => "id", "typeoid" => 20, "value" => "7"}]
        })

      assert {:ok, [%Messages.Relation{}, %Messages.Delete{} = d], _} =
               decode(payload, seeded(:default))

      assert d.changed_key_tuple_data == {"7", nil}
      assert d.old_tuple_data == nil
    end

    test "identity under REPLICA IDENTITY FULL → old_tuple_data (the whole row)" do
      payload =
        doc(%{
          "action" => "U",
          "schema" => "public",
          "table" => "users",
          "columns" => [
            %{"name" => "id", "typeoid" => 20, "value" => "7"},
            %{"name" => "name", "typeoid" => 25, "value" => "zed"}
          ],
          "identity" => [
            %{"name" => "id", "typeoid" => 20, "value" => "7"},
            %{"name" => "name", "typeoid" => 25, "value" => "zoe"}
          ]
        })

      assert {:ok, [%Messages.Relation{}, %Messages.Update{} = u], _} =
               decode(payload, seeded(:all_columns))

      assert u.old_tuple_data == {"7", "zoe"}
      assert u.changed_key_tuple_data == nil
    end

    test "an update with NO identity delivers the new tuple only (keyless, like pgoutput)" do
      payload =
        doc(%{
          "action" => "U",
          "schema" => "public",
          "table" => "users",
          "columns" => [
            %{"name" => "id", "typeoid" => 20, "value" => "7"},
            %{"name" => "name", "typeoid" => 25, "value" => "zed"}
          ]
        })

      assert {:ok,
              [
                %Messages.Relation{},
                %Messages.Update{changed_key_tuple_data: nil, old_tuple_data: nil}
              ], _} =
               decode(payload, seeded(:default))
    end
  end

  describe "JSON-typed values back to server text (ADR-0008 parity)" do
    test "boolean → t/f; bytea gets its \\x prefix back; numeric strings pass through" do
      cache =
        Wal2json.init_cache(
          relations: %{
            {"public", "t"} => %Messages.Relation{
              id: 1,
              namespace: "public",
              name: "t",
              replica_identity: :default,
              columns: [
                %Messages.Relation.Column{
                  name: "flag",
                  flags: [],
                  type: "boolean",
                  type_modifier: -1
                },
                %Messages.Relation.Column{
                  name: "raw",
                  flags: [],
                  type: "bytea",
                  type_modifier: -1
                },
                %Messages.Relation.Column{
                  name: "amt",
                  flags: [],
                  type: "numeric",
                  type_modifier: -1
                }
              ]
            }
          },
          replica_identity: %{{"public", "t"} => :default}
        )

      payload =
        doc(%{
          "action" => "I",
          "schema" => "public",
          "table" => "t",
          "columns" => [
            %{"name" => "flag", "typeoid" => 16, "value" => true},
            %{"name" => "raw", "typeoid" => 17, "value" => "54617069727573"},
            %{"name" => "amt", "typeoid" => 1700, "value" => "1.10"}
          ]
        })

      assert {:ok,
              [
                %Messages.Relation{},
                %Messages.Insert{tuple_data: {"t", "\\x54617069727573", "1.10"}}
              ], _} =
               decode(payload, cache)
    end
  end

  describe "T and M actions" do
    test "T resolves the truncated relation from the cache (options stay empty — wal2json carries none)" do
      cache =
        Wal2json.init_cache(
          relations: %{
            {"public", "users"} => %Messages.Relation{
              id: 16_384,
              namespace: "public",
              name: "users",
              replica_identity: :default,
              columns: []
            }
          },
          replica_identity: %{{"public", "users"} => :default}
        )

      payload = doc(%{"action" => "T", "schema" => "public", "table" => "users"})

      # a TRUNCATE can be a table's FIRST action: the relation rides AHEAD (the
      # assembler would halt "truncate for uncached relation" without the announce)
      assert {:ok,
              [
                %Messages.Relation{id: 16_384},
                %Messages.Truncate{truncated_relations: [16_384], options: []}
              ], _} =
               decode(payload, cache)
    end

    test "T for an unknown table is a value-free :decode_failure" do
      assert {:error, err} =
               decode(doc(%{"action" => "T", "schema" => "public", "table" => "nope"}))

      assert err.reason == :decode_failure
    end

    test "M maps to the Message struct with the transactional split (ADR-0001)" do
      txn =
        doc(%{
          "action" => "M",
          "lsn" => "0/30",
          "transactional" => true,
          "prefix" => "txn_prefix",
          "content" => "txn_content"
        })

      assert {:ok,
              [
                %Messages.Message{
                  transactional?: true,
                  lsn: 0x30,
                  prefix: "txn_prefix",
                  content: "txn_content"
                }
              ], _} =
               decode(txn)

      nontxn =
        doc(%{
          "action" => "M",
          "lsn" => "0/31",
          "transactional" => false,
          "prefix" => "p",
          "content" => "c"
        })

      assert {:ok, [%Messages.Message{transactional?: false, lsn: 0x31}], _} = decode(nontxn)
    end
  end

  describe "value-free failure boundary" do
    test "malformed JSON is a :decode_failure with NO payload bytes anywhere" do
      payload = ~s({"action":"I","broken": "secret-row-value)

      assert {:error, err} = decode(payload)
      assert err.reason == :decode_failure
      inspected = inspect(err) <> Exception.message(err)
      refute inspected =~ "secret-row-value"
    end

    test "a non-object JSON document is a :decode_failure" do
      assert {:error, err} = decode(~s([1,2,3]))
      assert err.reason == :decode_failure
    end

    test "an unknown action is :unsupported_message, value-free" do
      assert {:error, err} = decode(doc(%{"action" => "X"}))
      assert err.reason == :unsupported_message
    end

    test "a malformed lsn string is a :decode_failure" do
      assert {:error, err} = decode(doc(%{"action" => "C", "lsn" => "not-an-lsn"}))
      assert err.reason == :decode_failure
    end
  end

  describe "typmod parsing from format_type strings (type_modifier parity)" do
    test "varlena families and plain precision match pgoutput's raw atttypmod" do
      assert Wal2json.type_modifier("character varying(10)") == 14
      assert Wal2json.type_modifier("character(3)") == 7
      assert Wal2json.type_modifier("numeric(10,2)") == (10 <<< 16) + 2 + 4
      assert Wal2json.type_modifier("numeric(10)") == (10 <<< 16) + 4
      assert Wal2json.type_modifier("timestamp(3) without time zone") == 3
      assert Wal2json.type_modifier("time(2) with time zone") == 2
      assert Wal2json.type_modifier("text") == -1
      # array columns carry no column-level typmod even when the element prints one
      assert Wal2json.type_modifier("character varying(10)[]") == -1
      assert Wal2json.type_modifier("integer[]") == -1
    end
  end

  describe "wal2json identity[] synthesis discrimination (OBSERVED plugin behavior)" do
    # wal2json synthesizes identity[] FROM THE NEW TUPLE when an update's old tuple
    # was not logged (key columns unchanged) — pgoutput delivers NO old data there.
    # The decoder must DROP that synthesized identity (old_record nil), and KEEP a
    # real one (values differ). Red proof: delete synthesized_from_new?/2's
    # all-true branch and the first assertion fails (a bogus old_record appears).
    test "identity equal to the new tuple at every key column is synthesized → dropped" do
      cache = seeded(:default)

      payload =
        doc(%{
          "action" => "U",
          "schema" => "public",
          "table" => "users",
          "columns" => [
            %{"name" => "id", "typeoid" => 20, "value" => "7"},
            %{"name" => "name", "typeoid" => 25, "value" => "zed"}
          ],
          "identity" => [
            %{"name" => "id", "typeoid" => 20, "value" => "7"}
          ]
        })

      assert {:ok, [_relation, %Messages.Update{} = u], _} = decode(payload, cache)
      assert u.changed_key_tuple_data == nil
      assert u.old_tuple_data == nil
    end

    test "identity DIFFERING from the new tuple at a key column is real → kept" do
      cache = seeded(:default)

      payload =
        doc(%{
          "action" => "U",
          "schema" => "public",
          "table" => "users",
          "columns" => [
            %{"name" => "id", "typeoid" => 20, "value" => "7"},
            %{"name" => "name", "typeoid" => 25, "value" => "zed"}
          ],
          "identity" => [
            %{"name" => "id", "typeoid" => 20, "value" => "6"}
          ]
        })

      assert {:ok, [_relation, %Messages.Update{} = u], _} = decode(payload, cache)
      assert u.changed_key_tuple_data == {"6", nil}
      assert u.old_tuple_data == nil
    end
  end

  describe "wal2json drift re-emission keeps unchanged-TOAST columns (append-only merge)" do
    # Regression: a drift re-synthesis that REBUILDS the relation from the change's
    # columns drops a TOASTed column the change omits → the assembler classifies a
    # legitimate ADD COLUMN as a destructive DROP. The merge must be append-only.
    test "an ADD COLUMN change on a table with an untouched TOASTed column keeps it" do
      base =
        doc(%{
          "action" => "I",
          "schema" => "public",
          "table" => "drift_t",
          "columns" => [
            %{"name" => "id", "typeoid" => 20, "value" => "1"},
            %{"name" => "blob", "typeoid" => 25, "value" => "big"}
          ]
        })

      {:ok, _, cache} = decode(base)

      drifted =
        doc(%{
          "action" => "I",
          "schema" => "public",
          "table" => "drift_t",
          "columns" => [
            %{"name" => "id", "typeoid" => 20, "value" => "2"},
            %{"name" => "w", "typeoid" => 25, "value" => "new"}
          ]
        })

      assert {:ok, [rel, %Messages.Insert{} = ins], _} = decode(drifted, cache)
      assert Enum.map(rel.columns, & &1.name) == ["id", "blob", "w"]
      # the absent TOASTed column surfaces as the sentinel, not as a drop
      assert ins.tuple_data == {"2", :unchanged_toast, "new"}
    end
  end
end

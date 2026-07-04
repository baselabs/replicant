defmodule Replicant.Decoder.MessagesTest do
  use ExUnit.Case, async: true

  alias Replicant.Decoder.Messages

  test "Begin/Commit carry an lsn as a single uint64, not a tuple" do
    assert %Messages.Begin{final_lsn: 0x16E3778, commit_timestamp: nil, xid: 42}
    assert %Messages.Commit{lsn: 0x16E3778, end_lsn: 0x16E3779, commit_timestamp: nil, flags: []}
    # The uint64 LSN type (vs walex's {file, offset} tuple) is enforced by the
    # @type Replicant.lsn() (dialyzer) and proven end-to-end from real bytes in
    # Task 14's conformance suite. A runtime struct check cannot enforce it (structs
    # don't constrain field types) — and Elixir 1.19's type-checker proves such a
    # refute vacuous, emitting a warning; so no such refute here.
  end

  test "Relation carries replica_identity as an atom and columns as structs" do
    col = %Messages.Relation.Column{
      name: "id",
      type: "int4",
      flags: [:key],
      type_modifier: 4_294_967_295
    }

    rel = %Messages.Relation{
      id: 16_384,
      namespace: "public",
      name: "orders",
      replica_identity: :default,
      columns: [col]
    }

    assert rel.replica_identity == :default
    assert hd(rel.columns).flags == [:key]
  end

  test "Insert/Update/Delete carry tuple_data tuples; Update carries old/key tuples" do
    assert %Messages.Insert{relation_id: 16_384, tuple_data: {"a", "b"}}

    assert %Messages.Update{
      relation_id: 16_384,
      tuple_data: {"a"},
      old_tuple_data: {"old"},
      changed_key_tuple_data: nil
    }

    assert %Messages.Delete{
      relation_id: 16_384,
      old_tuple_data: nil,
      changed_key_tuple_data: {"k"}
    }
  end

  test "Truncate, Type, Origin, Unsupported structs exist" do
    assert %Messages.Truncate{
      number_of_relations: 1,
      options: [:cascade],
      truncated_relations: [16_384]
    }

    assert %Messages.Type{id: 2950, namespace: "pg_catalog", name: "uuid"}
    assert %Messages.Origin{origin_commit_lsn: 0x1, name: "origin_a"}
    assert %Messages.Unsupported{data: "<<raw>>"}
  end
end

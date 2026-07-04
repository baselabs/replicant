defmodule Replicant.SchemaChangeTest do
  use ExUnit.Case, async: true

  alias Replicant.{Decoder.Messages, SchemaChange}

  defp rel(name, cols, identity \\ :default) do
    %Messages.Relation{
      id: 16_384,
      namespace: "public",
      name: name,
      replica_identity: identity,
      columns: cols
    }
  end

  defp col(name, type, flags \\ []) do
    %Messages.Relation.Column{name: name, type: type, flags: flags, type_modifier: 4_294_967_295}
  end

  describe "classify/2 — additive" do
    test "a new column added on an unchanged type is additive" do
      old = rel("orders", [col("id", "int4", [:key]), col("status", "text")])
      new = rel("orders", [col("id", "int4", [:key]), col("status", "text"), col("note", "text")])

      assert %SchemaChange{kind: :additive, change: :column_added, table: "orders"} =
               SchemaChange.classify(old, new)
    end
  end

  describe "classify/2 — destructive (spec §7/§9: each MUST halt)" do
    test "dropped column" do
      old = rel("orders", [col("id", "int4", [:key]), col("payload", "text")])
      new = rel("orders", [col("id", "int4", [:key])])

      assert %SchemaChange{kind: :destructive, change: :column_dropped, table: "orders"} =
               SchemaChange.classify(old, new)
    end

    test "column type change" do
      old = rel("orders", [col("amount", "int4")])
      new = rel("orders", [col("amount", "int8")])

      assert %SchemaChange{kind: :destructive, change: :type_changed, table: "orders"} =
               SchemaChange.classify(old, new)
    end

    test "replica-identity change (default → all_columns)" do
      old = rel("orders", [col("id", "int4", [:key])], :default)
      new = rel("orders", [col("id", "int4", [:key])], :all_columns)

      assert %SchemaChange{kind: :destructive, change: :replica_identity_changed, table: "orders"} =
               SchemaChange.classify(old, new)
    end

    test "type_modifier change (varchar(100) -> varchar(10)) is destructive even though the type name is unchanged" do
      # atttypmod encodes length: varchar(100) -> 104, varchar(10) -> 14. Same "varchar"
      # name, narrowing modifier that truncates data — must classify :type_changed, not proceed.
      old =
        rel("orders", [
          %Messages.Relation.Column{name: "name", type: "varchar", flags: [], type_modifier: 104}
        ])

      new =
        rel("orders", [
          %Messages.Relation.Column{name: "name", type: "varchar", flags: [], type_modifier: 14}
        ])

      assert %SchemaChange{kind: :destructive, change: :type_changed, table: "orders"} =
               SchemaChange.classify(old, new)
    end

    test "simultaneous drop + type change → dropped wins (conservative cond precedence)" do
      old = rel("orders", [col("id", "int4", [:key]), col("amount", "int4")])
      new = rel("orders", [col("amount", "int8")])

      assert %SchemaChange{kind: :destructive, change: :column_dropped, table: "orders"} =
               SchemaChange.classify(old, new)
    end
  end

  describe "classify/2 — no change" do
    test "identical relations yield nil" do
      r = rel("orders", [col("id", "int4", [:key])])
      assert SchemaChange.classify(r, r) == nil
    end
  end
end

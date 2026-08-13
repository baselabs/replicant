defmodule Replicant.Casting.TypesTest do
  use ExUnit.Case, async: true

  alias Replicant.Casting.Types

  describe "cast_record/2 scalars" do
    test "bool / int / float / numeric / uuid / text" do
      assert Types.cast_record("t", "bool") == true
      assert Types.cast_record("f", "bool") == false
      # The ::text projection the snapshot readers use emits bool as the FULL WORD
      # ("true"/"false"), not pgoutput's typoutput form ("t"/"f") the stream casts
      # above — cast_record recognizes both so snapshot and stream converge (D5).
      assert Types.cast_record("true", "bool") == true
      assert Types.cast_record("false", "bool") == false
      assert Types.cast_record("123", "int4") == 123
      assert Types.cast_record("12.5", "float8") == 12.5
      assert Decimal.equal?(Types.cast_record("12.5", "numeric"), Decimal.new("12.5"))

      uuid = "6c2e2a30-43aa-4f4c-8e40-a91b15f88c0e"
      assert Types.cast_record(uuid, "uuid") == uuid
    end

    test "jsonb decodes via Jason" do
      assert Types.cast_record("{\"a\":1}", "jsonb") == %{"a" => 1}
    end

    test "timestamptz parses to DateTime" do
      assert %DateTime{} = Types.cast_record("2024-01-15T10:30:00Z", "timestamptz")
    end

    test "NaN / Infinity become atoms" do
      assert Types.cast_record("NaN", "float8") == :nan
      assert Types.cast_record("Infinity", "float8") == :infinity
      assert Types.cast_record("-Infinity", "numeric") == :neg_infinity
    end
  end

  describe "cast_record/2 arrays" do
    test "int and text arrays" do
      assert Types.cast_record("{1,2,3}", "_int4") == [1, 2, 3]
      assert Types.cast_record("{a,b}", "_text") == ["a", "b"]
    end

    # Postgres float8out emits the SHORTEST round-tripping text form, which has NO
    # decimal dot for whole numbers ("1"), uses scientific notation for very
    # large/small magnitudes ("1e+20"), and emits bare "NaN"/"Infinity"/"-Infinity".
    # String.to_float/1 raises on all of those, so the array clauses MUST be lenient
    # exactly like the scalar "float8" clause — otherwise a double precision[]/real[]
    # column with an ordinary whole-valued element halts the pipeline fail-closed.
    test "float arrays: whole numbers, scientific notation, special values, NULL, nested" do
      assert Types.cast_record("{1,2.5}", "_float8") == [1.0, 2.5]
      assert Types.cast_record("{1e+20}", "_float8") == [1.0e20]

      assert Types.cast_record("{NaN,Infinity,-Infinity}", "_float8") ==
               [:nan, :infinity, :neg_infinity]

      # NULL token -> nil; float4 shares the clause
      assert Types.cast_record("{1,NULL}", "_float4") == [1.0, nil]

      # multidimensional nests recursively
      assert Types.cast_record("{{1,2},{3,4}}", "_float8") == [[1.0, 2.0], [3.0, 4.0]]
    end
  end

  describe "lenient fallback" do
    test "an unknown or unparseable value returns the original string, never raises" do
      assert Types.cast_record("not-a-number", "int4") == "not-a-number"
      assert Types.cast_record("anything", "totally_unknown_type") == "anything"
    end
  end
end

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

  # 1.3.0 — multidimensional array support for EVERY casted array type. ArrayParser
  # returns NESTED lists for 2-D+ literals, but only the int/float array clauses
  # recursed; every other clause fed a nested list straight into its scalar parser
  # (Decimal.new/DateTime.from_iso8601/Jason.decode/Base.decode16!...) which raised —
  # halting the pipeline fail-closed on ORDINARY Postgres data (numeric[][] etc.).
  describe "cast_record/2 multidimensional arrays" do
    test "numeric[][] / decimal[][] cast to nested Decimals, NULL preserved" do
      assert Types.cast_record("{{1.5,2},{3,NULL}}", "_numeric") == [
               [Decimal.new("1.5"), Decimal.new(2)],
               [Decimal.new(3), nil]
             ]

      assert Types.cast_record("{{4.25}}", "_decimal") == [[Decimal.new("4.25")]]
    end

    test "timestamptz[][] / timestamp[][] cast to nested DateTimes" do
      {:ok, expected_tz, 0} = DateTime.from_iso8601("2024-01-15 10:30:00+00")

      assert Types.cast_record("{{2024-01-15 10:30:00+00}}", "_timestamptz") == [
               [expected_tz]
             ]

      assert Types.cast_record("{{2024-01-15 10:30:00}}", "_timestamp") == [
               [~U[2024-01-15 10:30:00Z]]
             ]
    end

    test "jsonb[][] / json[][] decode to nested terms" do
      assert Types.cast_record("{{1,2},{3,4}}", "_jsonb") == [[1, 2], [3, 4]]
      assert Types.cast_record("{{5}}", "_json") == [[5]]
    end

    test "date[][] / time[][] cast to nested Date/Time" do
      assert Types.cast_record("{{2024-01-15,2024-02-29}}", "_date") == [
               [~D[2024-01-15], ~D[2024-02-29]]
             ]

      assert Types.cast_record("{{04:05:06,04:05:06.789}}", "_time") == [
               [~T[04:05:06], ~T[04:05:06.789]]
             ]
    end

    test "bytea[][] decodes nested binaries" do
      assert Types.cast_record("{{\\x48656c6c6f}}", "_bytea") == [["Hello"]]
    end

    test "bool 2-D leaves no uncast t/f strings" do
      assert Types.cast_record("{{t,f},{f,t}}", "_bool") == [[true, false], [false, true]]
    end
  end

  # 1.3.0 — money is locale-fragile: the old regex strip turned a de_DE "1.234,56"
  # into Decimal 1.23456 (a 100x value error). The clause now parses ONLY the strict
  # C/en-US money shapes (optional $, minus, digits, valid comma grouping, optional
  # cents) and returns the ORIGINAL string for anything else — the file's lenient
  # idiom; never a silently-wrong Decimal, never a raise.
  describe "cast_record/2 money (locale-honest)" do
    test "C-locale and valid-grouping money → Decimal (scalar + array)" do
      assert Decimal.equal?(Types.cast_record("$1234567.89", "money"), Decimal.new("1234567.89"))
      assert Decimal.equal?(Types.cast_record("$1,234.56", "money"), Decimal.new("1234.56"))
      assert Decimal.equal?(Types.cast_record("-$5.00", "money"), Decimal.new("-5.00"))

      # Canonical array_out QUOTES money elements that contain a comma.
      assert Types.cast_record(~S({"$1,234.56","$0.50"}), "_money") == [
               Decimal.new("1234.56"),
               Decimal.new("0.50")
             ]

      assert Types.cast_record(~S({{"$1,234.56"},{"$0.50"}}), "_money") == [
               [Decimal.new("1234.56")],
               [Decimal.new("0.50")]
             ]
    end

    test "non-C locale money → the ORIGINAL string, never a wrong Decimal" do
      # de_DE lc_monetary: decimal comma. Neither form matches the strict shape, so
      # both deliver raw — the sink sees the server's honest text, not a 100x error.
      assert Types.cast_record("1234,56", "money") == "1234,56"
      assert Types.cast_record("1.234,56 €", "money") == "1.234,56 €"

      # Canonical array_out quotes the comma-containing elements.
      assert Types.cast_record(~S({"1234,56"}), "_money") == ["1234,56"]
      assert Types.cast_record(~S({{"1234,56"}}), "_money") == [["1234,56"]]

      # Parenthesized negatives (some locales) also deliver raw.
      assert Types.cast_record("($1,234.56)", "money") == "($1,234.56)"
    end
  end

  # 1.3.0 — timetz delivers the raw server string. There is no Elixir type for
  # time-with-offset; the old String.slice(0..7) silently dropped BOTH the fractional
  # seconds and the offset. The interval precedent applies: raw string, no loss.
  describe "cast_record/2 timetz (raw, lossless)" do
    test "fractional seconds and offset are preserved" do
      assert Types.cast_record("04:05:06.789-08", "timetz") == "04:05:06.789-08"
      assert Types.cast_record("04:05:06+00", "timetz") == "04:05:06+00"
    end
  end

  # 1.3.0 — interval/timetz arrays deliver raw-string elements (ADR-0008). The old
  # `<<"_int", _>>` PREFIX clause captured "_interval" and Integer.parse silently
  # truncated interval text ("2 mons 3 days" -> 2) — corrupted values on ordinary
  # data, worse than a halt (live-confirmed by review).
  describe "cast_record/2 interval / timetz arrays (raw-string elements)" do
    test "interval[] keeps interval text intact at any depth" do
      assert Types.cast_record(
               ~S({{"1 day","2 mons 3 days"},{"00:05:00","1 year"}}),
               "_interval"
             ) ==
               [["1 day", "2 mons 3 days"], ["00:05:00", "1 year"]]

      assert Types.cast_record("{1 day,NULL}", "_interval") == ["1 day", nil]
    end

    test "timetz[] delivers raw-string elements (fraction + offset intact)" do
      assert Types.cast_record(~S({"04:05:06.789-08","00:30:00+00"}), "_timetz") == [
               "04:05:06.789-08",
               "00:30:00+00"
             ]
    end

    test "int arrays match their exact names; _int2vector falls to the raw literal" do
      assert Types.cast_record("{{1,2},{3,4}}", "_int4") == [[1, 2], [3, 4]]
      assert Types.cast_record("{7,8}", "_int2") == [7, 8]

      # int2vector is a catalog type whose elements are space-separated inside ONE
      # quoted string — the catch-all delivers the honest raw literal.
      assert Types.cast_record(~S({"1 2 3"}), "_int2vector") == ~S({"1 2 3"})
    end
  end
end

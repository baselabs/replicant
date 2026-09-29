defmodule Replicant.Casting.ArrayParserTest do
  use ExUnit.Case, async: true

  alias Replicant.Casting.ArrayParser

  describe "parse/1" do
    test "simple array" do
      assert ArrayParser.parse("{1,2,3}") == {:ok, ["1", "2", "3"]}
    end

    test "empty array" do
      assert ArrayParser.parse("{}") == {:ok, []}
    end

    test "quoted elements with commas" do
      assert ArrayParser.parse(~s({"hello, world","foo"})) == {:ok, ["hello, world", "foo"]}
    end

    test "nested multidimensional" do
      assert ArrayParser.parse("{{1,2},{3,4}}") == {:ok, [["1", "2"], ["3", "4"]]}
    end

    test "NULL token becomes nil" do
      assert ArrayParser.parse("{1,NULL,3}") == {:ok, ["1", nil, "3"]}
    end

    test "malformed input errors (does not raise)" do
      assert {:error, _} = ArrayParser.parse("not an array")
    end
  end

  # 1.3.0 — quote-aware nested framing. The old brace-depth walk treated { and }
  # inside QUOTED elements as structural, so canonical array_out output whose
  # elements contain UNBALANCED braces ({"a}b"} closes early, {"c{d"} late) mis-framed
  # the nested slice and the whole literal degraded to the raw string at the cast
  # boundary — a silent type divergence on ordinary data.
  describe "quote-aware nested framing" do
    test "unbalanced braces inside quoted elements are not structural" do
      assert ArrayParser.parse(~S({{"a}b","c{d"}})) == {:ok, [["a}b", "c{d"]]}
    end

    test "escaped quotes inside quoted elements of a nested array" do
      assert ArrayParser.parse(~s({{"a\\"b"}})) == {:ok, [["a\"b"]]}
    end
  end

  describe "strictness" do
    # array_out never emits consecutive commas and Postgres rejects them on input;
    # silently skipping one produced a wrong-length list instead of an error.
    test "consecutive commas are a parse error" do
      assert {:error, _} = ArrayParser.parse("{a,,b}")
    end

    test "NULL inside a nested array becomes nil" do
      assert ArrayParser.parse("{{1,NULL}}") == {:ok, [["1", nil]]}
    end

    # array_out quotes a literal "NULL" string but leaves an element merely
    # STARTING with NULL unquoted ({NULLABLE,NULLs,...}) — the NULL branch must fire
    # only when a separator or the closing brace follows (review P2, live-confirmed).
    # Note the QUOTED "NULL" is the STRING, not the nil marker.
    test "an unquoted element merely starting with NULL is ordinary text" do
      assert ArrayParser.parse(~S({NULLABLE,NULLs,"NULL",nulls,NULL_FLAG,ordinary})) ==
               {:ok, ["NULLABLE", "NULLs", "NULL", "nulls", "NULL_FLAG", "ordinary"]}
    end

    # Canonical array_out emits the quoted EMPTY string for an empty text element —
    # the spot a strictness rewrite most easily conflates with an absent element.
    test "a quoted empty string is an element, never an error" do
      assert ArrayParser.parse("{\"\",x}") == {:ok, ["", "x"]}
      assert ArrayParser.parse(~S({{""},{"a"}})) == {:ok, [[""], ["a"]]}
    end
  end

  # 1.3.0 — scale pins, DURATION-ASSERTED (review P2: a result-only pin cannot go
  # red — a chain-broken accumulator measured 425 ms at 128 KB, far under any test
  # timeout). Current parses 2 MB in ~8-23 ms; a broken append chain needs ~seconds,
  # so the 500 ms bound keeps huge headroom while still catching the regression.
  describe "large elements" do
    test "a 2 MB quoted element parses in well under a second" do
      big = String.duplicate("x", 2_000_000)
      {us, result} = :timer.tc(fn -> ArrayParser.parse("{\"#{big}\"}") end)
      assert {:ok, [^big]} = result
      assert us < 500_000
    end

    test "a 2 MB unquoted element parses in well under a second" do
      big = String.duplicate("7", 2_000_000)
      {us, result} = :timer.tc(fn -> ArrayParser.parse("{" <> big <> "}") end)
      assert {:ok, [^big]} = result
      assert us < 500_000
    end
  end
end

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
end

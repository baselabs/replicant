defmodule Replicant.IdentifierTest do
  use ExUnit.Case, async: true

  alias Replicant.Identifier

  describe "valid identifiers" do
    test "lowercase letters, digits, underscore; letter or underscore first" do
      assert :ok = Identifier.validate("orders")
      assert :ok = Identifier.validate("replicant_orders")
      assert :ok = Identifier.validate("pub1")
      assert :ok = Identifier.validate("_internal")
    end

    test "max 63 chars (Postgres NAMEDATALEN-1)" do
      assert :ok = Identifier.validate(String.duplicate("a", 63))
    end
  end

  describe "invalid identifiers (injection gate)" do
    @hostile [
      "'; DROP TABLE x; --",
      "orders; DROP TABLE x",
      "orders--",
      "Orders",
      "ORDERS",
      "1orders",
      "order$",
      "order name",
      "order\"name",
      "order'name",
      String.duplicate("a", 64),
      "order.name",
      ""
    ]

    test "every hostile form is rejected with a value-free error" do
      Enum.each(@hostile, fn bad ->
        assert {:error, :invalid_identifier} = Identifier.validate(bad),
               "expected rejection of #{inspect(bad)}"
      end)
    end

    test "non-binary input is rejected" do
      assert {:error, :invalid_identifier} = Identifier.validate(:orders)
      assert {:error, :invalid_identifier} = Identifier.validate(nil)
      assert {:error, :invalid_identifier} = Identifier.validate(123)
    end
  end

  describe "adversarial probe (SQL-injection bypass forms)" do
    # Cyrillic small letter а (U+0430) renders like ASCII 'a' but is not [a-z].
    @cyrillic_a <<0x430::utf8>>

    @bypasses [
      # Trailing-newline / \Z-vs-\z bypass — MUST be rejected (pattern uses \z).
      {"orders\n", "trailing newline"},
      {"orders\n; DROP TABLE x", "newline then injection"},
      # Leading / embedded newline.
      {"\norders", "leading newline"},
      {"or\nders", "embedded newline"},
      # Carriage return (another \Z-family line terminator).
      {"orders\r", "trailing carriage return"},
      {"orders\r\n", "trailing CRLF"},
      # Comment / statement glue.
      {"orders;--", "semicolon comment"},
      {"orders/*x*/", "block comment"},
      {"orders`", "backtick glue"},
      # Unicode lookalike — non-ASCII lowercase letter.
      {"orders" <> @cyrillic_a, "trailing Cyrillic a"},
      {@cyrillic_a <> "orders", "leading Cyrillic a"},
      # Null byte / control chars.
      {"orders\0", "null byte"},
      {"orders\t", "tab"},
      {"orders\v", "vertical tab"},
      {"orders\f", "form feed"},
      # Empty / over-length / whitespace-only.
      {"", "empty"},
      {String.duplicate("a", 64), "64 chars (over NAMEDATALEN-1)"},
      {"   ", "whitespace only"}
    ]

    test "every bypass form returns {:error, :invalid_identifier}" do
      Enum.each(@bypasses, fn {bad, label} ->
        assert {:error, :invalid_identifier} = Identifier.validate(bad),
               "expected rejection of #{label}: #{inspect(bad)}"
      end)
    end
  end
end

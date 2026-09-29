defmodule Replicant.Casting.ArrayParser do
  @moduledoc """
  Parser for PostgreSQL array literals (`array_out` output).

  Implementation inspired by Supabase Realtime and Sequin.

  Parsing is STRICT about element separation (1.3.0): `array_out` never emits an
  empty element, and Postgres rejects `{a,,b}` / `{a,}` / `{,a}` on input — so the
  parser errors on them instead of silently skipping and delivering a wrong-length
  list. Quoted elements may contain braces, commas, and escaped quotes; they are
  framing-transparent (braces inside quotes are never structural).
  """

  # credo:disable-for-this-file Credo.Check.Refactor.Nesting

  @doc """
  Parses a PostgreSQL array literal string into an Elixir list.

  Returns all elements as strings - the caller is responsible for any
  type conversion. NULL values are returned as `nil`.

  ## Parameters
    - `array_string` - PostgreSQL array literal (e.g., `"{1,2,3}"`)

  ## Returns
    - `{:ok, list}` - Successfully parsed array as nested list
    - `{:error, reason}` - Parsing failed with reason

  ## Examples

      # Simple arrays
      Replicant.Casting.ArrayParser.parse("{1,2,3}")
      {:ok, ["1", "2", "3"]}

      # Empty arrays
      Replicant.Casting.ArrayParser.parse("{}")
      {:ok, []}

      # Arrays with quoted strings
      Replicant.Casting.ArrayParser.parse("{\"hello, world\",\"foo\"}")
      {:ok, ["hello, world", "foo"]}

      # Nested arrays (braces inside quoted elements are not structural)
      Replicant.Casting.ArrayParser.parse("{{\\\"a}b\\\"}}")
      {:ok, [["a}b"]]}

      # NULL values
      Replicant.Casting.ArrayParser.parse("{1,NULL,3}")
      {:ok, ["1", nil, "3"]}
  """
  @spec parse(binary()) :: {:ok, [term()]} | {:error, String.t()}
  def parse(array_string) when is_binary(array_string) do
    case array_string do
      "{}" -> {:ok, []}
      <<"{", rest::binary>> -> parse_array_contents(rest, [], "", false)
      _ -> {:error, "Invalid array format - must start with {"}
    end
  end

  # Main parsing loop. `elem_done?` is true only right after an element that appended
  # to `acc` directly and was immediately followed by the CLOSING brace (the
  # element-appending clauses pass `false` when a comma follows, and the closing
  # brace consumes the `true` on the next reduction). A comma or close with
  # `current == ""` and `elem_done? == false` is an EMPTY element: `array_out` never
  # emits one and Postgres rejects it on input, so it errors here — never a silent
  # skip that delivers a wrong-length list (1.3.0).
  defp parse_array_contents(<<>>, _acc, _current, _elem_done?),
    do: {:error, "Unexpected end of array - missing closing }"}

  defp parse_array_contents(<<"}", _rest::binary>>, _acc, "", false),
    do: {:error, "Trailing comma or empty array element"}

  defp parse_array_contents(<<"}", _rest::binary>>, acc, "", true),
    do: {:ok, Enum.reverse(acc)}

  defp parse_array_contents(<<"}", _rest::binary>>, acc, current, _elem_done?),
    do: {:ok, Enum.reverse([current | acc])}

  # Handle NULL values - the token is the null marker ONLY when a separator or the
  # closing brace follows (array_out quotes a literal "NULL" string, but an
  # UNQUOTED element merely STARTING with NULL — {NULLABLE,NULLs,...} — is ordinary
  # text the server emits bare; review P2, live-confirmed). Anything else falls
  # through to character accumulation.
  defp parse_array_contents(
         <<"NULL", rest::binary>>,
         acc,
         "",
         false
       )
       when binary_part(rest, 0, 1) == "," or binary_part(rest, 0, 1) == "}" do
    case rest do
      <<",", rest::binary>> -> parse_array_contents(rest, [nil | acc], "", false)
      <<"}", _::binary>> -> parse_array_contents(rest, [nil | acc], "", true)
    end
  end

  # Handle nested arrays - capture the balanced slice (quote-aware), then parse it
  defp parse_array_contents(<<"{", rest::binary>>, acc, "", false) do
    case parse_nested_array(rest, 1, false, "{") do
      {:ok, nested_content, remaining} ->
        case parse(nested_content) do
          {:ok, nested_array} ->
            case remaining do
              <<",", rest::binary>> ->
                parse_array_contents(rest, [nested_array | acc], "", false)

              <<"}", _::binary>> ->
                parse_array_contents(remaining, [nested_array | acc], "", true)

              <<>> ->
                {:error, "Unexpected end of array - missing closing }"}

              _ ->
                {:error, "Invalid character after nested array"}
            end

          {:error, _} = error ->
            error
        end

      {:error, _} = error ->
        error
    end
  end

  # Handle quoted strings - delegate to specialized parser
  defp parse_array_contents(<<"\"", rest::binary>>, acc, "", false),
    do: parse_quoted_string(rest, acc, "")

  # Handle comma separator - an empty current is a consecutive/leading comma: every
  # element-appending path (scalar via the comma below, NULL/quoted/nested via their
  # own clauses) leaves `current == ""` with `elem_done? == false`, so an empty
  # current here is always a malformed empty element.
  defp parse_array_contents(<<",", _rest::binary>>, _acc, "", false),
    do: {:error, "Consecutive commas or empty array element"}

  defp parse_array_contents(<<",", rest::binary>>, acc, current, _elem_done?),
    do: parse_array_contents(rest, [current | acc], "", false)

  # Handle regular characters - accumulate into current element
  defp parse_array_contents(<<char, rest::binary>>, acc, current, elem_done?),
    do: parse_array_contents(rest, acc, current <> <<char>>, elem_done?)

  # Parse quoted string - handle unterminated string error
  defp parse_quoted_string(<<>>, _acc, _buffer),
    do: {:error, "Unexpected end of array - unterminated quoted string"}

  # Handle escape sequences within quoted strings
  defp parse_quoted_string(<<"\\", escaped, rest::binary>>, acc, buffer) do
    case escaped do
      ?\\ -> parse_quoted_string(rest, acc, buffer <> "\\")
      ?\" -> parse_quoted_string(rest, acc, buffer <> "\"")
      _ -> parse_quoted_string(rest, acc, buffer <> "\\" <> <<escaped>>)
    end
  end

  # End of quoted string - must be followed by comma or closing brace
  defp parse_quoted_string(<<"\"", rest::binary>>, acc, buffer) do
    case rest do
      <<",", rest::binary>> -> parse_array_contents(rest, [buffer | acc], "", false)
      <<"}", _::binary>> -> parse_array_contents(rest, [buffer | acc], "", true)
      _ -> {:error, "Invalid character after quoted string"}
    end
  end

  defp parse_quoted_string(<<char, rest::binary>>, acc, buffer),
    do: parse_quoted_string(rest, acc, buffer <> <<char>>)

  # Parse a nested array slice, tracking whether we are INSIDE a quoted element so
  # braces there are never structural (1.3.0). `array_out` quotes any element whose
  # text contains a brace, so a quoted `{`/`}` in the walk is element data.
  defp parse_nested_array(<<>>, _depth, _in_string?, _buffer),
    do: {:error, "Unexpected end of array - unclosed nested array"}

  # Inside a quoted element, an escaped character never toggles string state
  defp parse_nested_array(<<"\\", char, rest::binary>>, depth, true, buffer),
    do: parse_nested_array(rest, depth, true, buffer <> "\\" <> <<char>>)

  defp parse_nested_array(<<"\"", rest::binary>>, depth, in_string?, buffer),
    do: parse_nested_array(rest, depth, not in_string?, buffer <> "\"")

  # Structural braces - only outside quoted elements
  defp parse_nested_array(<<"{", rest::binary>>, depth, false, buffer),
    do: parse_nested_array(rest, depth + 1, false, buffer <> "{")

  defp parse_nested_array(<<"}", rest::binary>>, depth, false, buffer) when depth > 1,
    do: parse_nested_array(rest, depth - 1, false, buffer <> "}")

  defp parse_nested_array(<<"}", rest::binary>>, 1, false, buffer),
    do: {:ok, buffer <> "}", rest}

  # Regular character (including braces while inside a quoted element)
  defp parse_nested_array(<<char, rest::binary>>, depth, in_string?, buffer),
    do: parse_nested_array(rest, depth, in_string?, buffer <> <<char>>)
end

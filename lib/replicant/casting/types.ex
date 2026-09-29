defmodule Replicant.Casting.Types do
  @moduledoc """
  Cast from Postgres to Elixir types

  Implementation inspired by Cainophile, Supabase Realtime and Sequin.
  Vendored from walex 4.8.0 (`WalEx.Casting.Types`).

  ## Raise-sites (decode-boundary contract)

  `cast_record/2` is *lenient by default*: every clause that parses a scalar
  wraps the parse in a `case`/`Integer.parse`/`Float.parse` and falls back to
  the original string on failure, and the catch-all clause returns the value
  unchanged for unknown types. Parse-*failures* therefore never raise.

  Three clauses can still raise on *truly-malformed* input, because they call
  a bang/raising function with no local rescue:

    * `Decimal.new/1` (the `"numeric"` and `"decimal"` scalar clauses and the
      `"_numeric"`/`"_decimal"` array clauses) raises `Decimal.Error` on a
      malformed numeric string.
    * `Base.decode16!/2` (the `"bytea"` scalar clause and the `"_bytea"` array
      clause) raises `ArgumentError` on non-hex bytea payloads.
    * `DateTime.from_naive!/2` (the `"timestamp"` scalar clause and the
      `"_timestamp"` array clause) raises on an invalid naive datetime; in
      practice the preceding `NaiveDateTime.from_iso8601/1` guards it, but the
      bang call is retained verbatim from upstream.

  `Jason.decode/1` (the `"jsonb"`/`"json"` scalar and `"_jsonb"`/`"_json"`
  array clauses) does NOT raise — it returns `{:error, _}`, which the
  surrounding `case` collapses to the lenient fallback. `money` (1.3.0) is
  lenient too: it parses only the strict C/en-US shapes and delivers the
  original string otherwise, because money output is locale-dependent
  (`lc_monetary`) and a forced `Decimal` is a silently WRONG value under a
  non-C locale — see ADR-0008. `timetz` (1.3.0) delivers the raw string
  (no Elixir type carries a time WITH its offset; the old truncation silently
  dropped fractional seconds and offset).

  Because malformed numeric/bytea inputs raise *through* `cast_record/2`, the
  Assembler (Task 13) MUST invoke `cast_record/2` inside a decode boundary —
  either `Replicant.Decoder.decode/1` (Task 8) or its own `try/rescue` — so a
  single malformed cell scrubs to a boundary error rather than crashing the
  pipeline. Do NOT widen `cast_record/2` to swallow these: the lenient
  fallback already covers ordinary parse-failures; only genuinely-malformed
  input reaches the raising path, and the boundary is the correct place to
  scrub it.

  ## Array clauses (multidimensional, 1.3.0)

  Every casted array type recurses through `cast_array_elements/2`, so a 2-D+
  literal (`numeric[][]`, `timestamptz[][]`, …) casts element-wise exactly like
  its scalar clause — including the raise-sites above (which the decode
  boundary scrubs) and the lenient fallbacks. Only `_int*`/`_float*` recursed
  before 1.3.0; every other array clause fed a nested LIST into its scalar
  parser and raised on ordinary Postgres data.
  """

  # credo:disable-for-this-file Credo.Check.Refactor.Nesting

  alias Replicant.Casting.ArrayParser

  # The strict C/en-US money shape (see the "money" clause and ADR-0008): optional
  # leading -, optional $, digits with valid ,### grouping (at least one group) or
  # plain digits, optional fraction. Anything else (a non-C lc_monetary form like
  # "1.234,56" or "1234,56", parenthesized negatives) delivers the ORIGINAL string.
  @money_shape ~r/\A-?\$?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?\z/

  @doc """
  Casts a PostgreSQL string value to its appropriate Elixir type.

  ## Examples

      iex> cast_record("t", "bool")
      true

      iex> cast_record("123", "int4")
      123

      iex> cast_record("123.45", "numeric")
      #Decimal<123.45>

      iex> cast_record("{1,2,3}", "_int4")
      [1, 2, 3]

      iex> cast_record("2024-01-15T10:30:00Z", "timestamptz")
      #DateTime<2024-01-15 10:30:00Z>

  Special values like NaN and Infinity are handled:

      iex> cast_record("NaN", "float8")
      :nan

  Returns the original value if casting fails.
  """
  @spec cast_record(binary(), term()) :: term()
  def cast_record("t", "bool"), do: true
  def cast_record("f", "bool"), do: false

  # The `::text` projection the snapshot readers use emits bool as the full word
  # ("true"/"false"), NOT pgoutput's typoutput form ("t"/"f") the stream casts above.
  # Recognize both so a snapshot row and a streamed row deliver the SAME boolean for a
  # bool column (spec §2 convergence). The stream never sends the full-word form, so these
  # clauses only fire on the snapshot path — they do not change stream decoding.
  def cast_record("true", "bool"), do: true
  def cast_record("false", "bool"), do: false

  # Handle interval type before general integer pattern
  def cast_record(record, "interval") when is_binary(record), do: record

  # Handle special numeric values before general numeric patterns
  def cast_record("NaN", type) when type in ["float4", "float8", "numeric"], do: :nan
  def cast_record("Infinity", type) when type in ["float4", "float8", "numeric"], do: :infinity

  def cast_record("-Infinity", type) when type in ["float4", "float8", "numeric"],
    do: :neg_infinity

  def cast_record(record, <<"int", _::binary>>) when is_binary(record) do
    case Integer.parse(record) do
      {int, _} ->
        int

      :error ->
        record
    end
  end

  def cast_record(record, <<"float", _::binary>>) when is_binary(record) do
    case Float.parse(record) do
      {float, _} ->
        float

      :error ->
        record
    end
  end

  def cast_record(record, "numeric") when is_binary(record), do: Decimal.new(record)
  def cast_record(record, "decimal"), do: cast_record(record, "numeric")

  def cast_record(record, "timestamp") when is_binary(record) do
    case NaiveDateTime.from_iso8601(record) do
      {:ok, %NaiveDateTime{} = naive} ->
        DateTime.from_naive!(naive, "Etc/UTC")

      _ ->
        record
    end
  end

  def cast_record(record, "timestamptz") when is_binary(record) do
    case DateTime.from_iso8601(record) do
      {:ok, %DateTime{} = date_time, _offset} ->
        date_time

      _ ->
        record
    end
  end

  def cast_record(record, "jsonb") when is_binary(record) do
    case Jason.decode(record) do
      {:ok, json} ->
        json

      _ ->
        record
    end
  end

  def cast_record(record, "json"), do: cast_record(record, "jsonb")

  def cast_record(record, "uuid") when is_binary(record), do: record

  def cast_record(record, "date") when is_binary(record) do
    case Date.from_iso8601(record) do
      {:ok, date} -> date
      _ -> record
    end
  end

  def cast_record(record, "time") when is_binary(record) do
    case Time.from_iso8601(record) do
      {:ok, time} -> time
      _ -> record
    end
  end

  # 1.3.0 — timetz delivers the raw server string: there is no Elixir type for
  # time-WITH-offset, and the old String.slice(0..7) silently dropped both the
  # fractional seconds and the offset. Same precedent as "interval" (ADR-0008).
  def cast_record(record, "timetz") when is_binary(record), do: record

  # 1.3.0 — money output is LOCALE-DEPENDENT (lc_monetary). The old regex strip turned
  # a de_DE "1.234,56" into Decimal 1.23456 — a silent 100x value error. Parse ONLY
  # the strict C/en-US shapes (@money_shape) and deliver the original string
  # otherwise (ADR-0008): never a silently-wrong Decimal, never a raise.
  def cast_record(record, "money") when is_binary(record) do
    if Regex.match?(@money_shape, record) do
      record |> String.replace(~r/[$,]/, "") |> Decimal.new()
    else
      record
    end
  end

  def cast_record(record, "bytea") when is_binary(record) do
    # PostgreSQL bytea hex format starts with \x
    if String.starts_with?(record, "\\x") do
      record
      |> String.slice(2..-1//1)
      |> Base.decode16!(case: :mixed)
    else
      record
    end
  end

  def cast_record(record, "inet") when is_binary(record), do: record
  def cast_record(record, "cidr") when is_binary(record), do: record
  def cast_record(record, "macaddr") when is_binary(record), do: record
  def cast_record(record, "macaddr8") when is_binary(record), do: record

  def cast_record(record, "xml") when is_binary(record), do: record

  # Geometric types - return as strings for now
  def cast_record(record, "point") when is_binary(record), do: record
  def cast_record(record, "line") when is_binary(record), do: record
  def cast_record(record, "lseg") when is_binary(record), do: record
  def cast_record(record, "box") when is_binary(record), do: record
  def cast_record(record, "path") when is_binary(record), do: record
  def cast_record(record, "polygon") when is_binary(record), do: record
  def cast_record(record, "circle") when is_binary(record), do: record

  # Range types - return as strings for now
  def cast_record(record, "int4range") when is_binary(record), do: record
  def cast_record(record, "int8range") when is_binary(record), do: record
  def cast_record(record, "numrange") when is_binary(record), do: record
  def cast_record(record, "tsrange") when is_binary(record), do: record
  def cast_record(record, "tstzrange") when is_binary(record), do: record
  def cast_record(record, "daterange") when is_binary(record), do: record

  # Text search types
  def cast_record(record, "tsvector") when is_binary(record), do: record
  def cast_record(record, "tsquery") when is_binary(record), do: record

  # Other specialized types
  def cast_record(record, "bit") when is_binary(record), do: record
  def cast_record(record, "varbit") when is_binary(record), do: record
  def cast_record(record, "oid") when is_binary(record), do: record
  def cast_record(record, "regclass") when is_binary(record), do: record
  def cast_record(record, "regproc") when is_binary(record), do: record
  def cast_record(record, "regtype") when is_binary(record), do: record
  def cast_record(record, "regrole") when is_binary(record), do: record
  def cast_record(record, "regnamespace") when is_binary(record), do: record

  # PostgreSQL internal types
  def cast_record(record, "name") when is_binary(record), do: record
  def cast_record(record, "pg_lsn") when is_binary(record), do: record
  def cast_record(record, "pg_snapshot") when is_binary(record), do: record
  def cast_record(record, "txid_snapshot") when is_binary(record), do: record

  # Array type casting - integer arrays with support for multidimensional arrays.
  # EXACT names, not a `<<"_int", _>>` prefix: `_interval` (OID 1187) and
  # `_int2vector` previously fell into this clause and `Integer.parse` silently
  # truncated interval text ("2 mons 3 days" -> 2) — corrupted values, worse than
  # a halt (review P1, live-confirmed). `_interval`/`_timetz` deliver raw-string
  # elements via the text-like clause below, mirroring their scalar raw-string
  # clauses (ADR-0008); `_int2vector` falls to the catch-all (raw literal).
  # Lenient (Integer.parse fallback) so a non-integer token returns unchanged rather
  # than raising — matches the scalar "int*" clause. Postgres int output is always
  # well-formed, so the fallback is defensive, not load-bearing.
  def cast_record(array_string, column_type)
      when is_binary(array_string) and column_type in ["_int2", "_int4", "_int8"] do
    case ArrayParser.parse(array_string) do
      {:ok, elements} ->
        cast_array_elements(elements, &cast_int_element/1)

      {:error, _} ->
        array_string
    end
  end

  # Array type casting - float arrays. MUST be lenient: Postgres float4out/float8out
  # emits the shortest round-tripping text form, which has no decimal dot for whole
  # numbers ("1"), uses scientific notation for large/small magnitudes ("1e+20"), and
  # emits bare "NaN"/"Infinity"/"-Infinity". String.to_float/1 raises on ALL of those,
  # so a double precision[]/real[] column with an ordinary whole-valued element would
  # halt the pipeline fail-closed — the array clause mirrors the scalar "float*" clause
  # (Float.parse + the special-value atoms) instead.
  def cast_record(array_string, column_type)
      when is_binary(array_string) and column_type in ["_float4", "_float8"] do
    case ArrayParser.parse(array_string) do
      {:ok, elements} ->
        cast_array_elements(elements, &cast_float_element/1)

      {:error, _} ->
        array_string
    end
  end

  # Array type casting - text-like arrays. `_interval`/`_timetz` deliver their
  # elements as raw strings at any depth (no faithful Elixir representation —
  # ADR-0008; the scalar clauses deliver raw strings too).
  def cast_record(array_string, column_type)
      when is_binary(array_string) and
             column_type in ["_text", "_varchar", "_interval", "_timetz"] do
    case ArrayParser.parse(array_string) do
      {:ok, elements} -> elements
      {:error, _} -> array_string
    end
  end

  # Array type casting - boolean arrays. 1.3.0: recurses (a 2-D literal left
  # inner "t"/"f" strings uncast before); the snapshot path's full-word
  # "true"/"false" form is accepted at any depth, mirroring the scalar clause.
  def cast_record(array_string, "_bool") when is_binary(array_string) do
    case ArrayParser.parse(array_string) do
      {:ok, elements} -> cast_array_elements(elements, &cast_bool_element/1)
      {:error, _} -> array_string
    end
  end

  # Array type casting - numeric/decimal arrays. 1.3.0: recurses for 2-D+
  # literals; keeps the scalar clause's raise-site (Decimal.new on malformed)
  # and its special-value atoms.
  def cast_record(array_string, "_numeric") when is_binary(array_string) do
    case ArrayParser.parse(array_string) do
      {:ok, elements} -> cast_array_elements(elements, &cast_numeric_element/1)
      {:error, _} -> array_string
    end
  end

  def cast_record(array_string, "_decimal"), do: cast_record(array_string, "_numeric")

  # Array type casting - timestamptz arrays. 1.3.0: recurses; lenient per element
  # (falls back to the original string), mirroring the scalar clause.
  def cast_record(array_string, "_timestamptz") when is_binary(array_string) do
    case ArrayParser.parse(array_string) do
      {:ok, elements} -> cast_array_elements(elements, &cast_timestamptz_element/1)
      {:error, _} -> array_string
    end
  end

  # Array type casting - timestamp arrays. 1.3.0: recurses; keeps the scalar
  # clause's raise-site (DateTime.from_naive! behind the from_iso8601 guard).
  def cast_record(array_string, "_timestamp") when is_binary(array_string) do
    case ArrayParser.parse(array_string) do
      {:ok, elements} -> cast_array_elements(elements, &cast_timestamp_element/1)
      {:error, _} -> array_string
    end
  end

  # Array type casting - UUID arrays
  def cast_record(array_string, "_uuid") when is_binary(array_string) do
    case ArrayParser.parse(array_string) do
      {:ok, elements} -> elements
      {:error, _} -> array_string
    end
  end

  # Array type casting - JSONB arrays. 1.3.0: recurses; lenient per element.
  def cast_record(array_string, "_jsonb") when is_binary(array_string) do
    case ArrayParser.parse(array_string) do
      {:ok, elements} -> cast_array_elements(elements, &cast_json_element/1)
      {:error, _} -> array_string
    end
  end

  def cast_record(array_string, "_json"), do: cast_record(array_string, "_jsonb")

  # Array type casting - date arrays. 1.3.0: recurses; lenient per element.
  def cast_record(array_string, "_date") when is_binary(array_string) do
    case ArrayParser.parse(array_string) do
      {:ok, elements} -> cast_array_elements(elements, &cast_date_element/1)
      {:error, _} -> array_string
    end
  end

  # Array type casting - time arrays. 1.3.0: recurses; lenient per element.
  def cast_record(array_string, "_time") when is_binary(array_string) do
    case ArrayParser.parse(array_string) do
      {:ok, elements} -> cast_array_elements(elements, &cast_time_element/1)
      {:error, _} -> array_string
    end
  end

  # Array type casting - network address arrays (inet, cidr, macaddr)
  def cast_record(array_string, "_inet") when is_binary(array_string) do
    case ArrayParser.parse(array_string) do
      {:ok, elements} -> elements
      {:error, _} -> array_string
    end
  end

  def cast_record(array_string, "_cidr") when is_binary(array_string) do
    case ArrayParser.parse(array_string) do
      {:ok, elements} -> elements
      {:error, _} -> array_string
    end
  end

  def cast_record(array_string, "_macaddr") when is_binary(array_string) do
    case ArrayParser.parse(array_string) do
      {:ok, elements} -> elements
      {:error, _} -> array_string
    end
  end

  # Array type casting - money arrays. 1.3.0: recurses; shares the scalar clause's
  # locale-honest strict shape (Decimal for C/en-US forms, original string otherwise).
  def cast_record(array_string, "_money") when is_binary(array_string) do
    case ArrayParser.parse(array_string) do
      {:ok, elements} -> cast_array_elements(elements, &cast_money_element/1)
      {:error, _} -> array_string
    end
  end

  # Array type casting - bytea arrays. 1.3.0: recurses; keeps the scalar clause's
  # raise-site (Base.decode16! on non-hex payloads).
  def cast_record(array_string, "_bytea") when is_binary(array_string) do
    case ArrayParser.parse(array_string) do
      {:ok, elements} -> cast_array_elements(elements, &cast_bytea_element/1)
      {:error, _} -> array_string
    end
  end

  # Fallback - return record unchanged if no specific casting is defined
  def cast_record(record, _column_type) do
    record
  end

  @doc false
  # Helper function to recursively cast array elements, supporting nested arrays
  defp cast_array_elements(elements, cast_fn) do
    Enum.map(elements, fn
      nil ->
        nil

      elem when is_list(elem) ->
        # Handle nested arrays recursively
        cast_array_elements(elem, cast_fn)

      elem ->
        cast_fn.(elem)
    end)
  end

  # Lenient int element: Integer.parse falls back to the original string on a
  # non-integer token rather than raising (parity with the scalar "int*" clause).
  defp cast_int_element(elem) do
    case Integer.parse(elem) do
      {int, _} -> int
      :error -> elem
    end
  end

  # Lenient float element: handles the special-value atoms Postgres emits
  # ("NaN"/"Infinity"/"-Infinity") and parses with Float.parse, falling back to the
  # original string. Parity with the scalar "float*" clause — never raises.
  defp cast_float_element("NaN"), do: :nan
  defp cast_float_element("Infinity"), do: :infinity
  defp cast_float_element("-Infinity"), do: :neg_infinity

  defp cast_float_element(elem) do
    case Float.parse(elem) do
      {float, _} -> float
      :error -> elem
    end
  end

  # Bool element: both pgoutput's "t"/"f" and the snapshot ::text projection's
  # full-word form, at any depth; anything else passes through unchanged.
  defp cast_bool_element("t"), do: true
  defp cast_bool_element("f"), do: false
  defp cast_bool_element("true"), do: true
  defp cast_bool_element("false"), do: false
  defp cast_bool_element(other), do: other

  # Numeric element: keeps the scalar clause's raise-site (Decimal.new on malformed)
  # plus its special-value atoms.
  defp cast_numeric_element("NaN"), do: :nan
  defp cast_numeric_element("Infinity"), do: :infinity
  defp cast_numeric_element("-Infinity"), do: :neg_infinity
  defp cast_numeric_element(elem), do: Decimal.new(elem)

  defp cast_timestamptz_element(elem) do
    case DateTime.from_iso8601(elem) do
      {:ok, %DateTime{} = dt, _offset} -> dt
      _ -> elem
    end
  end

  defp cast_timestamp_element(elem) do
    case NaiveDateTime.from_iso8601(elem) do
      {:ok, %NaiveDateTime{} = naive} -> DateTime.from_naive!(naive, "Etc/UTC")
      _ -> elem
    end
  end

  defp cast_json_element(elem) do
    case Jason.decode(elem) do
      {:ok, json} -> json
      _ -> elem
    end
  end

  defp cast_date_element(elem) do
    case Date.from_iso8601(elem) do
      {:ok, date} -> date
      _ -> elem
    end
  end

  defp cast_time_element(elem) do
    case Time.from_iso8601(elem) do
      {:ok, time} -> time
      _ -> elem
    end
  end

  # money/bytea array elements reuse the scalar clauses directly — they are total
  # and carry exactly the semantics (strict shape / raise-site) the array needs.
  defp cast_money_element(elem), do: cast_record(elem, "money")
  defp cast_bytea_element(elem), do: cast_record(elem, "bytea")
end

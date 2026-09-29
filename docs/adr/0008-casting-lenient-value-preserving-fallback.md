# ADR-0008: Casting contract — lenient value-preserving fallback for locale-dependent and unrepresentable types

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** replicant maintainer (1.3.0 audit remediation; ships in 1.3.0)

## Context

`Replicant.Casting.Types.cast_record/2` converts each decoded `pgoutput` text value into
the Elixir term a sink receives in `%Change{}.record`. Three defects forced a contract
decision rather than spot fixes:

1. **Multidimensional arrays crashed on ordinary data.** `ArrayParser.parse/1` returns
   nested lists for 2-D+ literals, but only the `_int*`/`_float*` array clauses recursed;
   every other clause fed a nested list straight into its scalar parser
   (`Decimal.new/1`, `DateTime.from_iso8601/1`, `Jason.decode/1`, …), which raised —
   a fail-closed halt (value-free, but a halt) on a plain `numeric[][]` column.
2. **`money` was locale-fragile.** Money output is governed by the server's
   `lc_monetary`. The regex strip (`String.replace(~r/[^\d.-]/, "")`) turned a
   `de_DE` `"1.234,56"` into `Decimal 1.23456` — a silently wrong 100x value — and
   multi-dot forms raised.
3. **`timetz` silently lost data.** `String.slice(record, 0..7)` dropped both the
   fractional seconds and the offset; there is no Elixir type for time-with-offset.
   `interval` already delivers the raw string for exactly this reason.

## Decision

- **`cast_record/2` is lenient by default, with a documented small set of raise-sites**
  (`Decimal.new` on malformed `numeric`, `Base.decode16!` on non-hex `bytea`,
  `DateTime.from_naive!` on `timestamp`) that the decode boundary scrubs value-free.
  This part is unchanged; ADR-0003 governs the boundary.
- **Every casted array type recurses element-wise** through `cast_array_elements/2`
  with a per-type scalar caster mirroring its scalar clause exactly — raise-sites stay
  raise-sites, lenient stays lenient. No array type halts on legitimate nested data.
- **Locale-dependent money parses only the strict C/en-US shapes** (optional leading
  minus and `$`, digits with valid `,###` grouping or plain digits, optional cents) and
  delivers the **original string otherwise** — never a silently-wrong `Decimal`, never
  a raise. A forced `Decimal` cannot be honest under an unknown `lc_monetary`.
- **Types with no faithful Elixir representation deliver the raw server string**
  (`timetz`, `interval`). Information-preserving beats lossy convenience.
- **The array literal parser is strict where Postgres is strict** (empty elements,
  unbalanced framing) and quote-aware (braces inside quoted elements are element
  data), because a silent mis-parse of `array_out` output is a wrong-length or
  wrong-typed `record` — worse than a scrubbed error.
- **`Replicant.lsn_from_string/1` returns `{:ok, lsn} | {:error, :invalid_lsn}`**
  instead of raising. A public function of a published Hex package must never put a
  caller's input bytes into an exception message (a crash report is a log surface —
  Critical Rule 1). This is a **public return-shape change shipped in 1.3.0 (a minor)
  by owner decision**: the affected surface is narrow (LSN display-string parsing),
  the old shape could embed input in a raise, and the ecosystem is young; the
  CHANGELOG carries an explicit upgrade note.

## Options considered

- **Always deliver `money` as a raw string** — breaks correct C/en-US consumers for no
  gain; rejected.
- **Detect the server locale (`SHOW lc_monetary`) and parse accordingly** — connection
  coupling plus a locale-table this library would have to maintain forever; rejected.
  The raw-string fallback is locale-honest without a table.
- **A `{Time, offset}` tuple for `timetz`** — invents a public shape no consumer asked
  for; the raw string matches the `interval` precedent; rejected (revisit on demand).
- **Widen `cast_record/2` to swallow all raises** — explicitly rejected: the
  moduledoc's raise-site contract predates this ADR; only genuinely-malformed input
  reaches the raising path, and the boundary (ADR-0003) is the correct scrub point.

## Consequences

- A sink receiving `money`/`timetz` must accept `Decimal | String.t` / `String.t`
  respectively; downstream adapters (e.g. `ash_replicant`) classify these types
  themselves and were checked before release.
- Multidimensional arrays of every casted type now deliver nested terms with the same
  per-element semantics as their scalar clauses.
- `type_modifier` on `Relation.Column`/`Change.Column` decodes as signed int32
  (Postgres `atttypmod` is signed; `-1` is the ubiquitous "no modifier" marker and
  previously surfaced as `4294967295`).
- The getting-started Livebook demonstrates the casting contract against a live
  server and its CI test asserts the observed values on every push.

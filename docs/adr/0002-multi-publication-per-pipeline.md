# ADR-0002: Multi-publication per pipeline (discovery union + fail-closed existence check)

**Status:** Accepted
**Date:** 2026-07-14
**Deciders:** replicant maintainer (feature A3; spec `2026-07-11-replicant-messages-multipub-design.md` §5.3)

## Context

A `replicant` pipeline consumed exactly one publication (`:publication` was a single string).
Real deployments need to consume **multiple publications through one replication slot** (pgoutput
accepts a comma-list of `publication_names`). Adding this touches three security- and
correctness-sensitive surfaces:

1. The identifier interpolation surface (Critical Rule 2 — the replication simple-query protocol
   cannot bind `$1`, so publication names are interpolated and must be allowlist-validated).
2. Snapshot/PK/column **discovery** queries, which enumerate the tables to snapshot and their PK
   and column sets from `pg_publication_tables`.
3. The failure mode when a requested publication does not exist on the server.

Two facts were probe-established on live PG16 during design:

- **`START_REPLICATION` with a missing publication does NOT error** — it silently streams the
  existing subset. So a typo'd publication would silently under-replicate.
- **A table in two publications produces two `pg_publication_tables` rows**, and the
  `pg_class`/`pg_index`/`pg_attribute` joins fan out per pubname-row, so a bare `pubname = ANY($1)`
  duplicates `array_agg` PK/column entries (`["id","id"]`).

## Decision

- **`publication: String | [String]`.** A single string is the **byte-unchanged default path**
  (the published 0.1.0 START_REPLICATION is reproduced exactly); a list enables multi-publication.
  Every name is validated via `Identifier.validate/1` and normalized to a list. (`config.ex`.)
- **Discovery queries drive from a DISTINCT table set:**
  `(SELECT DISTINCT schemaname, tablename FROM pg_publication_tables WHERE pubname = ANY($1)) p`
  THEN join — collapsing the pubname dimension before the joins so `array_agg` never duplicates
  PK/column entries (decision #19; probe-proven `pk_raw = ["id"]` for a shared table).
  (`query_builder.ex` `publication_tables/1`, `pk_columns/0`, `table_columns/0`.)
- **A new fail-closed connect-chain `:publication_check` step.** After recovery/invalidation
  checks and before slot classification, run `publication_exists` (`pubname = ANY(...)`) and
  **halt if the found set ≠ the requested set** — because START_REPLICATION would otherwise
  silently stream the subset. `publication_exists` interpolates a **validated `IN (...)` list**
  (the connect-chain simple-query protocol can't bind `$1`), each name allowlist-guarded.
  (`connection.ex` `:publication_check`; `query_builder.ex` `publication_exists/1`.)

## Options Considered

### Option A: Comma-string only (`publication: "p1,p2"`)
**Cons:** No validation boundary per name; pushes the parsing/validation burden onto the user
and makes the identifier-allowlist guarantee (Rule 2) harder to enforce. Rejected in favor of an
explicit list normalized + validated in Config.

### Option B: Bare `pubname = ANY($1)` on `pg_publication_tables` (no DISTINCT)
**Cons:** A table in two publications duplicates `array_agg` PK/column entries via the per-pubname
join fan-out — probe-disproven (`["id","id"]`). Would corrupt PK-based dedup. Rejected.

### Option C: Rely on `START_REPLICATION` to reject a missing publication
**Cons:** Probe-disproven — it silently streams the existing subset. A typo would silently
under-replicate with no error. Rejected in favor of an explicit connect-time existence check.

### Option D: Validated list + DISTINCT discovery + fail-closed existence check (adopted)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium (one new connect step, four discovery queries reworked) |
| Security | High — every interpolated name allowlist-validated (Rule 2) |
| Correctness | High — DISTINCT prevents discovery duplication; fail-closed prevents silent subset streaming |

## Trade-off Analysis

The adopted design pays for one new connect-chain step and a discovery-query rework in exchange
for (a) no silent under-replication on a bad publication name and (b) no PK/column duplication for
tables shared across publications. The rejected simpler options each fail silently — exactly the
class of failure this library's fail-closed posture exists to prevent.

## Consequences

- **Easier:** One slot can consume many publications; overlapping tables deliver once (pgoutput
  dedups streaming; the DISTINCT driving set dedups discovery); a typo'd publication fails fast at
  START with a `:publication_missing` signal instead of silently streaming a subset.
- **Harder:** The connect chain has one more fail-closed gate to reason about; discovery queries
  must keep the DISTINCT driving set (a future edit that reverts to a bare `ANY($1)` reintroduces
  the duplication bug).
- **Revisit if:** per-publication routing/filtering (delivering a change tagged by which
  publication carried it) is ever required — the current design intentionally deduplicates and
  does not preserve the pubname dimension past discovery.

## Verification (closeout 2026-07-14)

Proven at runtime against live PG16 by `test/integration/multipub_test.exs`: both publications'
changes deliver overlap-deduped in commit order; snapshot discovery unions across the list with
the shared table snapshotted exactly once (DISTINCT); a missing publication halts fail-closed at
START (`:publication_missing`).

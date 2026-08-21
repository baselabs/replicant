# Replicant — AI Agent & Contributor Guide

How to work effectively in this repo. The Critical Rules are binding.

## What this is

A framework-agnostic Elixir CDC consumer for Postgres logical replication
(`pgoutput`). It consumes the logical stream, assembles committed transactions,
and delivers them synchronously to a pluggable sink — advancing the replication
slot only AFTER the sink has durably persisted. **Tenant-blind, Ash-agnostic,
classification-blind.** Multitenancy and classification live in the sibling
`ash_replicant` sink adapter, never here.

**Capabilities (all shipped):** initial snapshot + resumable incremental backfill,
a lib-owned checkpoint store for non-transactional sinks (with bounded retry-then-halt),
batched checkpointing, sink-owned atomic batch delivery, `pgoutput` proto-v2
in-progress-transaction streaming, consumer-side disk spill for oversized
transactions, PG17+ forward-compat with failover slots, **multi-publication per
pipeline** (`publication: [p1, p2]` — fail-closed on a missing pub), and
**logical-decoding messages** (`messages: true` — `pg_logical_emit_message` payloads).
A logical-decoding message's `content` and `prefix` are **user bytes**: Critical Rule
1 binds — never log them, surface them in an error, or emit them in telemetry.

## Critical rules

**1. No row value in an error, log, or telemetry event.** Assume every value is
PII or a secret. The vendored decoder raises on malformed WAL and embeds raw
bytes; decode therefore runs behind `Replicant.Decoder.decode/1`, which catches
raises and scrubs raw bytes into a value-free `Replicant.Error`. Column names
stay **strings** — never `String.to_atom` (a wide or attacker-influenced schema
would exhaust the atom table). Telemetry metadata is allowlisted
(`Replicant.Telemetry`): LSNs, table names, counts, durations, error classes —
never values.

**2. Validate identifiers.** Slot and publication names reach SQL; they go
through `Replicant.Identifier.validate/1` (a strict Postgres-identifier
allowlist) before interpolation. A multi-publication list (`publication: [p1, p2]`)
validates EVERY name and fails closed if any requested pub is absent (a
`START_REPLICATION` that names a missing pub silently streams the subset). A
failure carries the invalid-SHAPE fact only, never the offending string. This
hardens walex's raw `'#{publication}'` interpolation.

**3. Exactly-once is at-least-once + a transaction-watermark-idempotent sink.**
The watermark is the **commit LSN at transaction granularity** (every row in a
pgoutput proto-v1 transaction shares one commit LSN). Skip any transaction whose
`commit_lsn <= checkpoint`; upsert rows by table PK. There is no naked
exactly-once without two-phase commit or an idempotent sink — do not claim one.
A **transactional** logical-decoding message rides `%Transaction.messages` and
inherits this effect-once dedup; a **non-transactional** message routes to
`handle_message/2` and is **at-least-once** (no dedup key, duplicates possible
on reconnect) — state each guarantee honestly.

An idle state mirror may acknowledge filtered WAL beyond its checkpoint; an
`append_log` sink must not. The append slot stays at the durable delivered
frontier so its reconnect origin can expose an out-of-band advance. A normal
published heartbeat releases WAL for a quiet append publication.

**4. Unchanged TOAST is a sentinel, not a value.** An UPDATE that does not touch
a TOASTed column sends a sentinel. `%Change{}` surfaces it as a first-class
`unchanged: [col]` list; the sentinel never appears in `record`. Sinks must
leave those columns untouched on upsert. A *change* of replica identity or a
dropped column classifies as `:destructive` and halts fail-closed.

**5. Stay tenant-blind.** No multitenancy, scope, or classification logic enters
this lib. That boundary is the whole reason `replicant` and `ash_replicant` are
separate.

## Development workflow

    mix deps.get
    mix format
    mix credo --strict
    mix compile --warnings-as-errors
    mix test
    mix dialyzer
    # or all quality gates at once:
    mix quality

All gates must pass before a commit/PR. Update `CHANGELOG.md` under
`[Unreleased]`.

**Pre-commit hook (one-time install per clone):** `.githooks/pre-commit` runs
`mix format --check-formatted` + `mix credo --strict` and **blocks the commit on
red** — a backstop for the two silent gates whose drift won't otherwise surface
until CI (`.github/workflows/ci.yml`). It installs into the default hook dir
alongside graphify's `post-commit`; it does **not** set `core.hooksPath`, so
graphify is unaffected.

    cp .githooks/pre-commit .git/hooks/pre-commit && chmod +x .git/hooks/pre-commit

Bypass with `git commit --no-verify` (CI still enforces both on push).

## Testing

- **Unit + real-byte conformance tests** (`test/**/*_test.exs`): no live server,
  no `postgrex` dependency. The decoder conformance suite decodes REAL captured
  `pgoutput` bytes (walex's MIT-licensed capture, inlined and credited) for every
  message type — including the unchanged-TOAST sentinel and all replica-identity
  modes. It never self-signs fixtures. An independent docker-PG16 capture
  (`test/integration/pg16_conformance_test.exs`) corroborates it against a live
  server.
- **Supported PostgreSQL versions: 15, 16, 17, 18.** Behavior is version-gated by
  `server_version_num`: the slot-invalidation query selects only the columns that exist on
  the connected major (PG15 → `wal_status`; PG16 → `+ conflicting`; PG17/18 → `+
  invalidation_reason, synced`), and failover slots are created on PG17/18 but structurally
  rejected on PG15/16 (`{:config, :failover_unsupported}` halt — PG15/16 reject the FAILOVER
  slot option). The CI matrix runs the full suite on all four majors.
- **Integration + crash-injection tests** (`test/integration/**`): gate on
  `REPLICANT_TEST_URL` pointing at a live PostgreSQL with `wal_level=logical`; skip when
  unset. **Operator-approved Docker port mappings (never `localhost:5432`): PG15 → 5615,
  PG16 → 5599, PG17 → 5617, PG18 → 5618.** Spin any major with
  `docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p <PORT>:5432 postgres:<MAJOR> -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10`
  then `export REPLICANT_TEST_URL="postgres://postgres@localhost:<PORT>/postgres"`. Run the
  whole matrix locally by spinning all four and running `mix test` against each URL in turn.
- **Version-behavior tests** (`test/integration/version_behavior_test.exs`, tagged
  `:integration`): run against whatever major `REPLICANT_TEST_URL` points at and branch on
  the live version — proving failover is created on PG17+ and rejected on PG<17, and that the
  version-gated invalidation query runs (on PG15 selecting `conflicting` would error). Each
  run emits an `R05-SUBSTRATE-RECEIPT pg=<major>` line; CI greps for the row's expected major
  (via `EXPECTED_PG_MAJOR`) to prove the matrix row actually ran integration non-vacuously.
- **PG17+ failover tests** (`test/integration/pg17_failover_test.exs`, tagged `:pg17`): run
  against a PG17 or PG18 server; auto-excluded (skipped, never vacuously passed) when the
  server is < 17.
- **TDD:** write the test first.

## Docs & lifecycle-artifact policy

- **Tracked repository-only:** `AGENTS.md` (the contributor/agent contract; excluded from Hex).
- **Tracked and Hex-published:** `README.md`, `CHANGELOG.md`,
  `CONTRIBUTING.md`, `usage-rules.md`, `LICENSE`, `NOTICE`, `docs/INVARIANTS.md`,
  `docs/ROADMAP.md`, and accepted decisions under `docs/adr/`.
- **Never tracked:** all brainstorm specs, plans, exec notes, reviews, and
  handoffs under `docs/superpowers/`, which is gitignored. Do not move those
  lifecycle artifacts into tracked documentation paths.
- AI-tool state dirs (`.claude/`, `.serena/`, etc.) are gitignored.

## graphify (code knowledge graph)

`graphify-out/graph.json` maps this repo (tree-sitter AST; rebuilt by the git post-commit hook; gitignored).

- For orientation ("where is X handled", "what connects A to B", "explain module M"), prefer `graphify query "<question>"` / `graphify explain "<Module>"` / `graphify path "<A>" "<B>"` over grep/Read fan-outs — one call returns a scoped subgraph with file:line hits.
- Graph output is NAVIGATION, never evidence. Edges reflect the last build, not the working tree, and cross-module call edges can be incomplete (Elixir: file-local only — alias-mediated calls are NOT resolved). Consumer sweeps and every load-bearing claim (review finding, plan anchor) still verify against live code: grep + file:line read.
- After large uncommitted changes, `graphify update .` refreshes the graph (AST-only, no API cost, no key).

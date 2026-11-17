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
**logical-decoding messages** (`messages: true` — `pg_logical_emit_message` payloads),
and the **ADR-0009 plugin decoders** (`decoder: :pglogical | :wal2json`, pre-15 majors)
with fail-closed divergence handling: a configured keyless wal2json table halts at start
(`allow_keyless_tables: true` opts into insert-only semantics), and a dropped column
halts `:destructive` on every decoder (wal2json: at the change for inserts/fixed-width
columns, else within `schema_check_interval`, default 30s).
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
    # or three of those gates at once — the alias is exactly
    # `format --check-formatted` + `credo --strict` + `dialyzer`;
    # `mix compile --warnings-as-errors` and `mix test` are NOT in it:
    mix quality

Update `CHANGELOG.md` under `[Unreleased]`.

**Checks run in CI, through the landing.** The repository has no pre-commit hook. Every check
(format, credo, compile warnings, the dependency audit, dialyzer, `mix test` on all six
PostgreSQL majors, docs and package, the reference example) is a job in
`.github/workflows/qualification.yml`. Maintainers land a commit through a `kci/<commit>`
branch: `.github/workflows/ci.yml` runs those jobs once on it, and its final `qualified` job
(which fails unless every job succeeded) is the verdict that moves `main`. A push to `main`
runs nothing. Pull requests run the same jobs from `.github/workflows/pull-request.yml` under
other check names. Contributors may run any of the commands above on their own machines first.

Qualification jobs do not cache dependencies, builds, or Dialyzer PLTs. Each candidate has
a new cache scope, and a cache written on `main` would be restorable by candidates.

## Workflow

Make the change, commit with named paths, and land it through `kci/<commit>`; `main` moves only
on that commit's `qualified` check. If it goes red, fix forward in the next commit.

- A test that fails on every run is a defect: fix it, or quarantine it with a reason, in the
  change that finds it.
- Update the CHANGELOG when behavior, a dependency or a contract changes; other docs only when
  a contract changes.
- For the release operator: a release is finished only when its tag, its Hex package and docs,
  and its GitHub release all exist. `scripts/release/publish_candidate.sh` does only Hex and
  docs, so finish with `gh release create vX.Y.Z --verify-tag --target <tag's full commit SHA>`.
  While the GitHub release is missing, the package check fails on every CI run, pull requests
  included.

## CI/CD and releases

infra-1, the DevOps session, owns CI/CD, the build box, releases and their keys for every
BaseLabs product, and its requirements bind this repository. They are written once in the infra
repository at `baselabs/docs/ci.md`, section "What infra requires of every product". Read them
from infra's default branch (`git -C ~/Developer/infra fetch -q origin`, then
`git -C ~/Developer/infra show origin/main:baselabs/docs/ci.md`), not from a local checkout,
and reread them whenever you touch a workflow, a Dockerfile, a release script or a secret.

- An agent's work ends at `main`, and it lands through `kimosabe-land` and nothing else. It never
  releases, deploys, publishes, tags a version, dispatches a workflow, pushes an image by hand,
  or handles a release, publish or CI key. When something should go out, it sends infra-1 one
  message: the repository, the full commit, what goes out, and anything infra-1 needs.
- An agent writes its own workflow changes. Before landing a change to `.github/`, a Dockerfile,
  a release script or anything that touches a secret, it sends infra-1 the branch and head and
  waits for infra-1's review. Any CI/CD, runner, box or key problem goes to infra-1, not around
  it.
- On the build box, gate work on `kimosabe-remote where` and your own routed run, never on
  `doctor`. Never list process arguments: no `ps` with `args` or `-f`, no `/proc/*/cmdline`;
  use `ps -eo pid,etimes,pcpu,comm`. Nothing that compiles or tests runs on the owner's Mac.
- A secret never appears on a command line, in a log or in a message. If you see one exposed,
  tell infra-1 where it sits without copying the value.
- Messages to infra-1 carry one result each and are complete enough to act on without a reply.
  No acknowledgements and no status notes.
- Every brief given to a child agent carries these same rules.

## Testing

- **Unit + real-byte conformance tests** (`test/**/*_test.exs`): no live server, no `postgrex`.
  The pgoutput suite decodes REAL captured bytes (walex's MIT capture, credited) for every
  message type, including the unchanged-TOAST sentinel and all replica-identity modes; it never
  self-signs fixtures. `test/integration/pg16_conformance_test.exs` corroborates it live. The
  plugin decoders' suite (`test/replicant/decoder/plugin_conformance_test.exs`) decodes real
  9.6/12 captures (`test/fixtures/wal2json_*`, `pglogical_*.bin`) with byte-flip tamper tests.
  The decoder-parity marquee (`test/integration/decoder_parity_test.exs`) proves byte-identical
  delivery across pgoutput and both plugins and is FAIL-LOUD: a pre-15 plugin row must wire
  `REPLICANT_PG96_URL` to a real 9.6 carrying both plugins. The snapshot leg
  (`test/integration/plugin_snapshot_pg96_test.exs`) proves exactly-once arrival across a 9.6
  two-part exported-snapshot handoff wherever a pre-10 server is wired.
- **Supported PostgreSQL versions: 9.6, 12, 15, 16, 17, 18.** The 9.6, 12 and 15-plugin rows
  serve the plugin decoders (ADR-0009); their images come from `test/support/pg_old.dockerfile`
  (digest-pinned bases, pglogical 2.4.8, wal2json pinned per major). PG15+ plugin lanes start
  with `output_plugin_libraries=wal2json,pglogical_output,pgoutput`; the `:pg_old_decoders`
  exclusion follows a plugin probe, not the version. Rows carry `EXPECTED_PG_MAJOR`. Behavior
  gates on `server_version_num`: the slot-invalidation query selects only columns the major has
  (PG15 `wal_status`; PG16 `+ conflicting`; PG17/18 `+ invalidation_reason, synced`); failover
  slots exist on PG17/18 and halt `{:config, :failover_unsupported}` on PG15/16. `:pg14`
  marquees (streaming, spill, messages) skip pre-14; `:pg10` publication modules skip on 9.6.
  CI runs the full suite on all six majors.
- **Integration + crash-injection tests** (`test/integration/**`): gate on
  `REPLICANT_TEST_URL` pointing at a live PostgreSQL with `wal_level=logical`; skip when
  unset. Contributors without a server can spin a major (never on `localhost:5432`) with
  `docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p <PORT>:5432 postgres:<MAJOR> -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10`
  and `export REPLICANT_TEST_URL="postgres://postgres@localhost:<PORT>/postgres"`. A shared
  server needs a `LOGIN REPLICATION CREATEDB` role, its own test database, and
  `GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal()`; tests set
  `logical_decoding_work_mem` on `CURRENT_USER` only.
- **Version-behavior tests** (`version_behavior_test.exs`) branch on the live major and emit an
  `R05-SUBSTRATE-RECEIPT pg=<major>` line CI greps against `EXPECTED_PG_MAJOR`. **PG17+
  failover tests** (`pg17_failover_test.exs`, `:pg17`) are auto-excluded below 17.
- **Reference example** (`examples/replication_pipeline`, CI job `reference-example`): a
  docker-compose stack (source PG18 → replicant release container → dest PG18), the public sink
  API's canary, proving delivery, TOAST-sentinel survival and restart-resume with zero
  duplicate effects. The repo-root `.dockerignore` is a WHITELIST: anything not explicitly
  allowed (`.env`, crash dumps, lifecycle artifacts) never rides the build context; extend it
  only with a specific `!path`.

## Docs & lifecycle-artifact policy

- **Tracked repository-only:** `AGENTS.md` (the contributor/agent contract; excluded from Hex).
- **Tracked and Hex-published:** `README.md`, `CHANGELOG.md`,
  `CONTRIBUTING.md`, `usage-rules.md`, `LICENSE`, `NOTICE`, `docs/INVARIANTS.md`,
  `docs/ROADMAP.md`, accepted decisions under `docs/adr/`, and the examples'
  READMEs (`examples/README.md`, `examples/replication_pipeline/README.md`).
  The example CODE under `examples/` is tracked repository-only.
- **Never tracked:** all brainstorm specs, plans, exec notes, reviews, and
  handoffs under `docs/superpowers/`, which is gitignored. Do not move those
  lifecycle artifacts into tracked documentation paths.
- **Instruction history:** `docs/reference/agent-history.md` (repository-only; not binding).
- AI-tool state dirs (`.claude/`, `.serena/`, etc.) are gitignored.

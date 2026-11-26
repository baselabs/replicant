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
and the **ADR-0009 plugin decoders** (`decoder: :pglogical | :wal2json`)
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

## Build and test

All checks run only in GitHub CI on GitHub-hosted runners. This public repository never
builds on the build box. Nothing that compiles or tests runs on the owner's Mac.
Covering tests for changed files and their direct dependents run in
`.github/workflows/qualification.yml`, through the same `kci/` route as all other checks.
Land changes with the landing tool, which submits a `kci/<commit>` candidate and moves `main`
only after its `qualified` check succeeds. Do not push directly to `main`.
It settles after promotion itself: it deletes local and origin branches that contain the landed
commits, retires clean delegate worktrees and sweeps `kci/` refs. Exit 0 means landed and settled;
exit 4 means landed with leftovers named, so fix the cause and run its sweep.

The commands below describe the CI gates. See [Testing](#testing) for the test coverage and
[Workflow](#workflow) for contribution and release responsibilities.

    mix deps.get --check-locked
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
(format, credo, compile warnings, the dependency audit, dialyzer, `mix test` on supported and legacy
PostgreSQL majors, docs and package, the reference example) is a job in
`.github/workflows/qualification.yml`. Maintainers land a commit through a `kci/<commit>`
branch: `.github/workflows/ci.yml` runs those jobs once on it, and its final `qualified` job
(which fails unless every job succeeded) is the verdict that moves `main`. A push to `main`
runs nothing. No workflow runs repository code from a pull request. A fork's change is qualified
when a maintainer submits it through the `kci/` landing.

Every workflow declares `defaults: run: shell: bash`, so each step runs with `pipefail`; every
script a job calls is bash with `set -euo pipefail`. A command whose output a check reads is
assigned to a variable first, never hidden by `|| true` or read through `<(...)` or `"$(...)"`
inside a test, so its failure fails the step.

Qualification jobs do not cache dependencies, builds, or Dialyzer PLTs. Each candidate has
a new cache scope, and a cache written on `main` would be restorable by candidates.

Timeouts follow the infrastructure CI requirements. Comments name each job's slowest
measured green run and duration; publishing uses the unmeasured limit until its own first run.
All runtime installers take versions only from the shared `scripts/toolchain.sh` reader's
outputs, select them strictly, and use its `--installed` check to refuse any Elixir, full OTP
patch, or Elixir OTP build-suffix mismatch. The example Dockerfile documents why its immutable
container toolchain is pinned separately.

## Workflow

Make the change, commit with named paths, and use the route in [Build and test](#build-and-test).
`main` moves only on that commit's `qualified` check.
If CI goes red, fix forward in the next commit.

- A test that fails on every run is a defect: fix it, or quarantine it with a reason, in the
  change that finds it.
- Update the CHANGELOG when behavior, a dependency or a contract changes; other docs only when
  a contract changes.
- Qualification uses the pinned shared Hex action to verify the archives declared in
  `.github/hex-packages.json`. The `hex-hexpm-replicant` artifact retains the package, docs and
  manifest for seven days. The release commit must have a stamped changelog.
- Give infra the repository, full qualified `release_commit`, version, release contents and
  successful qualification run. Infra obtains the owner's approval and dispatches
  `.github/workflows/publish.yml` on the default branch with that commit. Both the original and
  triggering actors must be `corepay` or `palermo-git`.
- Only `upload` enters `publish`. The pinned action verifies qualification and the archive,
  requires an existing Hex package, checks for a private twin, and exchanges and revokes a
  package-scoped token if needed. First publication belongs to the owner.
- `read-back` verifies the signed Hex registry and the qualified archive bytes. The served docs
  must identify `release_commit`. It retains the package, docs and checksums as `release-assets`.
  Infra compares the package with Hex, tags that commit as `baselabs_replicant-v<version>`, and
  creates the GitHub release from those assets and the packaged changelog section. After infra
  verifies the release, record the package digest in `scripts/release/published_packages.sha256`
  through normal landing.

## CI/CD and releases

The infrastructure maintainers own CI/CD, the build box, releases and their keys for every
BaseLabs product, and their CI requirements bind this repository. Maintainers read the current
requirements from their default branch, not from a local checkout, and reread them whenever
they touch a workflow, a Dockerfile, a release script or a secret.

- An agent's work ends at `main`, and it lands through the landing tool and nothing else. It never
  releases, deploys, publishes, tags a version, dispatches a workflow, pushes an image by hand,
  or handles a release, publish or CI key. When something should go out, it sends the infrastructure
  maintainers one message with the repository, full `release_commit`, version, what goes out,
  and the green `qualified` run.
- An agent writes its own workflow changes. Before landing a change to `.github/`, a Dockerfile,
  a release script or anything that touches a secret, it sends the infrastructure maintainers
  the branch and head for their advice; the repository's owner decides and lands the change.
  Any CI/CD, runner, box or key problem goes to them, not around it.
- This public repository never builds on the build box. Never list process arguments:
  no `ps` with `args` or `-f`, no `/proc/*/cmdline`; use `ps -eo pid,etimes,pcpu,comm`.
- A secret never appears on a command line, in a log or in a message. If you see one exposed,
  tell the infrastructure maintainers where it sits without copying the value.
- Messages to the infrastructure maintainers carry one result each and are complete enough to act on without a reply.
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
- **PostgreSQL substrate:** one qualification lane per PGDG-supported major
  (15 through 19, [ADR-0010](docs/adr/0010-postgresql-server-support.md)), plus 9.6
  and 12 compatibility lanes in 1.x. `.github/postgres.ref` pins the platform engine
  for its lane and the example. `.github/postgres-lanes.json` pins upstream images
  for the other majors and the engine lane's same-major pglogical companion.
  `scripts/ci/postgres-lanes.sh` validates the matrix. The shared PostgreSQL action
  checks the public engine digest anonymously and owns password-authenticated
  startup and unconditional teardown. Plugin builds use exact server headers and
  full source commits. wal2json stays on 9.6, 12 and 15. Every lane checks the
  live major and plugin inventory. Publication-capable pglogical runs require a
  successful parity receipt. The `:pg17`, `:pg14` and `:pg10` tags gate features
  on their minimum major. The slot-invalidation query uses `wal_status` from 13,
  `conflicting` from 16, and `invalidation_reason` and `synced` from 17.
- **Integration + crash-injection tests** (`test/integration/**`): gate on
  `REPLICANT_TEST_URL` pointing at a live PostgreSQL with `wal_level=logical`; skip when
  unset. Qualification provisions isolated containers on loopback with a new database,
  `wal_level=logical`, and throwaway password fixtures supplied through environment variables.
  Compatibility entrypoints require stable readiness across their init restart; the engine
  requires TCP readiness after initialization. Its disposable scratch is uid/gid 26 at
  `/var/lib/pg` plus `/tmp`; in-container psql names `-h /tmp`. Checks run only through the
  CI route above. A shared server needs a password-bearing `LOGIN REPLICATION CREATEDB`
  role, its own test database, and
  `GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal()`; tests set
  `logical_decoding_work_mem` on `CURRENT_USER` only.
- **Version-behavior tests** (`version_behavior_test.exs`) branch on the live major and emit an
  `R05-SUBSTRATE-RECEIPT pg=<major>` line CI greps against `EXPECTED_PG_MAJOR`. **PG17+
  failover tests** (`pg17_failover_test.exs`, `:pg17`) are auto-excluded below 17.
- **Reference example** (`examples/replication_pipeline`, CI job `reference-example`): a
  docker-compose stack (platform engine → replicant release container → platform engine), the public sink
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

## graphify (code knowledge graph)

`graphify-out/graph.json` maps this repo (tree-sitter AST; rebuilt by the git post-commit hook;
gitignored). Prefer `graphify query`, `graphify explain` and `graphify path` for orientation.
Graph output is navigation, never evidence: Elixir edges are file-local only, so verify
load-bearing claims against live code. `graphify update .` refreshes it after large uncommitted
changes.

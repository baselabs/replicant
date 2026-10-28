# Agent instruction history

Text moved byte-for-byte out of `AGENTS.md` on October 28, 2026, when the owner ruled that
process is sized to the risk of each change. Historical record only; not binding.

## From AGENTS.md: Development workflow (superseded gate line)

All gates must pass before a commit/PR. Update `CHANGELOG.md` under
`[Unreleased]`.

## From AGENTS.md: Testing (full wording before condensing, including the superseded TDD line)

## Testing

- **Unit + real-byte conformance tests** (`test/**/*_test.exs`): no live server,
  no `postgrex` dependency. The pgoutput conformance suite decodes REAL captured
  `pgoutput` bytes (walex's MIT-licensed capture, inlined and credited) for every
  message type — including the unchanged-TOAST sentinel and all replica-identity
  modes. It never self-signs fixtures. An independent docker-PG16 capture
  (`test/integration/pg16_conformance_test.exs`) corroborates it against a live
  server. The PLUGIN decoders' conformance suite
  (`test/replicant/decoder/plugin_conformance_test.exs`) decodes REAL bytes captured
  from the live 9.6/12 substrate servers (committed under `test/fixtures/`:
  `wal2json_*.{jsons}`, `pglogical_*.bin`) with the same byte-flip tamper discipline.
  The decoder-parity marquee (`test/integration/decoder_parity_test.exs`) proves one
  fixture delivers byte-identically across pgoutput and both plugins — FAIL-LOUD, never
  narrowed: the primary must carry BOTH plugins, a pre-15 plugin row must wire
  `REPLICANT_PG96_URL` to a real 9.6 carrying both (CI's 12 row starts that 9.6 beside
  the 12 primary; the marquee flunks if the wiring is missing), and the pgoutput
  reference runs on the same primary (core variant on 10-14; the truncate+message
  variant on 15+, which needs PG14+ messages and a plugin-bearing server no CI lane
  wires today). A connected snapshot leg
  (`test/integration/plugin_snapshot_pg96_test.exs`) runs wherever a real pre-10 server
  is wired (`REPLICANT_PG96_URL` when set, else a pre-10 `REPLICANT_TEST_URL` primary —
  CI's 9.6 and 12 rows; skipped as unrunnable elsewhere): a `decoder: :wal2json` +
  `snapshot: true` pipeline back-fills under 9.6's two-part exported-snapshot name
  (`%08X-%d`; 10+ export three parts) while a writer commits across the handoff, and
  every table row must arrive exactly once across snapshot and stream.
- **Supported PostgreSQL versions: 9.6, 12, 15, 16, 17, 18.** The 9.6, 12 and
  15-plugin rows exist for the plugin decoders (ADR-0009): their images are built by
  `test/support/pg_old.dockerfile` (digest-pinned bases + pglogical 2.4.8 + wal2json
  pinned per major — the `wal2json_2_6` tag on pre-15 bases, master `75a4b494` on 15+
  where 2.6 does not compile — plus a `wal2json2_4` build on pre-15 only, the
  option-halt test's lever). A PG15+ server gates every output plugin: plugin lanes
  there start with
  `output_plugin_libraries=wal2json,pglogical_output,pgoutput`. The trio's
  `:pg_old_decoders` exclusion is decided by a plugin probe on the primary, not the
  version: a stock 15-18 excludes it, a plugin-bearing primary of any major runs it.
  The rows carry `EXPECTED_PG_MAJOR`. Behavior is version-gated by
  `server_version_num`: the slot-invalidation query selects only the columns that exist on
  the connected major (PG15 → `wal_status`; PG16 → `+ conflicting`; PG17/18 → `+
  invalidation_reason, synced`), and failover slots are created on PG17/18 but structurally
  rejected on PG15/16 (`{:config, :failover_unsupported}` halt — PG15/16 reject the FAILOVER
  slot option). The streaming/spill/messages marquees (`:pg14` tag) are excluded on a pre-14
  primary — proto-v2 streaming and the pgoutput `messages` option are PG14+, and
  `logical_decoding_work_mem` is PG13+ — and the publication-dependent core integration
  modules (`:pg10` tag) are excluded on the 9.6 row (`CREATE PUBLICATION` is PG10+; that
  row's integration coverage is the plugin-decoder legs). The CI matrix runs the full suite
  on all six majors.
- **Integration + crash-injection tests** (`test/integration/**`): gate on
  `REPLICANT_TEST_URL` pointing at a live PostgreSQL with `wal_level=logical`; skip when
  unset. **Operator-approved Docker port mappings (never `localhost:5432`): PG9.6 → 5609,
PG12 → 5612 (build their images from `test/support/pg_old.dockerfile`),
PG15 → 5615,
  PG16 → 5599, PG17 → 5617, PG18 → 5618.** Spin any major with
  `docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p <PORT>:5432 postgres:<MAJOR> -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10`
  then `export REPLICANT_TEST_URL="postgres://postgres@localhost:<PORT>/postgres"`. Run the
  whole matrix locally by spinning all six and running `mix test` against each URL in turn.
  A shared server works too: a non-superuser role with `LOGIN REPLICATION CREATEDB`, its own
  test database, and `GRANT EXECUTE ON FUNCTION pg_catalog.pg_switch_wal()` in that database
  (the idle-ack tests force a WAL switch). Tests set `logical_decoding_work_mem` on
  `CURRENT_USER`, so they never alter another role.
- **Version-behavior tests** (`test/integration/version_behavior_test.exs`, tagged
  `:integration`): run against whatever major `REPLICANT_TEST_URL` points at and branch on
  the live version — proving failover is created on PG17+ and rejected on PG<17, and that the
  version-gated invalidation query runs (on PG15 selecting `conflicting` would error). Each
  run emits an `R05-SUBSTRATE-RECEIPT pg=<major>` line; CI greps for the row's expected major
  (via `EXPECTED_PG_MAJOR`) to prove the matrix row actually ran integration non-vacuously.
- **PG17+ failover tests** (`test/integration/pg17_failover_test.exs`, tagged `:pg17`): run
  against a PG17 or PG18 server; auto-excluded (skipped, never vacuously passed) when the
  server is < 17.
- **Reference example** (`examples/replication_pipeline`, CI job `reference-example`): a
  docker-compose stack (source PG18 → replicant OTP release container → dest PG18) that is
  the public sink API's canary — the job runs the example's own static gates (format,
  compile-warnings, credo --strict, dialyzer) then builds the stack and proves delivery,
  TOAST-sentinel survival, and restart-resume with zero duplicate effects. The repo-root
  `.dockerignore` is a WHITELIST (containment, not size): the build context is the repo
  root, so anything not explicitly allowed — `.env`, crash dumps, lifecycle artifacts —
  must never ride it; extend it only by adding a specific `!path`.
- **TDD:** write the test first.

# ADR-0009: Decoders for the pglogical and wal2json output plugins (PostgreSQL 9.6 to 14)

**Status:** Accepted 2026-09-29 (ships in 1.4.0)
**Date:** 2026-09-29
**Deciders:** replicant maintainer (roadmap row A7)

## Context

Replicant reads exactly one logical-decoding output plugin. The slot grammar emits
`CREATE_REPLICATION_SLOT ... LOGICAL pgoutput` (`lib/replicant/query_builder.ex`, the
`@pgoutput` constant), `Replicant.Decoder.decode/2` parses pgoutput frames only, and the
connect chain discovers tables through `pg_publication` and `pg_publication_tables`.
`pgoutput` and publications exist from PostgreSQL 10; the library is tested on 15, 16, 17
and 18 (`README.md`, "PostgreSQL version support").

A large installed base runs older majors. PostgreSQL 9.6 has `wal_level = logical` but no
`pgoutput` and no publications; 10 through 14 have both but are outside the tested range,
and many of those servers, including managed services, already carry the `pglogical`
extension (whose output plugin is `pglogical_output`) or `wal2json`, installed for an
earlier consumer. Today such a server cannot be read by Replicant at all: a 9.6 server
fails at slot creation, and a 10 to 14 server is unproven. The pieces that make Replicant
worth using (sink-owned effect-once by `commit_lsn`, the value-free boundary, fail-closed
halts, snapshot and checkpoint modes) do not depend on the plugin's wire format; only the
first parsing step does.

## Decision

Introduce a decoder behavior behind the existing pgoutput decoder and add two
implementations, selected by configuration. The sink contract, checkpoint semantics and
halt semantics do not change.

1. **Behavior.** A `Replicant.Decoder.Plugin` behavior is introduced and pgoutput becomes
   its first implementation. The behavior covers the four places the plugin's format leaks into the pipeline:
   the plugin name and options for `CREATE_REPLICATION_SLOT` and `START_REPLICATION`;
   `decode/2` from one XLogData payload (plus the frame's `wal_start` and `wal_end` LSNs,
   passed in `opts`) to the existing `Replicant.Decoder.Messages` structs; the table-set
   discovery queries the connect chain runs; and a capability set the config validator
   checks. `Replicant.Decoder.decode/2` keeps its signature and delegates to the configured
   plugin, so the value-free boundary (ADR-0003) stays in one place: every raise, throw or
   exit inside any plugin decoder still scrubs to `%Replicant.Error{}`.
2. **Configuration.** A new top-level option `decoder:` with `:pgoutput` (the default,
   byte-identical behavior), `:pglogical` or `:wal2json`. The table set is named per
   decoder: `publication:` for pgoutput (unchanged); `replication_sets:` for pglogical (the
   names pglogical uses in place of publications); `tables:` (a list of `{schema, table}`)
   for wal2json, passed as the plugin's `add-tables` option. Naming the wrong table-set key
   for the chosen decoder is refused at start with `:config_invalid`, and every name still
   passes `Replicant.Identifier.validate/1` before it reaches SQL or a plugin option
   (Critical Rule 2).
3. **pglogical.** One implementation for the `pglogical_output` binary protocol that
   pglogical 2.x speaks: the startup message that reports the negotiated protocol
   version and parameters, the relation-metadata message, begin, commit, origin, insert,
   update, delete, and the tuple format with its null, unchanged-TOAST, text and binary
   column markers. The decoder requests text-format values only (no
   `binary.want_binary_basetypes`), so the existing casting layer (ADR-0008) receives the
   same server text output pgoutput delivers. The decoder refuses at connect if the
   plugin's startup message reports a protocol version outside the range the decoder was
   built for (`:decoder_protocol_unsupported`).
4. **wal2json.** One implementation for the JSON format, wal2json's
   `format-version '2'` (one JSON document per action: `B`, `C`, `I`, `U`, `D`, `T`, `M`),
   with `include-lsn`, `include-xids`, `include-timestamp`, `include-types`,
   `include-typmod` and `include-pk` on, and `add-tables` carrying the configured tables.
   Format version 1 (one document per transaction) is not decoded: it has no per-change
   LSN and forces the whole transaction into one payload, which defeats the in-flight
   window. A wal2json build that rejects `format-version '2'` fails the slot's first
   `START_REPLICATION` and the pipeline halts `:decoder_option_unsupported` rather than
   falling back to version 1.
5. **Synthesized relations.** wal2json sends no relation message, so the decoder supplies
   a `%Relation{}` itself. The decoder builds one from the first change it sees for a table (column names
   and types come with every change) and re-checks every following change against it; a
   change whose column list differs is a schema change the plugin did not announce, so the
   decoder emits the new `%Relation{}` before the change, and the existing
   `Replicant.SchemaChange` classification runs unchanged. Because the stream cannot
   express a replica-identity change, the connect chain reads `pg_class.relreplident` for
   every configured table at every connect and reconnect and stores it on the synthesized
   relation; a change to it is detected at the next reconnect, not at the moment of the
   `ALTER TABLE`. That window is documented, not hidden. The same catalog read serves the
   pglogical decoder (verification section): pglogical's relation metadata carries no column
   types and no replica-identity field, so its TYPES and `relreplident` come from this read.
6. **Explicit markers.** Fields a plugin cannot express are set to an explicit marker, never
   a fabricated value. `type_modifier` is `-1` (PostgreSQL's own "no modifier" value) when the plugin
   does not carry it; `replica_identity` on a synthesized relation is the catalog value read
   at connect; a column's `flags` (key membership) come from wal2json's `identity` list or
   `include-pk` output and from pglogical's attribute flags. A change whose old-key data is
   required and absent halts `:decode_failure` (a defensive branch). The live substrate
   CORRECTED the premise: a keyless write never reaches ANY stream — PostgreSQL refuses
   the write outright on an update-publishing table (object_not_in_prerequisite_state,
   55000-class), wal2json drops it plugin-side, and pglogical refuses such tables in
   update-carrying replication sets — so the halt is unreachable by construction and
   the per-decoder divergences are documented instead (verification section).
7. **Commit LSN.** It is never fabricated: `commit_lsn` comes from the plugin's commit
   message (pglogical carries it; wal2json version 2's `C` action carries `lsn` with
   `include-lsn` on). A commit payload without it halts `:decoder_lsn_missing`. The frame's
   `wal_end` is used only for the keepalive and idle-advance paths, exactly as today.
8. **Capabilities.** They are refused at start, never degraded at run time. `streaming:` (pgoutput
   protocol 2 in-progress streaming), `failover: true`, and `messages: true` on pglogical
   are refused `:decoder_capability_unsupported` when the chosen decoder cannot express
   them. `messages: true` on wal2json maps the `M` action to `%Message{}` with the same
   transactional split as ADR-0001. `snapshot: true` and `snapshot: [mode: :incremental]`
   work with every decoder: `EXPORT_SNAPSHOT` is a slot option, not a plugin one, and the
   table set for the reader comes from the decoder's discovery query.
9. **Server version gates.** On `server_version_num < 100000` the pgoutput decoder is
   refused `{:config, :decoder_unsupported_on_server}` before slot creation. The slot
   status query gains a tier below 130000 that selects no invalidation column (`wal_status`
   and `max_slot_wal_keep_size` arrive in 13; on 9.6 to 12 a slot cannot be invalidated by
   size, and the only loss signal is a removed WAL segment, which surfaces as a
   `START_REPLICATION` failure and halts through the command-error watchdog). The 9.6
   function names (`pg_current_xlog_location` for `pg_current_wal_lsn`) are selected by the
   same version gate wherever the library calls them.

## Version matrix and where each cell is tested

| PostgreSQL | pgoutput | pglogical_output | wal2json | Tested by |
|---|---|---|---|---|
| 9.6 | absent | pglogical 2.x | yes | a real 9.6 with both plugins in the matrix (new row) |
| 10, 11 | present | pglogical 2.x | yes | not in the matrix; documented as untested |
| 12 | present | pglogical 2.x | yes | a real 12 in the matrix (new row): pgoutput and both plugins on one server |
| 13, 14 | present | pglogical 2.x | yes | not in the matrix; documented as untested |
| 15 to 18 | present, tested today | not tested here | not tested here | existing rows; pgoutput only |

Which pglogical release supports which major, and the exact wal2json release that
introduced each option named above, are read from the two projects' own sources before the
build and recorded in the intent; they are not asserted here. Two rows is the whole added
matrix: 9.6 is the only major where the plugins are the sole route, and 12 is the newest
major on which pgoutput and both plugins can be compared on one server against the same
fixture.

## Plugin-fact verification (2026-09-29, sources read before the build)

Each fact this ADR rests on, marked **OBSERVED** (verified in the project's own source) or
**CORRECTED** (the ADR's claim was adjusted; anchor record in
`.kimosabe/intents/pglogical-wal2json-decoders.md`):

- **pglogical startup message** (protocol version + parameter pairs), **begin/commit/origin**,
  and the **tuple markers `n`/`u`/`t`** — OBSERVED. The row messages carry `K`/`N` tuple
  markers exactly like pgoutput; `protocol.txt`'s "tupleformat `T`" row-header text is stale
  (BDR-era) — CORRECTED. Text values' `t`-kind length is NUL-inclusive (unlike pgoutput).
- **pglogical relation metadata carries NO column types and NO replica-identity field** —
  OBSERVED (`coltypes` is hardcoded false). CORRECTED consequence: the connect-time catalog
  read (§5) serves pglogical too — column TYPES for the ADR-0008 casting layer and
  `relreplident` to disambiguate a `REPLICA IDENTITY FULL` old tuple from a key-only one.
- **pglogical emits no TRUNCATE and no logical-decoding message** (no `truncate_cb`, no
  `message_cb`) — OBSERVED. The fixture's truncate and message legs therefore do not ride
  the pglogical comparison.
- **wal2json format-version 2, `C`-action commit LSN, per-action documents, `add-tables`** —
  OBSERVED. In format 2 the typmod is always embedded in the `type` string (`include-typmod`
  gates format 1 only) — CORRECTED; the decoder takes type names from `include-type-oids`
  (present since 1.0) through the same `OidDatabase` call pgoutput uses.
- **wal2json emits numerics as raw JSON numbers** (NaN/Infinity become `null`) — OBSERVED.
  CORRECTED: `numeric-data-types-as-string 'true'` (wal2json ≥ 2.6) is REQUIRED for
  byte-identical numerics and rides every `START_REPLICATION`; the substrate builds wal2json
  ≥ 2.6.
- **The SQL peek cannot carry a binary-output plugin's changes** — on a slot with pending
  WAL, `pg_logical_slot_peek_changes` under `pglogical_output` is refused with exactly
  `feature_not_supported` (0A000 — OBSERVED live; NOT `XX000` as first recorded) — the
  SAME SQLSTATE class as wal2json's format-bound rejection. The §4 option pre-flight
  therefore rejects ONLY on `invalid_parameter_value` (22023, the startup callback's
  option refusal) and probes pglogical with `proto_format 'json'` (textual output; the
  same startup option processing) so a pglogical resume with pending WAL is never
  falsely halted. Before this correction the probe halted every such resume with
  `:decoder_option_unsupported` (found by the crash-resume acceptance leg, 2026-09-30).
- **wal2json silently DROPS an update or delete on a table with no replica-identity index and
  identity ≠ FULL (including `REPLICA IDENTITY NOTHING`)** — the change never reaches the
  wire — OBSERVED. CORRECTED: the §6 `:decode_failure` old-key halt is reachable through
  pglogical's keyless UPDATE (delivered keyless, OBSERVED), not through wal2json, where the
  drop is documented to operators instead.
- **`messages: true` impossible on PG 9.6 and `T` impossible on PG 9.6/10** (server-side:
  `pg_logical_emit_message` and logical-decoding TRUNCATE arrived in PG 10/11) — CORRECTED
  matrix cells; those fixture legs run on 12 and 15 only.
- **wal2json silently DROPS an update/delete on a keyless table** (no replica-identity
  index and identity ≠ FULL) with only a server-side WARNING — wal2json.c's UPDATE/DELETE
  guard, OBSERVED in source; no option exists to make it error and no wire signal exists.
  RESOLVED 2026-09-30 (beyond documentation): a configured keyless table halts
  `{:decoder, :table_keyless}` at connect (both catalog paths), with
  `allow_keyless_tables: true` as the explicit insert-only opt-in. pgoutput refuses the
  write at the server (fail-closed at the source) and pglogical DELIVERS the keyless
  update, which halts at decode — the divergence was wal2json-only.
- **pglogical re-emits relation metadata after DDL** (a relcache-invalidation callback,
  pglogical_relcache.c — OBSERVED in source), so dropped columns classify `:destructive`
  immediately there, exactly like pgoutput. wal2json format 2 has NO relation messages,
  so a dropped column was detectable only at reconnect. RESOLVED 2026-09-30: two wire
  rules (a cached column absent from an INSERT — inserts carry every live column — or
  absent from an UPDATE when the cached type is fixed-width and can never be stored
  out-of-line) re-emit the subset relation through the shipped `column_dropped`
  classification, and the residual TOASTable-on-update-only case (wire-identical to the
  unchanged-TOAST sentinel) is bounded by the periodic catalog guard
  (`schema_check_interval`, default 30s). The false-positive direction is structurally
  excluded: the UPDATE rule fires only on types no column STORAGE setting can externalize.
- **A pre-PG15 pgoutput walsender still streams EMPTY `BEGIN`/`COMMIT` pairs** for
  transactions with zero published changes (every catalog-touching txn — DDL included;
  PostgreSQL 15 added the server-side skip) — OBSERVED live on the 12.22 substrate while
  running the full matrix rows (2026-09-30): empty transactions reached sinks, shifted
  kill-window checkpoints and acked filtered WAL past append frontiers. The assembler now
  suppresses them on the v1 path exactly as the streamed (proto-v2) path always had —
  no sink call, a state mirror acks the proven-empty WAL, an `:append_log` sink does not
  (Rule 3's append clause). On PG15+ nothing changes (the server skips them).

## Acceptance

- A real PostgreSQL 9.6 and a real 12, each with `pglogical` and `wal2json` installed,
  join the CI matrix and the local substrate with port mappings recorded in `AGENTS.md`
  the way the 15 to 18 mappings are. Images are built from a committed Dockerfile that
  compiles the two extensions against the official `postgres:9.6` and `postgres:12`
  images, pinned by digest. No mock, stub or canned payload stands in for a server.
- One fixture (a transaction touching every casted type, a TOASTed column updated without
  touching it, an update and a delete under each replica identity, a truncate, and a
  transactional message where the plugin can carry one) is committed. The delivered
  `%Replicant.Transaction{}` list is byte-identical, after `commit_lsn`, `xid` and
  timestamps are normalized, across pgoutput on 15, pglogical on 9.6 and 12, and wal2json
  on 9.6 and 12. The comparison is a test that reads the three deliveries and diffs them;
  a difference is a failure, not a documented deviation. The truncate and message legs run
  only where a plugin plus server can carry them (pgoutput on 15; wal2json on 12; pglogical
  and 9.6 carry neither — verification section), and the byte-identical core is the legs all
  five decoder×server cells share.
- Real captured bytes from the 9.6 and 12 servers (one frame per message kind per plugin)
  join the conformance suite with the same byte-flip tamper test the pgoutput fixtures
  carry, so each new fixture is proven to go red on mutation.
- Every halt in this ADR has a red-capable test: `:decoder_protocol_unsupported`,
  `:decoder_option_unsupported`, `:decoder_lsn_missing`, `:decoder_capability_unsupported`,
  `{:config, :decoder_unsupported_on_server}`, the shape-mismatch relation re-emit, and the
  `REPLICA IDENTITY NOTHING` update halt, each on the live server where it can occur.
- The crash-injection marquee (loss = 0, effect-dup = 0) runs once per new decoder on 12.
- The default path is untouched: with `decoder:` absent, every existing test passes and the
  emitted replication commands are byte-identical to 1.3.0.

## Options considered

- **Extend the pgoutput parser with plugin-specific clauses** rather than a behavior:
  rejected. The three formats share no bytes, and one module would carry three parsers
  behind one set of pattern matches; the value-free boundary is easier to prove per module.
- **wal2json format version 1**: rejected (whole-transaction payloads, no per-change LSN,
  the `write-in-chunks` option produces partial JSON the decoder would have to reassemble).
- **Derive replica identity from the stream alone for wal2json**: rejected. The stream
  reveals it only when an update or delete arrives, so a change to `NOTHING` on a quiet
  table would be invisible until it caused a halt; the catalog read at connect names the
  window instead of hiding it.
- **Support 10 to 14 through pgoutput only and skip the plugins**: rejected as incomplete.
  It leaves 9.6 unreadable and does not help a server whose operator can install no new
  extension but already runs pglogical.
- **Binary base types from pglogical** (`binary.want_binary_basetypes`): deferred. It would
  bypass the text casting layer and need a second casting contract; text output is what
  every other path delivers.

## Consequences

- Sinks see no new struct, callback or field; a sink written for pgoutput runs unchanged
  against a 9.6 server. The guarantees table in `docs/INVARIANTS.md` applies per checkpoint
  mode, not per decoder.
- Operators of 9.6 to 12 accept three documented differences: a replica-identity change is
  classified at the next reconnect (wal2json); a column DROP is invisible to the wal2json
  stream (an absent column is the unchanged-TOAST sentinel, so the drop surfaces as
  `unchanged:` until the next reconnect's catalog read); and `type_modifier` is `-1` where
  the plugin's stream does not carry it (wal2json parses real typmods from its type
  strings and pglogical takes them from the connect-time catalog read, so `-1` survives
  only on a catalog miss).
- The test matrix grows by two rows and one image build; the images are the maintainer's
  to keep building as the official base images age.
- The library gains its first plugin-specific error atoms; `Replicant.Error.reason/0` and
  the halt-reason table in `usage-rules.md` grow by the six reasons above.

## Non-goals

- No statement-based or test_decoding output; only the two plugins named here.
- No schema migration, extension installation or configuration change on the source; the
  plugin must already be installed and `wal_level` already `logical`.
- No pglogical subscription, node or replication-set management; Replicant reads a slot the
  operator created the sets for.
- No support for PostgreSQL 9.4 or 9.5 (no `confirmed_flush_lsn` on 9.4; both are outside
  what a 9.6 test can prove).
- No change to how 15 to 18 are read; pgoutput remains the default and the only decoder
  tested there.

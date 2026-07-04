# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added — Plan 1: offline CDC core (decode / assemble / validate / redact)

- Vendored `pgoutput` byte parser behind a **value-free-error decode boundary**
  (`Replicant.Decoder.decode/1`) — catches every raise and scrubs raw bytes so no
  row value ever reaches an error, log, or telemetry event (Critical Rule 1).
- Type-aware value casting (`Replicant.Casting.Types` / `Replicant.Casting.ArrayParser`)
  and the OID→type database (`Replicant.Decoder.OidDatabase`), vendored from walex
  (MIT; credited in `NOTICE`).
- `Replicant.Assembler` — pgoutput message stream → `%Replicant.Transaction{}`:
  transaction-granularity commit LSN, unchanged-TOAST extraction (the sentinel is a
  first-class `unchanged` list, never in `record`), watermark skip
  (`commit_lsn <= checkpoint`), additive/destructive schema-change classification
  with fail-closed halt, and synchronous per-transaction sink apply.
- Data contract structs: `Replicant.Transaction`, `Replicant.Change` (+ `Change.Column`),
  `Replicant.SchemaChange`; the LSN facade (`Replicant.lsn/0` uint64, `lsn_to_string/1`).
- `Replicant.Sink` behaviour with `@optional_callbacks` (a minimal sink compiles clean).
- Identifier allowlist (`Replicant.Identifier`) + validated slot/publication SQL builder
  (`Replicant.QueryBuilder`), hardening walex's raw interpolation against injection
  (Critical Rule 2).
- Structure-only telemetry allowlist (`Replicant.Telemetry`) — LSNs/counts/table
  names/durations/error classes only, never row values (Critical Rule 1).
- Typed, value-free `Replicant.Error`.
- Real-`pgoutput` byte conformance suite (walex-captured fixtures) covering every
  message type including the unchanged-TOAST sentinel and all replica-identity modes —
  runs with no live database.

### Pending — Plan 2 (next slice): live streaming + exactly-once

- `Replicant.Connection` (`Postgrex.ReplicationConnection`): slot lifecycle,
  ack-after-checkpoint + keepalive, slot-invalidation fail-closed halt,
  reconnect/backoff, and a go-forward-only start guard.
- The crash-injection integration suite (real PG16; kill at adversarial points; assert
  exactly-once). Not in this release.

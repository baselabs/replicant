# ADR-0003: The value-free error, log, and telemetry boundary (Critical Rule 1)

**Status:** Accepted
**Date:** 2026-08-12
**Deciders:** replicant maintainer (1.0 hardening D8; records the Plan-1 decision that shipped in 0.1.0)

## Context

Every row value that flows through this library must be assumed to be **PII or a secret**.
Two facts make naive error handling dangerous:

1. The vendored `pgoutput` byte parser (walex/MIT, credited in `NOTICE`) raises on malformed
   WAL and its exception messages embed the raw bytes it could not parse — i.e. row values.
2. A sink call is a user-code boundary: a `raise`/`throw`/`exit` from sink code (e.g. a
   `GenServer.call` timeout) can embed the transaction's row values in its reason term.

If any of these reached an `OTP crash log`, a `Logger` call, a telemetry event, or a surfaced
`%Error{}`, the library would become a PII-exfiltration path. The error/log/telemetry surface
is exactly where operators route to centralized (and often replicated) observability systems.

## Decision

A single **value-free boundary** wraps every path that can surface a value. The boundary is
enforced at four mechanically distinct points:

- **Decode boundary** — `Replicant.Decoder.decode/1` runs the vendored parser inside a `rescue`
  and `catch`, scrubbing any fault into a `%Replicant.Error{reason: :decode_failure, shape:
  inspect(mod)}` that keeps only a structural module name, never bytes or values.
  (`lib/replicant/decoder/decoder.ex`.)
- **Assembler boundary** — `Replicant.Assembler.handle_message/2` wraps the per-message work
  in `rescue`/`catch`, AND **every sink-call site** (`deliver_now`, `deliver_message`,
  `flush_sink_batch`, `apply_chunk`, `commit_txn` checkpoint write, `checkpoint/0` read)
  independently scrubs `raise`/`throw`/`exit` to a value-free `:sink_failed` /
  `:checkpoint_store_failed` / `:snapshot_failed` reason. This includes the batch-flush and
  snapshot-chunk paths that run OUTSIDE `handle_message/2`'s rescue. (`lib/replicant/assembler.ex`,
  `lib/replicant/assembler_server.ex`, `lib/replicant/snapshotter.ex`.)
- **Telemetry allowlist** — `Replicant.Telemetry` defines `@allowed_meta_keys` (LSNs, table
  names, counts, durations, error-class atoms, `slot_name`, `attempt`, `transactional`) and
  `validate!/1` RAISES on any off-allowlist key. Telemetry metadata is the only external
  observable channel besides the halt itself. (`lib/replicant/telemetry.ex`.)
- **Halt reason discard** — `Replicant.Supervisor.halt/2` accepts a reason for symmetry but
  DISCARDS it (`_reason`); the halt tears the pipeline down from an unlinked spawned process
  (to avoid the self-termination deadlock), and the only outward signal is the value-free
  telemetry `[:replicant, :*, :halted]`. (`lib/replicant/supervisor.ex`.)

There is **no `Logger` usage and no `IO.*` usage** anywhere in `lib/` — the library's only
observable channels are telemetry and the halt.

## Options Considered

### Option A: Log, then redact
**Cons:** Logging infrastructure (backends, formatters, structured-log shippers) is a wide and
version-varying surface; a redaction layer that runs after a value is formatted can miss a new
backend or a `%Inspect` path. Rejected — the value must never be materialized in a loggable form.

### Option B: Trust callers (sinks, operators) not to log values
**Cons:** The library cannot enforce caller behavior, and the most dangerous values are the ones
embedded in sink `exit` reasons and vendored-parser raises — neither of which a caller controls.
Rejected.

### Option C: Value-free boundary at every surfacing path (adopted)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low — one scrapping discipline applied at each surfacing site |
| Security | High — no value can reach an error, log, telemetry event, or crash dump by construction |
| Diagnosability | Trade: errors carry a structural `shape` + fixed atom `reason`, never the offending bytes |

## Trade-off Analysis

The boundary trades diagnostic detail (an operator sees `:decode_failure` + `shape:
"Postgrex.Error"`, never the bad bytes) for a hard guarantee that the error/log/telemetry surface
is not a PII path. That trade is load-bearing for a CDC consumer whose entire payload is user
data. The structural `shape` field is enough to triage (which subsystem faulted) without the
bytes; deeper diagnosis is done against the source WAL with direct DB access, not via library
emissions.

## Consequences

- **Easier:** Operators can route `Replicant` telemetry and errors to any centralized
  observability system without a redaction layer; a malformed WAL or a crashing sink cannot leak
  values through the library's own surfaces.
- **Harder:** Every new code path that can surface a value (a new sink-call site, a new fault
  class) must add the same `rescue`/`catch` + scrub. A future edit that drops the catch on a
  sink-call path is a Critical-Rule-1 regression; the closeout security lens exists to catch it.
- **Revisit if:** the library ever ships a `Logger`-based diagnostic mode — that would need its
  own value-scrubbing layer and is currently a non-goal (no `Logger` in `lib/`).

## Verification

`grep -rn 'Logger\|IO\.\|IO\.inspect' lib/` → zero matches. Every sink-call site lists an
explicit value-free catch (corroborated by the security audit at the 1.0 readiness review,
2026-08-12). The decode boundary's scrub is exercised by the conformance suite's malformed-byte
fixtures.

# ADR-0005: Spill is ephemeral non-fsync'd scratch, not a durable WAL

**Status:** Accepted
**Date:** 2026-08-12
**Deciders:** replicant maintainer (1.0 hardening D8; records the `replicant-streaming-spill` decision that shipped in 0.1.0)

## Context

A single in-progress streamed transaction can be **larger than memory**. Without a spill path the
bounded in-flight window halts fail-closed at the first oversized transaction — a hard ceiling on
transaction size. Spilling the transaction to disk introduces an at-rest surface: spilled frames
are `:erlang.term_to_binary({subxid, %Change{}})`, i.e. **cleartext row values** for the duration
of the transaction. Two design questions follow:

1. Is spill a durable WAL (fsync'd, crash-surviving) or ephemeral scratch?
2. Is the spill file encrypted at rest?

## Decision

- **Spill is ephemeral non-fsync'd scratch.** Frames are written to a per-txn file under a `0700`
  spill directory with `0600` files, length-prefixed, write-once-read-once within a single
  transaction's lifetime, deleted on commit/abort/reset/halt, and swept per-slot on every
  reconnect. There is **no fsync** — spill is not a WAL; a crash re-streams the transaction from
  the durable `confirmed_flush` LSN (the spill is discarded, never replayed).
  (`lib/replicant/spill.ex`, `lib/replicant/spill/reader.ex`.)
- **Independent queue, resident, and disk budgets.** Socket intake pauses on actual
  queued payload bytes at `max_inflight_lag` (or 512 frames); completed assembler
  frames release credits. WAL-position distance is not used as a memory estimate.
  Streamed resident payload spills reclaimable row buffers above `max_inflight_lag`;
  messages remain in memory and charged. Spill repeats until the budget is met or
  no row payload can be reclaimed. `max_spill_bytes` separately limits spill files
  (exceeding it halts `:spill_exhausted`). With spill configured, non-streamed
  transactions retain a `max_inflight_lag + max_spill_bytes` **memory** allowance
  for compatibility, since they cannot use the streamed spill path. None of these
  budgets is an exact RSS limit: decoded-term overhead and socket buffers are extra.
- **No in-lib encryption.** Key management is a separable subsystem with its own operational
  surface (KMS, rotation, envelope keys) that this library does not own. The operator places the
  spill `dir` on a secure/encrypted volume and cleans it on decommission; the default `dir` is a
  `0700` subdir of the OS temp dir. (`lib/replicant/spill.ex`, documented in the README.)
- **Value-free on fault.** Any spill `File.*` / `binary_to_term` fault scrubs to a fixed
  `Replicant.Spill.Error{reason: :spill_io_failed}` (no bytes). `binary_to_term` uses `[:safe]`
  (no atom/function creation) and shape-validates the frame; the snapshot-resume-token path
  additionally rejects the compressed external-term format before deserialization (a
  decompression-bomb guard). (`lib/replicant/spill/reader.ex`, `lib/replicant/snapshot_progress.ex`.)

## Options Considered

### Option A: fsync'd durable spill (treat it like a WAL)
**Cons:** Spill is NOT a WAL — its correctness comes from re-streaming on crash, not from
surviving one. fsync'ing would impose disk-latency on the hot path for no correctness gain (the
durable anchor is the slot's `confirmed_flush`, not the spill file). Rejected.

### Option B: In-lib blanket encryption of spill files
**Cons:** Key management is its own operational subsystem (provisioning, rotation, envelope
encryption). Embedding it here would couple a CDC library to a KMS and force a key-handling
contract on every consumer. The `0600`/`0700` permissions + scratch-lifetime + operator-placed
encrypted volume is the documented mitigation. Rejected as a library responsibility; noted as a
clean future addition (a persistent `dir` is the operator's to secure). See the 1.0 P2 backlog.

### Option C: Ephemeral non-fsync'd scratch, no in-lib encryption (adopted)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low — one `File.*` boundary, no fsync, no key management |
| Correctness | High — loss=0 via re-stream; the spill is never the durable anchor |
| Security | Medium — cleartext at rest during a spill; mitigated by 0600/0700 + scratch lifetime + operator-secured volume |

## Trade-off Analysis

The adopted design treats spill as scratch whose only job is to keep a too-large transaction out
of RAM until its `StreamCommit`. Correctness does NOT depend on the spill surviving a crash — it
depends on the durable `confirmed_flush` watermark + idempotent sink, which already exist. Paying
fsync or encryption cost would optimize a non-load-bearing property. The residual cleartext-at-rest
risk during a spill is real and documented; it is the operator's to mitigate via the spill `dir`
placement (the default OS-temp location is cleared by the OS).

## Consequences

- **Easier:** Oversized transactions deliver effect-once without halting; no fsync latency on the
  hot path; no key-management surface in the library.
- **Harder:** Spilled row values are cleartext at rest for the duration of one transaction. This
  is a deployment-time responsibility (point `dir` at a secure volume) and is documented in the
  README "Operator guidance" and noted in the 1.0 P2 backlog (in-lib blanket encryption is a clean
  future addition).
- **Revisit if:** the threat model includes an attacker who can read the owner's filesystem
  mid-spill AND the operator cannot place `dir` on an encrypted volume — then in-lib encryption
  (Option B) becomes the right call and is additive (a `streaming: [spill: [encrypt: true]]` knob
  over a key-management subsystem).

# Replicant — binding invariants for a sink author

These five invariants are the **contract between `replicant` and any sink / consumer**. They are
the published form of the Critical Rules that govern the library's own implementation (the full
narrative lives in the contributor guide). A sink that violates one is a bug in the sink; a
library change that violates one is a bug in the library.

## 1. No row value in an error, log, or telemetry event

Assume every value is PII or a secret. The library guarantees that **no row value ever reaches an
error, a log line, a telemetry event, or a crash dump through its own surfaces** — decode faults
are scrubbed to a value-free `Replicant.Error{reason, shape}`, telemetry metadata is allowlisted
to LSNs / table names / counts / durations / error-class atoms, and there is no `Logger` or `IO.*`
usage in `lib/`. The sink must uphold the same boundary: do not log `record` values, do not embed
them in error reasons, do not emit them in telemetry. (Governing ADR:
[0003](adr/0003-value-free-error-boundary.md).)

A logical-decoding message's `content` and `prefix` are **user bytes** — they are treated exactly
like row values under this rule.

## 2. Validate identifiers before they reach SQL

Slot and publication names pass through `Replicant.Identifier.validate/1` (a strict
`[a-z_][a-z0-9_]{0,62}` Postgres-identifier allowlist) before any SQL interpolation; a
multi-publication list validates every name and fails closed on a missing publication. Column names
reaching SQL are server-quoted (`format('%I')` / `quote_ident`). A sink that interpolates
catalog-sourced identifiers into its own SQL must apply the same discipline.

## 3. Exactly-once is at-least-once + a transaction-watermark-idempotent sink

The honest construction. The unit of delivery and of the watermark is the **transaction**, keyed by
its single `commit_lsn` (every row in a `pgoutput` proto-v1 transaction shares one commit LSN). A
sink MUST skip any transaction whose `commit_lsn <= checkpoint` and upsert rows by table PK. The
guarantee is stated **per mode**, never as a naked exactly-once:

- **Transactional sink + transactional path → effect-once** (dup=0, loss=0): the sink persists
  rows + checkpoint atomically in one DB transaction; re-delivery upserts to zero net effect.
- **Non-transactional sink / `handle_message/2` non-transactional messages / lib-mode incremental
  snapshot chunks → at-least-once, duplicate-bounded**: no dedup key, so duplicates are possible on
  reconnect. State this guarantee honestly to your consumers; do not claim effect-once for these.

The slot ack advances only after the sink durably commits (ack-after-checkpoint). (Governing ADR:
[0004](adr/0004-commit-lsn-transaction-watermark.md).)

## 4. Unchanged TOAST is a sentinel, not a value

An UPDATE that does not touch a TOASTed column sends a sentinel, not the value. The library
surfaces it as a first-class `unchanged: [col]` list on `Replicant.Change`; the sentinel never
appears in `record`. A sink MUST leave those columns untouched on upsert (do not overwrite them
with NULL or a placeholder). A change of replica identity or a dropped column classifies as
`:destructive` and halts fail-closed.

## 5. Stay tenant-blind

This library is deliberately **tenant-blind and classification-blind**. There is no multitenancy,
scope, or classification logic in `lib/` — that boundary is the whole reason `replicant` and
`ash_replicant` are separate packages. Do not add tenant/scope/classification concerns to a sink
that targets the core library; put them one layer up.

---

## Reference: delivery modes and their guarantees

| Mode | When | Guarantee |
|---|---|---|
| Sink-owned, per-transaction (default) | `handle_transaction/1` | **effect-once** (transactional sink) — dup=0, loss=0 |
| Sink-owned batch delivery | `batch_delivery:` + `handle_batch/1` | **effect-once** (atomic multi-txn write) — dup=0, loss=0 |
| Lib-owned checkpoint store | `checkpoint_store:` | **at-least-once, dup-bounded to one transaction** — never loss |
| Non-transactional message | `messages: true` + `handle_message/2` | **at-least-once** — duplicates possible on reconnect |
| Transactional message | `messages: true`, rides `%Transaction.messages` | **effect-once** (inherits the txn's `commit_lsn` dedup) |
| Initial snapshot (`snapshot: true`) | `handle_snapshot/2` | sink-owned effect-once chunks; lib-mode dup ≤ 1 chunk |
| Incremental snapshot | `snapshot: [mode: :incremental]` | sink-owned effect-once chunks; lib-mode dup ≤ 1 chunk |
| Spilled oversized transaction | `streaming: [spill: [...]]` | **inherits the active checkpoint-mode guarantee** (effect-once sink-owned; at-least-once lib-mode — spill adds no duplicates), delivered as a single-pass lazy `changes` |

See the [README](../README.md) for the full configuration reference and the [ROADMAP](ROADMAP.md)
for the feature tracker.

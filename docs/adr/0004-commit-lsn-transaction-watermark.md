# ADR-0004: The commit-LSN transaction-granularity watermark (Critical Rule 3)

**Status:** Accepted
**Date:** 2026-08-12
**Deciders:** replicant maintainer (1.0 hardening D8; records the Plan-1/Plan-2 decision that shipped in 0.1.0)

## Context

"Exactly-once delivery" is widely overclaimed. Without two-phase commit between the source WAL
and the sink, true exactly-once is impossible; the honest construction is **at-least-once
delivery + a transaction-watermark-idempotent sink**. Two design questions follow:

1. At what granularity is the watermark — per-row or per-transaction?
2. What LSN does the slot-ack report — the received `wal_end` or the durably-checkpointed one?

A per-row LSN is unworkable: in a `pgoutput` proto-v1 transaction, **every row shares the
transaction's single commit LSN**, so a per-row watermark would collapse an N-row transaction
into one row and provide no intra-transaction dedup. And acking the received `wal_end` (the
upstream `walex` behavior) is at-most-once: a crash between dispatch and persist then loses rows
— the slot has already advanced past them.

## Decision

- **The watermark is the transaction's `commit_lsn`** (a single `non_neg_integer`, the 64-bit
  `(xlog_file <<< 32) ||| xlog_offset`). The sink skips any transaction whose `commit_lsn <=
  checkpoint` and upserts rows by table PK. Plain integer comparison is correct WAL ordering.
  (`lib/replicant/transaction.ex`, skip in `lib/replicant/assembler.ex`.)
- **The ack reports the last durably-checkpointed LSN**, never the received `wal_end`. A
  keepalive reply and the async ack both report `checkpoint_lsn`; the slot advances over
  un-persisted WAL only in the narrow idle case (a quiet-but-filtered publication carries nothing
  for the publication, verified safe by the transaction-boundary idle predicate). A crash between
  dispatch and persist therefore re-delivers from the durable `confirmed_flush` and the idempotent
  sink dedups. (`lib/replicant/connection.ex`.)
- **The guarantee is stated per mode, not naked.** Sink-owned transactional delivery is
  effect-once (dup=0). Lib-mode (`checkpoint_store`), non-transactional `handle_message/2`, and
  lib-mode incremental-snapshot chunks are honestly **at-least-once, duplicate-bounded** — the
  library never claims a naked exactly-once.

## Options Considered

### Option A: Per-row LSN watermark
**Cons:** Every row in a pgoutput proto-v1 transaction shares one commit LSN, so a per-row key
collapses an N-row transaction to one. Rejected — it provides no intra-transaction dedup and
misrepresents the protocol.

### Option B: Ack the received `wal_end` (the upstream `walex` fire-and-forget ack)
**Cons:** This is at-most-once. A crash between dispatch and persist loses rows the slot already
advanced past. Rejected — it violates the zero-loss posture.

### Option C: Commit-LSN transaction watermark + ack-after-checkpoint (adopted)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low — plain integer comparison + a single durable watermark |
| Correctness | High — loss=0 unconditional; dup bounded (0 for transactional sinks, ≤1 txn/batch for non-txn) |
| Honesty | High — the guarantee is stated per mode, never as a naked exactly-once |

## Trade-off Analysis

The adopted design rests the exactly-once CLAIM on the sink being transactional + idempotent (it
persists rows + checkpoint atomically, then upserts by PK on re-delivery). That is the only
honest path without two-phase commit. The cost is that a non-transactional sink (files, external
APIs) cannot reach effect-once — the library offers lib-mode `checkpoint_store` for those sinks
and states the at-least-once bound plainly rather than overclaiming.

## Consequences

- **Easier:** The watermark is one integer per transaction; comparison is trivial; the sink
  contract is "skip `commit_lsn <= checkpoint`, upsert by PK, persist the new checkpoint." For an
  Ash/Postgres sink the idempotency half is best delegated to [`ash_onetime`](https://hex.pm/packages/ash_onetime)
  (`strategy :idempotency` keyed on `commit_lsn`) — authoritative DB-constraint admission rather
  than a hand-rolled pre-check; replicant supplies the key, ash_onetime decides the replay.
- **Harder:** The ack-after-checkpoint seam is load-bearing — an edit that acks `wal_end` instead
  reintroduces at-most-once loss. The idle-ack advance is gated on a transaction-boundary
  predicate precisely so it can never ack past an undelivered transaction or message.
- **Revisit if:** proto-v3 two-phase streaming (`Stream Prepare`) is added — a prepared txn would
  need its own "prepared-but-not-committed" concept in the idle predicate, but the commit-LSN
  watermark itself is unchanged.

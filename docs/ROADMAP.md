# Replicant — Feature Tracker

**Updated:** 2026-07-06 · **Core HEAD:** `7bfa193` · **Branch:** `main`

Living tracker for what remains after the v1 zero-loss streaming core. The
prose summary in [`README.md`](../README.md#roadmap) mirrors this; this file is
the detailed source. Ranked by **value only** — build order follows what
unlocks the most, never what is cheapest.

## Sequencing (user directive, 2026-07-05)

**One in-build project at a time.** `ash_replicant` (a separate standalone lib)
does **not** start until `replicant` is **completed in full and published**.
Everything below is replicant-internal until that gate clears.

**Resolved (2026-07-05):** "completed in full" = build the remaining §3
functional slices, THEN publish `replicant` to Hex, THEN start `ash_replicant`.
Value-ranked build order: **(1) checkpoint-store → (2) batching → (3) proto≥2
streaming.** checkpoint-store leads on value (unlocks a new sink class:
non-transactional targets like files / external APIs) and on sequencing (it
defines the full checkpoint model that batching's checkpoint-granularity change
builds on). `ash_replicant`, a transactional Ash sink, needs none of the three —
it is gated purely by the publish milestone, not by these slices. **Active:
checkpoint-store CLOSEOUT-REVIEWED + RESOLVED 2026-07-05 (`/review-autopilot --fix`, HEAD `2d23832`) —
graded 89/100, **→ 100 on the user ruling** for the sole design-decision (F3: spec §4/§8
"halt fail-closed" on persistent store outage vs the shipped unpaced disconnect-retry; loss=0
holds either way). 6 findings FIXED with proven-red-capable tests — 1 blocking (store-outage
`DBConnection.ConnectionError` crashed the store GenServer), 1 should-fix (store call-exit
crashed the Connection), 3 note, 1 advisory (DELETE op-class coverage); +5 tests → gate battery
ALL-PASS 231/0, dialyzer 0. **F3 RESOLVED (user ruled 2026-07-05): pull §14.18 forward — the
unpaced-retry interim is now RESOLVED by `replicant-store-fault-retry` (§14.18
bounded-retry-then-halt, **shipped 2026-07-05**: transient blip self-heals within N,
persistent outage retries N then HALTS + alerts).** **`replicant-batching` ✅ SHIPPED + CLOSED-OUT 2026-07-06 (exec-autopilot 5/5 then /review-autopilot --fix → 100/100 grader-verified). Both closeout design-decisions user-ratified + implemented: (1) LSN-span cap base → `max(lib_checkpoint, stream_floor)` (floor-corrected, spec §15 amendment; fixes fresh-slot cold-start); (2) batch discarded on mid-stream reconnect (spec §15 new §9 invariant). Cross-vendor Codex caught 2 uniques. Lib-mode batched-checkpointing only; sink-owned → `replicant-batch-delivery` ✅ executed 2026-07-06 (row 2b). [design](superpowers/specs/2026-07-05-replicant-batching-design.md).**

## Shipped — v1 streaming core (fail-closed, reviewed, green)

| Slice | What landed | Closeout |
|---|---|---|
| Plan 1 — offline core | decode / assemble / validate / redact; value-free boundary, TOAST sentinel, watermark skip, schema-change classification, identifier allowlist, sink behaviour, telemetry, real-`pgoutput` byte conformance | 100/100 |
| Plan 2 — live streaming + exactly-once | `Connection` owns the slot, ack-after-checkpoint, slot-invalidation fail-closed halt, `AssemblerServer`, per-pipeline supervision, §4 bounded in-flight window, real-PG16 crash-injection (loss=0, effect-dup=0) | 89→100 |
| replicant-snapshot — initial backfill | `snapshot: true`: `EXPORT_SNAPSHOT` → `COPY` at `consistent_point` → hand off at snapshot LSN, gap-free/dup-free; `handle_snapshot/2` + `handle_snapshot_complete/1`; mid-COPY crash halts `:snapshot_incomplete` | 100/100 |

Test evidence (HEAD): unit 184/0, integration 197/0 (`2699e72`, one commit
back; HEAD is a test-only de-flake), dialyzer 0.

## Remaining — spec §3 non-goals, each a named future slice

The v1 primitive is **fail-closed without any of these** — absence refuses
partial delivery rather than doing it silently.

| # | Slice | Unlocks | Depends on | Status |
|---|---|---|---|---|
| — | **`ash_replicant`** (standalone lib) | The first-class consumer: an Ash/Postgres sink adapter carrying multitenancy + classification, one layer up from the tenant-blind core. This is what turns the primitive into a usable product. | `replicant` published (gate) | **Deferred until `replicant` is published** (one in-build project at a time). Highest intrinsic value, sequenced last by directive. New repo at `/Users/rp/Developer/Base/ash_replicant` (bare skeleton). Sibling to `arcadic`→`ash_arcadic`. |
| 1 | `replicant-checkpoint-store` | Non-transactional sinks (files, external APIs): a lib-owned checkpoint table with a mandatory checkpoint-**after**-persist write order (dup, never loss). | Core (shipped) | ✅ Shipped + closeout-ready 2026-07-05 (13/13 tasks, exec-autopilot) · `/review-autopilot --fix` HEAD `2d23832`: graded **89/100 → 100** on user ruling for the sole design-decision; 6 findings fixed, gate battery ALL-PASS 231/0 dialyzer 0. **F3 RESOLVED** (2026-07-05: build §14.18 next — spawns slice 1b). [design](superpowers/specs/2026-07-05-replicant-checkpoint-store-design.md) · [plan](superpowers/plans/2026-07-05-replicant-checkpoint-store.md) · [review](superpowers/reviews/2026-07-05-replicant-checkpoint-store-lens-reports.md) |
| 1b | `replicant-store-fault-retry` (§14.18) | Bounded-retry-then-halt on a checkpoint-store fault: a transient blip self-heals within N; a persistent outage retries N times then **HALTS + alerts the operator** (replaces the current UNPACED connect-retry interim + the immediate mid-stream write halt). Closes checkpoint-store closeout F3. | `replicant-checkpoint-store` (shipped) | ✅ **Shipped 2026-07-05** (exec-autopilot; 7/7 tasks, per-task two-stage opus review; unit **224/0**, integration **25/0**, dialyzer 0, credo/format clean; 0 tier escalations, 3 test-hardening review-fix rounds T1/T5/T6). Closeout `/review-autopilot` pending. Design adversarially reviewed (9 challenges 8-acc/1-refuted); plan machine-gated + independently reviewed (5/5 fixed). [design](superpowers/specs/2026-07-05-replicant-store-fault-retry-design.md) · [plan](superpowers/plans/2026-07-05-replicant-store-fault-retry.md) |
| 2 | `replicant-batching` | Throughput: batched **checkpointing** for lib mode — defer the lib-owned checkpoint write + slot ack to once per batch of N txns (sink delivery stays per-txn). Amortizes the synchronous serial store round-trip. Per-transaction checkpointing is the correctness baseline it optimizes. | Core + checkpoint-store + store-fault-retry (shipped) | ✅ **Shipped + closed-out 2026-07-06** (exec-autopilot 5 tasks 2 opus/3 sonnet, 0 tier escalations/0 review-fix rounds → `/review-autopilot --fix` **100/100 grader-verified**; post-fix HEAD `7bfa193`, unit **249/0** + integration **28/0**, dialyzer 0, credo **677/0**, format/compile clean). Commits: config `c22f03e`, assembler/server `2ef4d7d`, pipeline `3c4734b`, integration `ce9bf25`, docs `904d153`; closeout fixes `113a2a4`/`65040f3`/`7a9812f`/`c6c1c48`/`85672f1`/`7bfa193`. Both closeout design-decisions user-ratified + implemented (spec §15 amendments): LSN-span base → `max(lib_checkpoint, stream_floor)`; batch discarded on mid-stream reconnect. Cross-vendor Codex: 2 uniques (reconnect-stale-batch, value-free-leak). Opt-in `checkpoint_store: [batch: [max_transactions: 100, max_delay_ms: 1000]]` + auto LSN-span lag-cap (`max_inflight_lag/4`); dup bound widens to one batch (crash + graceful stop + mid-stream reconnect), loss=0 unconditional. Sink-owned batching → `replicant-batch-delivery` ✅ executed 2026-07-06 (row 2b). [design](superpowers/specs/2026-07-05-replicant-batching-design.md) · [plan](superpowers/plans/2026-07-05-replicant-batching.md) · [review](superpowers/reviews/2026-07-06-replicant-batching-lens-reports.md) |
| 2b | `replicant-batch-delivery` | Sink-owned (transactional) batching: a new `handle_batch` sink callback delivering N transactions as ONE atomic unit — amortizes the *sink's own* commit cost (distinct from `replicant-batching`'s library-owned checkpoint-write amortization). Carved out of `replicant-batching` because it needs a mechanically distinct delivery contract + atomic-multi-txn semantics (a fresh-context reviewer confirmed the boundary is mechanical, not effort). | `replicant-batching` (shipped) | ✅ **Executed 2026-07-06** (exec-autopilot, 9 tasks: 5 opus / 3 sonnet / 1 haiku; per-task two-stage opus review, 4 review-fix rounds total, **0 tier escalations**). Post-sweep HEAD `6387e35` (range `7bfa193..6387e35`, 8 code/test/docs commits): **unit 271/0 + integration 31/0**, dialyzer 0, credo 741/0, format/compile clean (gate-log `20260706-071040-bd-full-sweep-6387e350f1`). **Effect-once (dup=0) crash-injection marquee GREEN against live PG16** (mid-batch atomic-checkpoint fault rolls the whole `handle_batch` txn back → resume re-delivers dup=0). Designed via brainstorm-autopilot, user-approved; fresh-context adversarial review folded — 11 challenges / 3 blocking, all reconciled. Effect-once preserved (dup=0, loss=0) — the transactional idempotent sink writes N txns + checkpoint atomically; **stronger** than batching's dup≤batch. Opt-in top-level `batch_delivery: [max_transactions: 100, max_delay_ms: 1000]` (sink-owned only; `+ checkpoint_store` ⇒ `:config_invalid`; missing `handle_batch/1` ⇒ `:batch_unsupported`). 6 lib modules (sink/config/assembler/assembler_server/pipeline/connection); telemetry unchanged. **CLOSED-OUT 2026-07-06 via `/review-autopilot` (8 lenses): 97 → 100 after one in-session fix.** The lone should-fix (`flush_sink_batch`'s result `case` lacked a value-free catch-all → a non-conforming `handle_batch` RETURN raised `CaseClauseError` in the flush path — which runs via `do_flush`, outside `handle_message/2`'s value-free rescue — leaking the returned term into the OTP crash log, Critical Rule 1; caught by the security lens **and** the Codex cross-vendor lens, duplicate/live-repro'd) was fixed in commit `215d58b` (value-free `_unexpected` catch-all + red-capable test RED→GREEN; spec §8 matrix + §14.19 amended). Fresh full battery @ `215d58b`: credo **744/0**, dialyzer 0, unit **303/0**, integration **31/0** (marquee dup=0 green). [design](superpowers/specs/2026-07-06-replicant-batch-delivery-design.md) · [review](superpowers/reviews/2026-07-06-replicant-batch-delivery-lens-reports.md) |
| 3 | `replicant-streaming` | `pgoutput` proto ≥ 2 in-progress-transaction streaming (opt-in `streaming:`): decode Stream Start/Stop/Commit/Abort + (sub)xid-prefixed changes, reassemble each streamed txn **in memory** demultiplexed by top-level xid (interleave + subtransaction-abort filtering), deliver the complete `%Transaction{}` on Stream Commit through the **unchanged** sink contract. Primary benefit is server-side (walsender streams a large txn instead of buffering it whole); consumer memory bounded by the existing §4 halt. | Core (shipped) | 🏗️ **In build 2026-07-06** (brainstorm-autopilot → plan-autopilot → exec-autopilot; design fresh-context-reviewed — 9 challenges/3 blocking folded; wire format verified via a live PG16 pgoutput-v2 probe + PG16 protocol docs, incl. a savepoint-rollback `StreamAbort(top, subxid)`). **Plan: 10 tasks (7 opus/3 sonnet), machine-gated (plan-verify 0 errors) + independently reviewed (5 findings/5 fixed — 2 blocking: decoder-test path, marquee `go_forward_only`).** Sink contract + `telemetry.ex` unchanged; effect-once preserved. Next: `/exec-autopilot`. [design](superpowers/specs/2026-07-06-replicant-streaming-design.md) · [plan](superpowers/plans/2026-07-06-replicant-streaming.md) |
| 3-spill | `replicant-streaming-spill` | Consumer-side disk spill so a single transaction **larger than memory** streams through without halting — the "unbounded single-txn size" capability. Carved from row 3 (its first-disk-I/O + PII-at-rest + OTP-lifetime risk surface earns its own review); row 3 is fail-closed without it (a too-large streamed txn halts, as under v1). | `replicant-streaming` | Not started (named follow-on) |
| — | Multi-sink fan-out per slot | Deliberate non-goal, no slice: one slot = one sink = one checkpoint keeps the watermark tractable. Compose a dispatching sink or run multiple slots. | — | Won't build |

## Why `ash_replicant` is #1 (value-only ranking)

- **It is the reason the core exists.** README states it outright: replicant is
  "the reliable CDC consumer sibling to `arcadic`," with multitenancy,
  classification, and Ash resources living "one layer up, in a future
  `ash_replicant` sink adapter." Until it exists, the core has no first-class
  consumer.
- **The other three optimize a primitive that already works.** Checkpoint-store,
  batching, and proto-v2 each add value only under a specific pressure
  (non-transactional sinks / throughput / unbounded single-txn size). None
  delivers new end-user capability the way the consumer layer does.
- **Building the real consumer first de-risks the rest.** It exercises the Sink
  contract end-to-end and reveals whether checkpoint-store / batching are
  actually needed, rather than building them speculatively.

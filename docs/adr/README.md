# Architecture Decision Records

Product-shaping decisions (contracts, data model, architecture, security posture) that a bare
clone / CI / future maintainer must understand — recorded as plain numbered Markdown files in
the [Nygard ADR](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions.html)
format (Context → Decision → Options → Consequences).

These are the tracked, product-level companion to the on-disk deliberation notes under
`docs/superpowers/` (gitignored). An ADR records the DECISION and its trade-offs; the design
note records the deliberation. `docs/ROADMAP.md` rows link their governing ADR here.

ADRs are authored when a slice's stakes warrant one (a contract/data-model/architecture/
security-posture decision), not retroactively for pre-existing history.

| ADR | Title | Feature |
|-----|-------|---------|
| [0001](0001-logical-decoding-messages-delivery-guarantees.md) | Logical-decoding message delivery guarantees (transactional vs non-transactional) | A2 |
| [0002](0002-multi-publication-per-pipeline.md) | Multi-publication per pipeline (discovery union + fail-closed existence check) | A3 |

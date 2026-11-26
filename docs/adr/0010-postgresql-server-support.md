# ADR-0010: PostgreSQL server support

**Status:** Accepted 2026-11-26

## Context

PostgreSQL 14 reached end of life on November 12, 2026. PostgreSQL 19.0 shipped
on November 19. The [PGDG versioning policy](https://www.postgresql.org/support/versioning/)
currently lists PostgreSQL 15 through 19 as supported. Replicant 1.x also serves
older servers and output plugins under [ADR-0009](0009-pglogical-wal2json-decoders.md).

## Decision

Replicant supports the PostgreSQL majors supported by the PostgreSQL Global
Development Group (PGDG), with one real CI lane per supported major. Support for
a major ends on its published PGDG end-of-life date and is announced in the
compatibility documentation and changelog. A Replicant patch or minor release
does not raise the server floor or refuse a configuration that already works.
Removing a working configuration requires a Replicant major release. Until then,
retired servers may remain in compatibility CI to guard the existing contract.

Retain `decoder: :pglogical` on supported majors. pglogical can evaluate filters
that call user-defined functions and requires every applicable set predicate to
pass. Native publications restrict the expressions and combine publication
predicates with OR. Replicant's pglogical snapshots preserve the same
intersection as its stream.
[PostgreSQL row filters](https://www.postgresql.org/docs/19/logical-replication-row-filter.html)
· [pglogical filter parser](https://github.com/2ndQuadrant/pglogical/blob/REL2_4_8/pglogical_functions.c#L1266-L1387)
· [pglogical filter evaluation](https://github.com/2ndQuadrant/pglogical/blob/REL2_4_8/pglogical_output_plugin.c#L619-L672)

Deprecate PostgreSQL below 15 and `decoder: :wal2json` in 1.x. Replicant 2.0
will remove that server range, along with the wal2json-only options `tables:`,
`allow_keyless_tables:`, and `schema_check_interval:`. pglogical's
`replication_sets:` option stays.

## Qualification

The 1.x matrix retains the 9.6 and 12 compatibility lanes. PostgreSQL 15, 16,
17 and 19 run on pinned upstream images. The PostgreSQL 18 lane runs against the
platform engine and uses a same-major upstream companion for pglogical tests.
The wal2json builds remain on 9.6, 12 and 15.

The pinned pglogical 2.4.8 tag selects its PGXS build files via
`compat$(PGVER)`. It provides compatibility code for all five supported majors,
including an explicit PostgreSQL 19 port. PGDG publishes 2.4.8 packages for
each of those majors on Debian trixie amd64. Replicant CI builds from the source
pin against the matching server headers and checks pglogical against pgoutput
on each major.
[pglogical 2.4.8 Makefile](https://github.com/2ndQuadrant/pglogical/blob/REL2_4_8/Makefile)
· [PostgreSQL 19 port](https://github.com/2ndQuadrant/pglogical/commit/bfea88079a63d44d3ad4947bd36e4d17b6955cb1)
· [PGDG package index](https://apt.postgresql.org/pub/repos/apt/dists/trixie-pgdg/main/binary-amd64/)

## Wal2json migration

A logical slot names its output plugin at creation. To move to pgoutput, create
the publication and a new pgoutput slot, then hand off the durable checkpoint
at a verified cutover boundary. For a coordinated cutover on the same source,
quiesce writes, drain the old pipeline through its last committed change and
persist its checkpoint. Confirm the new slot's consistent point covers the
cutover before resuming writes. Validate the publication's table and operation
coverage and reconcile the sink's checkpoint with the new slot's origin before
starting consumption. Never carry an LSN across unrelated sources or advance a
checkpoint over an unverified write interval.

A fresh snapshot can rebuild a current-state mirror. It cannot reconstruct
missing append-log history. Preserve that history through the handoff or recover
it from an independently retained record before advancing the append-log checkpoint.

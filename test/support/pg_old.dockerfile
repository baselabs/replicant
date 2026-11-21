# ADR-0009 — the old-major test substrate: pglogical + wal2json compiled into the
# official postgres:9.6 and postgres:12 images (base pinned by digest; the tags are
# frozen upstream so the digest IS the reproducibility guarantee). Also builds the
# wal2json 2.4 snapshot under a second plugin name (`wal2json2_4`) — the live lever
# for the :decoder_option_unsupported halt (a build that predates
# numeric-data-types-as-string rejects the option wal2json ≥ 2.6 requires).
#
# Base images and decoder roles are declared once in .github/postgres-lanes.json.
# scripts/ci/postgres-lanes.sh validates them; the postgres action passes BASE.
# No default base: a missing build argument refuses before any build stage.
#
# The build context is the repo root. Plugin sources are pinned by commit (not a
# moving branch) so a rebuild reproduces the same bytes.
#
# The final image is the base image plus the plugin files and nothing else. Every
# `make install` writes into the DESTDIR staging tree /plugins, and only that tree
# is copied over the base. The plugins compile against server headers of exactly
# the base's server build ($PG_VERSION, set by the official image), fetched from
# PGDG's archive suite, which keeps every published build after the live suite
# moves on. The archive is served over HTTPS only, so ca-certificates goes in from
# the base's own sources first (the bookworm-based 12 image ships without it).
# Copying the build stage's whole lib directory instead put apt's newer
# minor-release server libraries (15.20) under the base's 15.19 server, and the
# server never started (CI, November 2026).

ARG BASE
FROM ${BASE} AS build

# The 9.6 image rides Debian stretch (archived upstream; PGDG's stretch archive has
# no arm64 suite at all). The Debian archive's stretch/updates carries
# postgresql-server-dev-9.6 for arm64; installing it needs the base image's PGDG
# libpq5 downgraded to Debian's matching build (--allow-downgrades pins the trio).
RUN set -eux; \
    case "$PG_MAJOR" in 9.6) \
      printf 'deb http://archive.debian.org/debian stretch main\ndeb http://archive.debian.org/debian-security stretch/updates main\n' > /etc/apt/sources.list; \
      rm -f /etc/apt/sources.list.d/*.list; \
      apt-get update -o Acquire::Check-Valid-Until=false; \
      apt-get install -y --no-install-recommends --allow-downgrades \
        "libpq5=9.6.24-0+deb9u1" "libpq-dev=9.6.24-0+deb9u1" \
        "postgresql-server-dev-9.6=9.6.24-0+deb9u1" \
        build-essential git ca-certificates libkrb5-dev libselinux1-dev \
        libxslt1-dev libpam0g-dev zlib1g-dev libssl-dev libedit-dev; \
      ;; \
    12|15) \
      apt-get update; \
      apt-get install -y --no-install-recommends ca-certificates; \
      sed -E 's#https?://apt\.postgresql\.org#https://apt-archive.postgresql.org#; s#-pgdg main.*$#-pgdg-archive main#' \
        /etc/apt/sources.list.d/pgdg.list > /etc/apt/sources.list.d/pgdg-archive.list; \
      grep -q 'https://apt-archive.postgresql.org' /etc/apt/sources.list.d/pgdg-archive.list; \
      apt-get update; \
      apt-get install -y --no-install-recommends build-essential git \
        libkrb5-dev libselinux1-dev libxslt1-dev libpam0g-dev zlib1g-dev libssl-dev libedit-dev \
        libzstd-dev liblz4-dev \
        "postgresql-server-dev-$PG_MAJOR=$PG_VERSION"; \
      ;; \
    *) echo "unsupported plugin substrate major" >&2; exit 1 ;; \
    esac

# pglogical 2.4.8 (output plugin `pglogical_output`) — OBSERVED protocol facts in
# .kimosabe/intents/pglogical-wal2json-decoders.md.
RUN set -eux; \
    git clone --quiet https://github.com/2ndQuadrant/pglogical.git /tmp/pglogical; \
    cd /tmp/pglogical; \
    git checkout --quiet 9a0e182745885ad0152ea387988c95a483396a81; \
    make -j"$(nproc)" PG_CONFIG=/usr/lib/postgresql/$PG_MAJOR/bin/pg_config; \
    make install DESTDIR=/plugins PG_CONFIG=/usr/lib/postgresql/$PG_MAJOR/bin/pg_config

# wal2json is pinned PER MAJOR, both pins commit-exact and carrying
# numeric-data-types-as-string (coverage verified live on the built images):
#   * pre-15 bases: the wal2json_2_6 tag's "Stamp 2.6" commit 75629c2 — the
#     committed substrate the 9.6/12 CI rows and their receipts are built on;
#     2.6 sources predate PG15's ReorderBufferTXN change and do not compile there.
#   * 15+ bases: master @ 75a4b494 ("Remove test_decoding from the regression
#     tests", 21 commits past 2.6) — the first committed-lineage pin carrying the
#     PG15/16 compile shims (txn->xact_time.commit_time under
#     PG_VERSION_NUM >= 150000).
# wal2json_2_4 resolves to 36fbee6cbb7e4bc1bf9ee4f55842ab51393e3ac0.
# The wal2json2_4 lever (the option-rejection halt's live substrate) builds only on
# the pre-15 majors — wal2json_2_4 predates PG15 and no 15+ lane needs it.
RUN set -eux; \
    git clone --quiet https://github.com/eulerto/wal2json.git /tmp/wal2json; \
    cd /tmp/wal2json; \
    case "$PG_MAJOR" in \
      9.6|12) git checkout --quiet 75629c2e1e81a12350cc9d63782fc53252185d8d ;; \
      15) git checkout --quiet 75a4b494a3fe0c4626e699de5bf2732079badb7b ;; \
      *) echo "unsupported plugin substrate major" >&2; exit 1 ;; \
    esac; \
    make -j"$(nproc)" PG_CONFIG=/usr/lib/postgresql/$PG_MAJOR/bin/pg_config; \
    make install DESTDIR=/plugins PG_CONFIG=/usr/lib/postgresql/$PG_MAJOR/bin/pg_config; \
    case "$PG_MAJOR" in \
      9.6|12) \
        mv /plugins/usr/lib/postgresql/$PG_MAJOR/lib/wal2json.so /plugins/usr/lib/postgresql/$PG_MAJOR/lib/wal2json_head.so; \
        git checkout --quiet 36fbee6cbb7e4bc1bf9ee4f55842ab51393e3ac0; \
        make -j"$(nproc)" PG_CONFIG=/usr/lib/postgresql/$PG_MAJOR/bin/pg_config; \
        make install DESTDIR=/plugins PG_CONFIG=/usr/lib/postgresql/$PG_MAJOR/bin/pg_config; \
        mv /plugins/usr/lib/postgresql/$PG_MAJOR/lib/wal2json.so /plugins/usr/lib/postgresql/$PG_MAJOR/lib/wal2json2_4.so; \
        mv /plugins/usr/lib/postgresql/$PG_MAJOR/lib/wal2json_head.so /plugins/usr/lib/postgresql/$PG_MAJOR/lib/wal2json.so \
        ;; \
      15) ;; \
      *) echo "unsupported plugin substrate major" >&2; exit 1 ;; \
    esac

FROM ${BASE}
COPY --from=build /plugins/ /

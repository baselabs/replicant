# ADR-0009 — the old-major test substrate: pglogical + wal2json compiled into the
# official postgres:9.6 and postgres:12 images (base pinned by digest; the tags are
# frozen upstream so the digest IS the reproducibility guarantee). Also builds the
# wal2json 2.4 snapshot under a second plugin name (`wal2json2_4`) — the live lever
# for the :decoder_option_unsupported halt (a build that predates
# numeric-data-types-as-string rejects the option wal2json ≥ 2.6 requires).
#
# Build (the substrate majors from the SAME Dockerfile; a 15+ base additionally
# needs output_plugin_libraries=wal2json,pglogical_output,pgoutput at server
# start — PG15+ gates EVERY output plugin, pgoutput included):
#   docker build -f test/support/pg_old.dockerfile --build-arg BASE=postgres:9.6@sha256:caddd35b05cdd56c614ab1f674e63be778e0abdf54e71a7507ff3e28d4902698 -t replicant-pg96-plugins:test .
#   docker build -f test/support/pg_old.dockerfile --build-arg BASE=postgres:12@sha256:2f2a8c2a7d10862e7fba2602e304523554f9df8244c632dafe2628ccb398fb5c -t replicant-pg12-plugins:test .
#   docker build -f test/support/pg_old.dockerfile --build-arg BASE=postgres:15@sha256:5f72c7b5bd616308ccfd2e74d6be16fb06364e5eecbb815fe9dc6ab9761d2111 -t replicant-pg15-plugins:test .
#
# The build context is the repo root. Plugin sources are pinned by commit (not a
# moving branch) so a rebuild reproduces the same bytes.

ARG BASE=postgres:12@sha256:2f2a8c2a7d10862e7fba2602e304523554f9df8244c632dafe2628ccb398fb5c
FROM ${BASE} AS build

# The 9.6 image rides Debian stretch (archived upstream; PGDG's stretch archive has
# no arm64 suite at all). The Debian archive's stretch/updates carries
# postgresql-server-dev-9.6 for arm64; installing it needs the base image's PGDG
# libpq5 downgraded to Debian's matching build (--allow-downgrades pins the trio).
RUN set -eux; \
    case "$PG_MAJOR" in 9.*) \
      printf 'deb http://archive.debian.org/debian stretch main\ndeb http://archive.debian.org/debian-security stretch/updates main\n' > /etc/apt/sources.list; \
      rm -f /etc/apt/sources.list.d/*.list; \
      apt-get update -o Acquire::Check-Valid-Until=false; \
      apt-get install -y --no-install-recommends --allow-downgrades \
        "libpq5=9.6.24-0+deb9u1" "libpq-dev=9.6.24-0+deb9u1" \
        "postgresql-server-dev-9.6=9.6.24-0+deb9u1" \
        build-essential git ca-certificates libkrb5-dev libselinux1-dev \
        libxslt1-dev libpam0g-dev zlib1g-dev libssl-dev libedit-dev; \
      ;; \
    *) \
      apt-get update; \
      apt-get install -y --no-install-recommends build-essential git ca-certificates \
        libkrb5-dev libselinux1-dev libxslt1-dev libpam0g-dev zlib1g-dev libssl-dev libedit-dev \
        libzstd-dev liblz4-dev \
        postgresql-server-dev-$PG_MAJOR; \
      ;; \
    esac

# pglogical 2.4.8 (output plugin `pglogical_output`) — OBSERVED protocol facts in
# .kimosabe/intents/pglogical-wal2json-decoders.md.
RUN set -eux; \
    git clone --quiet https://github.com/2ndQuadrant/pglogical.git /tmp/pglogical; \
    cd /tmp/pglogical; \
    git checkout --quiet 9a0e182745885ad0152ea387988c95a483396a81; \
    make -j"$(nproc)" PG_CONFIG=/usr/lib/postgresql/$PG_MAJOR/bin/pg_config; \
    make install PG_CONFIG=/usr/lib/postgresql/$PG_MAJOR/bin/pg_config

# wal2json is pinned PER MAJOR, both pins commit-exact and carrying
# numeric-data-types-as-string (coverage verified live on the built images):
#   * pre-15 bases: the wal2json_2_6 tag's "Stamp 2.6" commit 75629c2 — the
#     committed substrate the 9.6/12 CI rows and their receipts are built on;
#     2.6 sources predate PG15's ReorderBufferTXN change and do not compile there.
#   * 15+ bases: master @ 75a4b494 ("Remove test_decoding from the regression
#     tests", 21 commits past 2.6) — the first committed-lineage pin carrying the
#     PG15/16 compile shims (txn->xact_time.commit_time under
#     PG_VERSION_NUM >= 150000).
# The wal2json2_4 lever (the option-rejection halt's live substrate) builds only on
# the pre-15 majors — wal2json_2_4 predates PG15 and no 15+ lane needs it.
RUN set -eux; \
    git clone --quiet https://github.com/eulerto/wal2json.git /tmp/wal2json; \
    cd /tmp/wal2json; \
    case "$PG_MAJOR" in \
      9.*|10|11|12|13|14) git checkout --quiet 75629c2e1e81a12350cc9d63782fc53252185d8d ;; \
      *) git checkout --quiet 75a4b494 ;; \
    esac; \
    make -j"$(nproc)" PG_CONFIG=/usr/lib/postgresql/$PG_MAJOR/bin/pg_config; \
    make install PG_CONFIG=/usr/lib/postgresql/$PG_MAJOR/bin/pg_config; \
    case "$PG_MAJOR" in \
      9.*|10|11|12|13|14) \
        mv /usr/lib/postgresql/$PG_MAJOR/lib/wal2json.so /usr/lib/postgresql/$PG_MAJOR/lib/wal2json_head.so; \
        git checkout --quiet wal2json_2_4; \
        make -j"$(nproc)" PG_CONFIG=/usr/lib/postgresql/$PG_MAJOR/bin/pg_config; \
        make install PG_CONFIG=/usr/lib/postgresql/$PG_MAJOR/bin/pg_config; \
        mv /usr/lib/postgresql/$PG_MAJOR/lib/wal2json.so /usr/lib/postgresql/$PG_MAJOR/lib/wal2json2_4.so; \
        mv /usr/lib/postgresql/$PG_MAJOR/lib/wal2json_head.so /usr/lib/postgresql/$PG_MAJOR/lib/wal2json.so \
        ;; \
    esac

FROM ${BASE}
COPY --from=build /usr/lib/postgresql/$PG_MAJOR/lib/ /usr/lib/postgresql/$PG_MAJOR/lib/
COPY --from=build /usr/share/postgresql/$PG_MAJOR/extension/ /usr/share/postgresql/$PG_MAJOR/extension/

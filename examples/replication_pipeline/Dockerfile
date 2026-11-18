# The reference pipeline as one OTP release in one container.
# Build context is the REPO ROOT (the example path-depends on replicant):
#   docker compose build   (from examples/replication_pipeline)
#
# Builder and runtime use the same base so the release's ERTS always matches.
# Docker resolves FROM before COPY, so this immutable image reference cannot
# read .tool-versions. It is a separately pinned published container toolchain
# with a different OTP patch; it does not select the hosted CI runtime.
# Hosted example gates use .tool-versions and refuse runtime differences;
# the container's release is exercised by the end-to-end reference canary.
# The image is digest-pinned — hexpm/elixir date-suffixed tags have observed registry churn
# (policy says "never removed"; live probes show gaps), so the digest is the
# stable pin and the CI canary build is the daily pull-check.

FROM hexpm/elixir:1.20.3-erlang-29.0.5-debian-bookworm-20260803-slim@sha256:f2e721ffbb64a60cd56979c2d4e9651cf89c202c4a55466d979bf71b262802a3 AS base

FROM base AS builder

RUN apt-get update -y && apt-get install -y --no-install-recommends build-essential git ca-certificates && apt-get clean

WORKDIR /build

# replicant (path dep) is COPYed first so dep layers cache independently. The
# repo-root .dockerignore is a whitelist: only these paths ride the context.
COPY mix.exs README.md LICENSE NOTICE ./
COPY lib ./lib
COPY examples/replication_pipeline/mix.exs examples/replication_pipeline/mix.lock ./examples/replication_pipeline/
COPY examples/replication_pipeline/config ./examples/replication_pipeline/config
COPY examples/replication_pipeline/lib ./examples/replication_pipeline/lib

WORKDIR /build/examples/replication_pipeline

ENV MIX_ENV=prod

RUN mix local.hex --force && mix local.rebar --force \
    && mix deps.get --only prod --check-locked \
    && mix compile --warnings-as-errors

RUN mix release

FROM base AS runtime

RUN apt-get update -y && apt-get install -y --no-install-recommends libssl3 ca-certificates && apt-get clean

WORKDIR /app
COPY --from=builder /build/examples/replication_pipeline/_build/prod/rel/replication_pipeline ./

CMD ["/app/bin/replication_pipeline", "start"]

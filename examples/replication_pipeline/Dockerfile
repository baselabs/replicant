# The reference pipeline as one OTP release in one container.
# Build context is the REPO ROOT (the example path-depends on replicant):
#   docker compose build   (from examples/replication_pipeline)
#
# Builder and runtime use the same base so the release's ERTS always matches.
# The base tracks the repo toolchain (elixir 1.20.3 / OTP 29; the image's
# 29.0.5 patch is the closest published tag to .tool-versions' 29.0.3) AND is
# digest-pinned — hexpm/elixir date-suffixed tags have observed registry churn
# (policy says "never removed"; live probes show gaps), so the digest is the
# stable pin and the CI canary build is the daily pull-check.

FROM hexpm/elixir:1.20.3-erlang-29.0.5-debian-bookworm-20260803-slim@sha256:f2e721ffbb64a60cd56979c2d4e9651cf89c202c4a55466d979bf71b262802a3 AS builder

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
    && mix deps.get --only prod \
    && mix compile --warnings-as-errors

RUN mix release

FROM hexpm/elixir:1.20.3-erlang-29.0.5-debian-bookworm-20260803-slim@sha256:f2e721ffbb64a60cd56979c2d4e9651cf89c202c4a55466d979bf71b262802a3

RUN apt-get update -y && apt-get install -y --no-install-recommends libssl3 ca-certificates && apt-get clean

WORKDIR /app
COPY --from=builder /build/examples/replication_pipeline/_build/prod/rel/replication_pipeline ./

CMD ["/app/bin/replication_pipeline", "start"]

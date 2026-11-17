# Contributing to Replicant

Thank you for your interest in contributing to Replicant!

## Prerequisites

- **Elixir** 1.20.3 and **Erlang/OTP** 29 (the exact local/CI toolchain is in `.tool-versions`)
- **PostgreSQL 15, 16, 17, or 18** with `wal_level=logical` (for integration tests).
  Use the approved host ports PG15 `5615`, PG16 `5599`, PG17 `5617`, or PG18 `5618` — e.g.
  `docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p 5599:5432 postgres:16 \
    -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10`

## Getting Started

```bash
git clone https://github.com/baselabs/replicant.git
cd replicant
mix deps.get
mix test
```

There is no pre-commit hook. Every check runs in CI on your pull request (see
[CI](#ci) below); running them locally first is up to you.

## Development Workflow

1. Fork the repository and work on a short-lived branch.
2. Make your changes with clear, descriptive commit messages.
3. These are the checks CI runs on your pull request; running them first saves a round trip:

```bash
mix format                        # Format code
mix credo --strict                # Lint
mix compile --warnings-as-errors  # Zero warnings
mix test                          # Run tests
mix dialyzer                      # Type checking
mix audit                         # Dependency + Hex advisory audit
# or all quality gates at once:
mix quality
```

4. Update `CHANGELOG.md` under `[Unreleased]`.
5. Open a Pull Request against `main`.

### The reference example

[`examples/replication_pipeline`](examples/replication_pipeline/README.md) is a
separate nested Mix project (it path-depends on this library), so it has its
own gates — run them from its directory:

```bash
cd examples/replication_pipeline
mix format --check-formatted && mix compile --warnings-as-errors \
  && mix credo --strict && mix dialyzer
docker compose up -d --build       # the full stack (source → pipeline → dest)
```

CI runs exactly this as the `reference-example` job — the public sink API's
end-to-end canary — so a change to the sink surface that compiles clean here
can still red there. The repo-root `.dockerignore` is a whitelist (the
example's build context is the repo root); extend it only by adding a
specific `!path`.

## CI

Every check above, `mix test` on all six PostgreSQL majors, and the docs and
package build are jobs in
[`.github/workflows/qualification.yml`](.github/workflows/qualification.yml).
A pull request runs them through
[`pull-request.yml`](.github/workflows/pull-request.yml), with each check name
prefixed `pull request /` and a final `pull request checked`. A maintainer then
lands the change through a `kci/<commit>` branch, where
[`ci.yml`](.github/workflows/ci.yml) runs the same jobs once and `main` moves
only on its `qualified` check.

## Code Style

- Use `mix format` — `.formatter.exs` holds the config.
- Add `@moduledoc` and `@doc` to public modules and functions.
- Read [`docs/INVARIANTS.md`](docs/INVARIANTS.md) before changing decoder,
  assembler, identifier, or telemetry code — its Critical Rules (no row values
  in errors/logs/telemetry, validated
  identifiers, transaction-granularity exactly-once, TOAST-sentinel handling,
  tenant-blindness) are binding.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

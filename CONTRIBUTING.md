# Contributing to Replicant

Thank you for your interest in contributing to Replicant!

## Prerequisites

- **Elixir** 1.15+ and **Erlang/OTP** 26+
- **PostgreSQL 16** with `wal_level=logical` (for integration tests) — e.g.
  `docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres:16`

## Getting Started

```bash
git clone https://github.com/baselabs/replicant.git
cd replicant
mix deps.get
mix test
```

## Development Workflow

1. Work directly on `main` (or a short-lived branch if required by your
   workflow).
2. Make your changes with clear, descriptive commit messages.
3. Ensure all checks pass before opening a PR:

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

## Code Style

- Use `mix format` — `.formatter.exs` holds the config.
- Add `@moduledoc` and `@doc` to public modules and functions.
- Read `AGENTS.md` before changing decoder, assembler, identifier, or telemetry
  code — its Critical Rules (no row values in errors/logs/telemetry, validated
  identifiers, transaction-granularity exactly-once, TOAST-sentinel handling,
  tenant-blindness) are binding.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

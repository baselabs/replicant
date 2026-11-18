# Contributing to Replicant

Thank you for your interest in contributing to Replicant!

## Prerequisites

- **Elixir** and **Erlang/OTP** at the exact versions in
  [`.tool-versions`](https://github.com/baselabs/replicant/blob/main/.tool-versions).
- **PostgreSQL 15, 16, 17, or 18** with `wal_level=logical` (for integration tests).
  Use the approved host ports PG15 `5615`, PG16 `5599`, PG17 `5617`, or PG18 `5618` — e.g.
  `docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p 5599:5432 postgres:16 \
    -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10`

## Getting Started

```bash
git clone https://github.com/baselabs/replicant.git
cd replicant
mix deps.get --check-locked
mix test
```

There is no pre-commit hook. Checks run when a maintainer submits your change
through the `kci/` landing (see [CI](#ci) below); running them locally first is up to you.

## Development Workflow

1. Fork the repository and work on a short-lived branch.
2. Make your changes with clear, descriptive commit messages.
3. These are the checks CI runs on a maintainer's landing candidate:

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
own lockfile and gates. From the repository root, audit that lock with the
root project's locked audit tool, then run the example's own gates:

```bash
mix deps.get --check-locked
mix deps.audit --path examples/replication_pipeline
cd examples/replication_pipeline
mix deps.get --check-locked
mix hex.audit
mix format --check-formatted && mix compile --warnings-as-errors \
  && mix credo --strict && mix dialyzer
docker compose up -d --build       # the full stack (source → pipeline → dest)
```

CI runs the dependency audit in `static` and the remaining example gates in
the `reference-example` job — the public sink API's
end-to-end canary — so a change to the sink surface that compiles clean here
can still red there. The repo-root `.dockerignore` is a whitelist (the
example's build context is the repo root); extend it only by adding a
specific `!path`.

## CI

Infra's binding requirements live in
[`baselabs/docs/ci.md`, "What infra requires of every product"](https://github.com/baselabs/infra/blob/main/baselabs/docs/ci.md#what-infra-requires-of-every-product).
Read that section from infra's default branch before changing workflows or release scripts.

Every check above, `mix test` on all six PostgreSQL majors, and the docs and
package build are jobs in
[`.github/workflows/qualification.yml`](.github/workflows/qualification.yml).
No workflow runs repository code from a pull request. A fork's change is
qualified when a maintainer lands it through a `kci/<commit>` branch.
[`ci.yml`](.github/workflows/ci.yml) runs the checks once on that candidate,
and `main` moves only on its `qualified` check.

Qualification jobs do not cache dependencies, builds, or Dialyzer PLTs. Each candidate
has a new cache scope, and a cache written on `main` would be restorable by candidates.

Workflow timeouts follow infra's rule in the linked CI requirements. Each comment
names its slowest measured green run and duration; an unmeasured publish job uses
the unmeasured limit until its own first green run.
Every runtime installer reads `.tool-versions` with strict selection and immediately
refuses a differing Elixir version, full OTP patch version, or Elixir OTP build suffix.
The example Dockerfile explains its separately pinned container toolchain.

Release scripts read GitHub through `gh api`. The package-check step receives only
its read-only job token as `GH_TOKEN: ${{ github.token }}`, with job-level
`contents: read`; qualification reads no repository or environment secret. Release
absence requires the HTTP 404 status line from `gh api -i` (exit 1); HTTP 200 means
the release exists. Other statuses, transport errors, missing authentication, or
failed metadata reads refuse the check without including response values in errors.
Hex metadata still uses public unauthenticated reads.

## Publishing (release operator only)

Publishing runs in [`.github/workflows/publish.yml`](.github/workflows/publish.yml),
on GitHub-hosted Ubuntu 24.04 with the exact `.tool-versions` runtime. Qualification
and publishing pin the same Hex version because Hex records its version in package
metadata. No workstation publish command or credential-file loader remains.

After the version commit lands on `main`, infra asks an owner (`corepay` or
`palermo-git`) to dispatch **Publish to Hex** on `main` with no inputs. The default
branch ruleset guarantees qualification; publishing makes no run lookup or
artifact download. The owner must also be the triggering actor on a rerun.
Infra declares the GitHub `publish` environment and its `HEX_API_KEY` secret.
Only the archive upload step receives that secret.

An early anonymous Hex lookup refuses a package name that does not already exist:
a first publication of `replicant` is the owner's decision. All lookup errors fail
closed. The workflow checks out the dispatched SHA without persisted credentials.
Building requires a clean checkout of live `origin/main` and refuses an existing
version, Git tag, or GitHub release. Only the keyless build step receives the job
token through `GH_TOKEN` for GitHub reads; tokens never enter arguments or files.

Both qualification and publication build the package twice from `git archive`
and compare its bytes, inspect the exact package manifest and Hex checksum, then
audit and compile its extracted source using the committed producer lock.
The generated external consumer starts without a lock and resolves fresh, runs
both audits on that resolution, compiles, and exercises the public API.
Documentation builds with warnings as errors before any upload. Its archive is
inspected alongside the package archive.

The single credentialed step runs `mix run --no-mix-exs` with those two inspected
file paths. Its standalone script uses only Hex's APIs and file reads: it loads no
project code, fetches no dependencies, and rebuilds nothing. Immediately before
uploading, it rechecks that the package name exists and the version is absent.
It uploads the exact inspected files, then verifies Hex's package checksum.
Temporary archives are removed even after failure. After Hex read-back and
successful docs upload, infra creates the `v<version>` tag at that full source SHA
and the GitHub release:

```bash
gh release create vX.Y.Z --verify-tag --target <full sha>
```

Record the verified package digest in `scripts/release/published_packages.sha256`
through the normal landing process. All four release identities (tag, Hex package,
HexDocs, GitHub release) and the ledger must agree for subsequent package checks.
An interrupted or ambiguous upload requires the operator to inspect public state
before retrying; a partially published version is refused by a new publication attempt.

Agents never release, deploy, publish, tag, create a GitHub release, dispatch the
publish workflow, or handle publish credentials. Their work ends at the commit on
`main`.

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

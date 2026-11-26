# Contributing to Replicant

Thank you for your interest in contributing to Replicant!

## Prerequisites

- **Elixir** and **Erlang/OTP** at the exact versions in
  [`.tool-versions`](https://github.com/baselabs/replicant/blob/main/.tool-versions).
- **PostgreSQL** with `wal_level=logical` for integration tests, on any supported major:
  the platform engine named in [`.github/postgres.ref`](.github/postgres.ref) or a
  compatibility major listed in [`.github/postgres-lanes.json`](.github/postgres-lanes.json).
  Point `REPLICANT_TEST_URL` at it; `.github/actions/postgres` and `scripts/ci/postgres.sh`
  show exactly how CI starts each server, with password authentication.

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

The infrastructure maintainers' CI requirements bind every workflow and release script here;
maintainers read them before changing either.

Every check above, `mix test` on the declared PostgreSQL matrix, and the docs and
package build are jobs in
[`.github/workflows/qualification.yml`](.github/workflows/qualification.yml).
No workflow runs repository code from a pull request. A fork's change is
qualified when a maintainer lands it through a `kci/<commit>` branch.
[`ci.yml`](.github/workflows/ci.yml) runs the checks once on that candidate,
and `main` moves only on its `qualified` check.

Qualification jobs do not cache dependencies, builds, or Dialyzer PLTs. Each candidate
has a new cache scope, and a cache written on `main` would be restorable by candidates.

Workflow timeouts follow the infrastructure CI requirements. Each comment
names its slowest measured green run and duration; an unmeasured publish job uses
the unmeasured limit until its own first green run.
Every runtime installer takes versions only from the shared `scripts/toolchain.sh`
reader's outputs, selects them strictly, and uses its `--installed` check to refuse a
differing Elixir version, full OTP patch version, or Elixir OTP build suffix.
The example Dockerfile explains its separately pinned container toolchain.

## Publishing (release operator only)

The pinned shared Hex action builds the package and renders docs during qualification
using the selected commit's locked runtime and the action's pinned Hex. It verifies the
file set, source bytes, and requirements against the committed package declaration
and compiles a consumer of the unpacked package. The qualified package, docs and
manifest are retained for seven days. When cutting a release, move its entries from
`Unreleased` to the dated version section and use the package-scoped tag in the new
changelog comparison links.

Give infra the repository, full `release_commit`, version, release contents and
successful `qualified` run. Infra obtains the owner's approval and dispatches
[Publish to Hex](.github/workflows/publish.yml) from the default branch with that
40-character lowercase commit SHA. Both the original and triggering actors must
be `corepay` or `palermo-git`.

Only `upload` enters the `publish` environment. The shared action checks the
selected commit's qualification, the packaged changelog stamp, package and tag
state, and public/private version collisions before exchanging a package-scoped
token for any required upload. First publication belongs to the owner. If Hex
holds a release for approval, retry with the same `release_commit`. Upload
selects the newest successful qualification for that SHA, so keep the original
run and its archives available through approval.

`read-back` uses the qualification run and manifest digest from `upload`. The
shared action verifies the signed registry entry, byte equality for both
archives, and the docs' `release.json` naming the package, version and selected
commit. The workflow retains the two archives and their `SHA256SUMS` in a single
`release-assets` artifact. Infra compares the package against Hex, annotates
`baselabs_replicant-v<version>` at the published commit, and creates the GitHub
release with notes from the packaged changelog. Existing releases keep their
legacy `v<version>` tags.

Consumers verify the permanent [GitHub release](https://github.com/baselabs/replicant/releases)
by downloading the archives and checksums:

```bash
gh release download baselabs_replicant-vX.Y.Z --repo baselabs/replicant --pattern 'replicant-*' --pattern SHA256SUMS
sha256sum --check SHA256SUMS
curl --fail --silent --show-error https://repo.hex.pm/tarballs/replicant-X.Y.Z.tar -o hex.tar
cmp replicant-X.Y.Z.tar hex.tar
tar -xOf replicant-X.Y.Z-docs.tar.gz release.json
rm hex.tar
```

Check that `release.json` names `replicant`, the release version and the full
commit at the GitHub release tag. Record the verified package digest in
`scripts/release/published_packages.sha256` through the normal landing process
after the Hex package, HexDocs, annotated tag and GitHub release agree.

Infra owns the dispatch, release, tag and credentials. The repository session
delivers the qualified release commit on the default branch.

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

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

Every check above, `mix test` on all six PostgreSQL majors, and the docs and
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
and publishing read Hex from the selected source's `.tool-versions` because Hex
records its version in package metadata.

After qualification, give infra the repository, full `release_commit`, version,
release contents, and successful `qualified` run. Infra obtains the owner's
approval and dispatches **Publish to Hex** from the default branch with that
40-character lowercase SHA. Both the original and triggering actors must be
`corepay` or `palermo-git`. The first
job verifies default-branch ancestry, a successful GitHub Actions `qualified`
check on that exact SHA, the version in its `mix.exs`, and the absence of its
version tag. An anonymous Hex lookup requires the existing `replicant` package
name. First publication belongs to the owner.

The build job checks out the selected source with complete history and installs
its exact runtime. It uses the workflow revision for release tooling, so advancing
the default branch does not change the selected package source. Qualification and
publication build the package twice from `git archive` and compare the bytes,
check the package manifest and Hex checksum, and audit and compile its extracted
source using the committed producer lock. The generated external consumer
resolves fresh, runs both audits, compiles, and exercises the public API.
Documentation renders once with warnings as errors from the extracted package and
producer lock. The builder writes `release.json` with exactly `package`,
`version`, and `release_commit` as `commit` before inspecting the archive.

The shared changelog check requires exactly one root changelog in the inspected
package, entries under the release version, and empty sections above it. The
build job then plans the missing uploads. An untagged version already served by
Hex can resume when its metadata and served package bytes match the inspected
archive and the version is active. Served docs are kept based on validated
provenance. Docs uploads require an authoritative archive 404 and metadata that
does not report docs. Disagreement, malformed responses, and lookup failures
refuse publication. The archives, their checksums, and the plan travel together
to the upload job.

Only the upload job enters the `publish` environment. It verifies the downloaded
checksums and runs the standalone uploader with `mix run --no-mix-exs` in a
temporary directory without a Mix project. When an upload is planned, the script
exchanges GitHub workload identity for a short-lived `package:hexpm/replicant`
token and revokes it on exit. Runner cleanup retries revocation after failure
or cancellation. Package uploads disable replacement. Hex's docs POST can
replace an existing archive. The uploader checks served package bytes, release
metadata, and docs absence in succession before sending it. The workflow
serializes dispatches. Another Hex writer can still race the final absence
check and the POST.
If Hex holds either archive for approval, dispatch again with the same
`release_commit` after approval. A held package can finish its docs upload once
the package is publicly visible.

Keyless read-back verifies the signed Hex registry entry for the version and
outer checksum, checks the served package against that digest, and requires
readable served docs with `index.html` and valid
`release.json`. The docs commit must be `release_commit` or its ancestor in the
fetched default-branch history. A docs build can vary in search data ordering,
so the workflow preserves the served docs bytes and uses their provenance to
identify the published commit.

The workflow retains both served archives, `SHA256SUMS` computed over those bytes,
and `published_commit` in one `release-assets` artifact. It also exposes
`published_commit` as a read-back output. Infra uses `bin/release-tag` to compare
the package against Hex, annotate `v<version>` at the served docs commit, and
create the GitHub release with notes from the packaged changelog.

Consumers verify the permanent [GitHub release](https://github.com/baselabs/replicant/releases)
for `v<version>`, downloading its archives and `SHA256SUMS`:

```bash
gh release download vX.Y.Z --repo baselabs/replicant --pattern 'replicant-*' --pattern SHA256SUMS --pattern published_commit
sha256sum --check SHA256SUMS
curl --fail --silent --show-error https://repo.hex.pm/tarballs/replicant-X.Y.Z.tar -o hex.tar
cmp replicant-X.Y.Z.tar hex.tar
tar -xOf replicant-X.Y.Z-docs.tar.gz release.json
cat published_commit
rm hex.tar
```

Check that `release.json` names `replicant`, this version, and the same full commit
as `published_commit` and the release tag. No rebuilt docs digest is involved.

Record the verified package digest in `scripts/release/published_packages.sha256`
through the normal landing process. All four release identities (tag, Hex package,
HexDocs, GitHub release) and the ledger must agree for subsequent package checks.
Held versions never add or alter manifest entries speculatively. An interrupted
or ambiguous upload requires infra to inspect public state before the owner
retries the same commit. A tagged version or existing GitHub release always refuses.

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

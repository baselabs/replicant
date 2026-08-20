#!/usr/bin/env bash
# Package-boundary gate for the Replicant Hex package.
#
# WHY: `mix.exs` `files:` is a glob allowlist (`lib`, `README*`, `docs/adr`, ...). Any
# ignored/untracked file that matches a glob silently enters the tarball — a synthetic
# `README.secret`, `lib/.env`, and `docs/adr/private-note.md` all packaged while the old CI
# grep gate still exited 0. This gate closes that hole: it compares the EXACT set of regular
# files in the built/retained tarball against a checked-in manifest and rejects every missing
# path, every extra path, and every symlink or special entry. It also asserts the package
# metadata name/version.
#
# Text greps are not an acceptance gate: this operates on the real unpacked tar bytes.
#
# Usage:
#   verify_package.sh [TARBALL]
#     TARBALL omitted -> build a fresh temporary tarball from the working tree (pre-mint check).
#     TARBALL given   -> verify that exact retained artifact (post-mint / R07 check).
#
# Env:
#   EXPECTED_VERSION  override the expected package version (default: mix.exs @version).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
manifest="$repo_root/scripts/release/package_files.manifest"

if [[ ! -f "$manifest" ]]; then
  echo "::error::package manifest missing: $manifest" >&2
  exit 1
fi

expected_name="replicant"
expected_version="${EXPECTED_VERSION:-}"
if [[ -z "$expected_version" ]]; then
  expected_version="$(grep -oE '@version "[^"]+"' "$repo_root/mix.exs" | head -1 | sed -E 's/@version "([^"]+)"/\1/')"
fi
if [[ -z "$expected_version" ]]; then
  echo "::error::could not determine expected version" >&2
  exit 1
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

tarball="${1:-}"
if [[ -z "$tarball" ]]; then
  # Build a fresh tarball from the working tree into an isolated build path so this can run
  # standalone or from inside another mix invocation without contending on _build locks.
  echo "verify_package: building a fresh tarball from the working tree" >&2
  ( cd "$repo_root" && MIX_ENV=dev MIX_BUILD_PATH="$work/_build" mix hex.build --output "$work/pkg.tar" >/dev/null )
  tarball="$work/pkg.tar"
fi

if [[ ! -f "$tarball" ]]; then
  echo "::error::tarball not found: $tarball" >&2
  exit 1
fi

# --- 1. Outer members are exactly the Hex envelope. -------------------------------------
members="$(tar -tf "$tarball" | LC_ALL=C sort | tr '\n' ' ')"
expected_members="CHECKSUM VERSION contents.tar.gz metadata.config "
if [[ "$members" != "$expected_members" ]]; then
  echo "::error::unexpected outer tar members: [$members] expected [$expected_members]" >&2
  exit 1
fi

# --- 2. Metadata name/version. -----------------------------------------------------------
tar -xOf "$tarball" metadata.config > "$work/metadata.config"
got_name="$(grep -aoE '\{<<"name">>,<<"[^"]+">>\}' "$work/metadata.config" | head -1 | sed -E 's/.*<<"([^"]+)">>\}/\1/')"
got_version="$(grep -aoE '\{<<"version">>,<<"[^"]+">>\}' "$work/metadata.config" | head -1 | sed -E 's/.*<<"([^"]+)">>\}/\1/')"
if [[ "$got_name" != "$expected_name" ]]; then
  echo "::error::package name is '$got_name', expected '$expected_name'" >&2
  exit 1
fi
if [[ "$got_version" != "$expected_version" ]]; then
  echo "::error::package version is '$got_version', expected '$expected_version'" >&2
  exit 1
fi

# --- 3. Extract contents and reject symlinks / special entries. --------------------------
contents="$work/contents"
mkdir -p "$contents"
tar -xOf "$tarball" contents.tar.gz | tar -xzf - -C "$contents"

if find "$contents" -type l | grep -q .; then
  echo "::error::package contains a symlink:" >&2
  find "$contents" -type l | sed 's|^|  |' >&2
  exit 1
fi
if find "$contents" ! -type f ! -type d | grep -q .; then
  echo "::error::package contains a special (non-regular, non-directory) entry:" >&2
  find "$contents" ! -type f ! -type d | sed 's|^|  |' >&2
  exit 1
fi

# --- 4. Exact regular-file set == manifest. ----------------------------------------------
actual="$work/actual.manifest"
( cd "$contents" && find . -type f | sed 's|^\./||' | LC_ALL=C sort ) > "$actual"

expected_sorted="$work/expected.manifest"
LC_ALL=C sort "$manifest" > "$expected_sorted"

extra="$(LC_ALL=C comm -13 "$expected_sorted" "$actual" || true)"
missing="$(LC_ALL=C comm -23 "$expected_sorted" "$actual" || true)"

status=0
if [[ -n "$extra" ]]; then
  echo "::error::package contains files NOT in the manifest (boundary leak):" >&2
  printf '  + %s\n' "${extra//$'\n'/$'\n  + '}" >&2
  status=1
fi
if [[ -n "$missing" ]]; then
  echo "::error::package is MISSING files listed in the manifest:" >&2
  printf '  - %s\n' "${missing//$'\n'/$'\n  - '}" >&2
  status=1
fi
if [[ $status -ne 0 ]]; then
  exit 1
fi

count="$(wc -l < "$actual" | tr -d ' ')"
echo "verify_package: OK — $expected_name $got_version, $count regular files match the manifest, no symlink/special entries"

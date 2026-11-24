#!/usr/bin/env bash
# Build and inspect both release archives; workflows check release identity separately.
# Check mode removes all artifacts; publication retains only the inspected paths.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

usage() {
  echo "usage: build_candidate.sh --check | <output-directory>" >&2
  exit 2
}

[[ $# -eq 1 && -n "$1" ]] || usage
[[ "$1" == "--check" || "$1" != -* ]] || usage
output_dir="$1"

log() { echo "build_candidate: $*" >&2; }
die() { echo "::error::build_candidate: $*" >&2; exit 1; }

tree_status="$(git status --porcelain)"
[[ -z "$tree_status" ]] || die "working tree is dirty"
commit="$(git rev-parse HEAD)"
version="$(grep -oE '@version "[^"]+"' mix.exs | head -1 | sed -E 's/@version "([^"]+)"/\1/')"
[[ -n "$version" ]] || die "could not read package version"

build_tree="$(mktemp -d "${TMPDIR:-/tmp}/replicant-package.XXXXXX")"
output_owned=0
complete=0
cleanup() {
  status=$?
  trap - EXIT
  if [[ $output_owned -eq 1 && $complete -eq 0 ]]; then
    rm -rf -- "$output_dir"
  fi
  rm -rf -- "$build_tree"
  exit "$status"
}
trap cleanup EXIT

if [[ "$output_dir" == "--check" ]]; then
  output_dir="$build_tree/artifacts"
fi
mkdir "$output_dir"
output_owned=1
output_dir="$(cd "$output_dir" && pwd -P)"

source_archive="$build_tree/source.tar"
git archive --format=tar --output="$source_archive" "$commit"
tar -xf "$source_archive" -C "$build_tree"
package="$output_dir/package.tar"
( cd "$build_tree" && MIX_ENV=dev MIX_BUILD_PATH="$build_tree/_build" mix hex.build --output "$package" >/dev/null )
[[ -f "$package" ]] || die "mix hex.build produced no tarball"

rebuild_tree="$build_tree/reproducibility"
mkdir "$rebuild_tree"
tar -xf "$source_archive" -C "$rebuild_tree"
( cd "$rebuild_tree" && MIX_ENV=dev MIX_BUILD_PATH="$rebuild_tree/_build" mix hex.build --output "$rebuild_tree/package.tar" >/dev/null )
cmp -s "$package" "$rebuild_tree/package.tar" || die "package build is not byte-reproducible"

EXPECTED_VERSION="$version" bash "$repo_root/scripts/release/verify_package.sh" "$package" >&2
bash "$repo_root/scripts/release/consume_candidate.sh" "$package" "$output_dir/docs.tar.gz" >&2
# The upload step receives these same inspected file paths, with no intervening rebuild.
chmod a-w "$package" "$output_dir/docs.tar.gz"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  printf 'version=%s\ncommit=%s\n' "$version" "$commit" >> "$GITHUB_OUTPUT"
fi
complete=1
log "PASS - package built twice identically, inspected, audited, consumed; docs built once and inspected"

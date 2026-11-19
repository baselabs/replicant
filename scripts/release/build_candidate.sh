#!/usr/bin/env bash
# Build and inspect both release archives without a registry credential.
# Check mode removes all artifacts; publication retains only the inspected paths.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

usage() {
  echo "usage: build_candidate.sh --check | <output-directory>" >&2
  exit 2
}

[[ $# -eq 1 && -n "$1" ]] || usage
mode="publish"
output_dir="$1"
if [[ "$1" == "--check" ]]; then
  mode="check"
elif [[ "$1" == -* ]]; then
  usage
fi

log() { echo "build_candidate: $*" >&2; }
die() { echo "::error::build_candidate: $*" >&2; exit 1; }

tree_status="$(git status --porcelain)"
[[ -z "$tree_status" ]] || die "working tree is dirty"
commit="$(git rev-parse HEAD)"
version="$(grep -oE '@version "[^"]+"' mix.exs | head -1 | sed -E 's/@version "([^"]+)"/\1/')"
[[ -n "$version" ]] || die "could not read package version"
published_manifest="$repo_root/scripts/release/published_packages.sha256"
[[ -r "$published_manifest" ]] || die "published package digest manifest is unavailable"
published_digest="$(awk -v artifact="replicant-$version.tar" '$2 == artifact {print $1}' "$published_manifest")"
if [[ -n "$published_digest" && ! "$published_digest" =~ ^[0-9a-f]{64}$ ]]; then
  die "published package digest manifest has an invalid or duplicate entry for $version"
fi

if [[ "$mode" == "publish" ]]; then
  elixir -r "$repo_root/scripts/release/package_identity.exs" \
    -e 'Replicant.PackageIdentity.verify_publish!(Enum.at(System.argv(), 0), Enum.at(System.argv(), 1))' -- \
    "$version" "$commit"
else
  elixir -r "$repo_root/scripts/release/package_identity.exs" \
    -e 'Replicant.PackageIdentity.verify_build!(Enum.at(System.argv(), 0), Enum.at(System.argv(), 1), Enum.at(System.argv(), 2))' -- \
    "$version" "$commit" "$published_digest"
fi

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

if [[ "$mode" == "check" ]]; then
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
# Each consumer extracts into its own fresh tree and resolves the committed lock.
bash "$repo_root/scripts/release/consume_candidate.sh" "$rebuild_tree/package.tar" "$rebuild_tree/docs.tar.gz" >&2
cmp -s "$output_dir/docs.tar.gz" "$rebuild_tree/docs.tar.gz" || die "docs build is not byte-reproducible"
if [[ "$mode" == "publish" ]]; then
  # A held version is resumable only with these exact bytes, never with a rebuild
  # that differs from the registry. Repeat tag/release refusals after the build.
  elixir -r "$repo_root/scripts/release/package_identity.exs" \
    -e '
      [version, commit, archive, docs] = System.argv()
      Replicant.PackageIdentity.verify_publish_archive!(version, commit, archive, docs)
    ' -- "$version" "$commit" "$package" "$output_dir/docs.tar.gz"
fi
# The upload step receives these same inspected file paths, with no intervening rebuild.
chmod a-w "$package" "$output_dir/docs.tar.gz"
complete=1
log "PASS - package built twice identically, inspected, audited, consumed; docs built twice identically and inspected"

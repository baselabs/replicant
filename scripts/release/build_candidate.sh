#!/usr/bin/env bash
# Build and prove package bytes from an exact commit. `--check` uses only throwaway bytes;
# the default mint mode is allowed only on clean main equal to live origin/main.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

mode="mint"
if [[ "${1:-}" == "--check" ]]; then
  mode="check"
  shift
fi
[[ $# -eq 0 ]] || { echo "usage: build_candidate.sh [--check]" >&2; exit 2; }

log() { echo "build_candidate: $*" >&2; }
die() { echo "::error::build_candidate: $*" >&2; exit 1; }

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}';
  else shasum -a 256 "$1" | awk '{print $1}'; fi
}

[[ -z "$(git status --porcelain)" ]] || die "working tree is dirty"
commit="$(git rev-parse HEAD)"
version="$(grep -oE '@version "[^"]+"' mix.exs | head -1 | sed -E 's/@version "([^"]+)"/\1/')"
[[ -n "$version" ]] || die "could not read package version"

if [[ "$mode" == "mint" ]]; then
  [[ "$(git branch --show-current)" == "main" ]] || die "candidate mint is allowed only on main"

  if remote_main="$(git ls-remote --exit-code origin refs/heads/main 2>/dev/null)"; then
    remote_main="$(printf '%s' "$remote_main" | awk '{print $1}')"
    [[ "$remote_main" == "$commit" ]] || die "HEAD $commit does not equal live origin/main $remote_main"
  else
    die "could not prove live origin/main identity"
  fi
fi

elixir -r "$repo_root/scripts/release/package_identity.exs" \
  -e 'Replicant.PackageIdentity.verify_candidate!(hd(System.argv()))' -- "$version"

build_tree="$(mktemp -d "${TMPDIR:-/tmp}/replicant-package.XXXXXX")"
witness_ref=""
witness_oid=""
primary=""
by_digest=""
backup=""
receipt=""
artifacts_retained=0
receipt_retained=0
witness_owned=0
mint_complete=0
cleanup() {
  status=$?
  trap - EXIT

  if [[ $witness_owned -eq 1 && ( "$mode" == "check" || $mint_complete -eq 0 ) ]]; then
    git update-ref -d "$witness_ref" "$witness_oid" >/dev/null 2>&1 || true
  fi

  if [[ "$mode" == "mint" && $mint_complete -eq 0 ]]; then
    if [[ $receipt_retained -eq 1 ]]; then
      chmod u+w "$receipt" >/dev/null 2>&1 || true
      rm -f -- "$receipt"
    fi

    if [[ $artifacts_retained -eq 1 ]]; then
      for target in "$primary" "$by_digest" "$backup"; do
        chmod u+w "$target" >/dev/null 2>&1 || true
        rm -f -- "$target"
      done
    fi
  fi

  rm -rf "$build_tree"
  exit "$status"
}
trap cleanup EXIT

source_archive="$build_tree/source.tar"
git archive --format=tar --output="$source_archive" "$commit"
tar -xf "$source_archive" -C "$build_tree"
staged_tar="$build_tree/replicant-$version.tar"
( cd "$build_tree" && MIX_ENV=dev MIX_BUILD_PATH="$build_tree/_build" mix hex.build --output "$staged_tar" >/dev/null )
[[ -f "$staged_tar" ]] || die "mix hex.build produced no tarball"

digest="$(sha256_of "$staged_tar")"
size="$(wc -c < "$staged_tar" | tr -d ' ')"
lock_digest="$(sha256_of "$build_tree/mix.lock")"
verification="$build_tree/verification.txt"

EXPECTED_VERSION="$version" bash "$repo_root/scripts/release/verify_package.sh" "$staged_tar" >&2
bash "$repo_root/scripts/release/consume_candidate.sh" "$staged_tar" "$verification" >&2

if [[ "$mode" == "check" ]]; then
  artifacts_dir="$build_tree/artifacts"
  witness_ref="refs/attestations/checks/replicant/$version-$commit"
else
  artifacts_dir="$repo_root/.kimosabe/artifacts"
  witness_ref="refs/attestations/packages/replicant/$version"
fi

primary="$artifacts_dir/replicant-$version.tar"
by_digest="$artifacts_dir/by-digest/$digest.tar"
backup="$artifacts_dir/backups/replicant-$version.tar"
receipt="$artifacts_dir/replicant-$version-receipt.txt"

for target in "$primary" "$by_digest" "$backup" "$receipt"; do
  [[ ! -e "$target" ]] || die "refusing to overwrite existing package evidence: $target"
done
if git show-ref --verify --quiet "$witness_ref"; then
  die "package witness already exists: $witness_ref"
fi

receipt_tmp="$build_tree/receipt.txt"
{
  echo "Replicant package candidate receipt"
  echo "version: $version"
  echo "source_commit: $commit"
  echo "artifact: $primary"
  echo "artifact_backup: $backup"
  echo "artifact_by_digest: $by_digest"
  echo "size_bytes: $size"
  echo "sha256: $digest"
  echo "source_mix_lock_sha256: $lock_digest"
  echo "elixir: $(elixir --version 2>/dev/null | tail -1)"
  echo "built_at: ${BUILD_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
  echo "published: NO"
  echo ""
  cat "$verification"
} > "$receipt_tmp"

elixir -r "$repo_root/scripts/release/package_witness.exs" \
  -e 'Replicant.PackageWitness.retain_copies!(hd(System.argv()), tl(System.argv()))' -- \
  "$staged_tar" "$primary" "$by_digest" "$backup"
artifacts_retained=1

elixir -r "$repo_root/scripts/release/package_witness.exs" \
  -e 'Replicant.PackageWitness.retain_copies!(hd(System.argv()), tl(System.argv()))' -- \
  "$receipt_tmp" "$receipt"
receipt_retained=1

elixir -r "$repo_root/scripts/release/package_witness.exs" \
  -e 'Replicant.PackageWitness.verify_copies!(tl(System.argv()), hd(System.argv()))' -- \
  "$digest" "$primary" "$by_digest" "$backup"

witness_oid="$(elixir -r "$repo_root/scripts/release/package_witness.exs" \
  -e 'IO.puts(Replicant.PackageWitness.create!(Enum.at(System.argv(), 0), Enum.at(System.argv(), 1), Enum.at(System.argv(), 2), Enum.at(System.argv(), 3)))' -- \
  "$repo_root" "$witness_ref" "$commit" "$receipt"
)"
witness_owned=1

mix run --no-start "$repo_root/scripts/release/upload_candidate.exs" \
  --artifact "$primary" --receipt "$receipt" --witness-ref "$witness_ref"

mint_complete=1

if [[ "$mode" == "check" ]]; then
  log "CHECK PASS — throwaway package built, audited, documented, consumed, and uploader dry-run verified"
else
  log "MINT PASS — immutable candidate retained and witnessed at $witness_ref"
fi
echo "$digest  $primary"

#!/usr/bin/env bash
# Build exactly ONE immutable Replicant release candidate from a clean, committed source tree
# and retain it — never publish, tag, or read a Hex credential (that is R07, human-authorized).
#
# The candidate is built from `git archive HEAD` into a throwaway tree, NOT from the working
# checkout: `mix hex.build` expands the configured `files:` globs from `File.cwd!/0`, so an
# ignored or untracked matching file (a `README.secret`, a `lib/.env`) would contaminate a
# checkout build. `git archive` contains only committed bytes, so the tarball's contents are
# exactly the reviewed commit.
#
# The result is content-addressed, made read-only, copied to a durable backup, and described in
# a receipt recording the exact source commit, mix.lock digest, size, and SHA-256 — the identity
# R07 verifies before uploading these exact bytes.
#
# Env:
#   BUILD_DATE   ISO date stamped into the receipt (default: `date -u`); pass for reproducibility.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

artifacts_dir="$repo_root/.kimosabe/artifacts"
mkdir -p "$artifacts_dir/by-digest"

log() { echo "build_candidate: $*" >&2; }
die() { echo "::error::build_candidate: $*" >&2; exit 1; }

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}';
  else shasum -a 256 "$1" | awk '{print $1}'; fi
}

# --- 0. Source must be clean and committed (settle source before candidate bytes). ----------
[[ -z "$(git status --porcelain)" ]] || die "working tree is dirty; commit the final source before minting the candidate"

commit="$(git rev-parse HEAD)"
version="$(grep -oE '@version "[^"]+"' mix.exs | head -1 | sed -E 's/@version "([^"]+)"/\1/')"
[[ -n "$version" ]] || die "could not read @version from mix.exs"
tag="v$version"
log "candidate replicant $version from commit $commit"

# --- 1. Identity collisions fail closed. -----------------------------------------------------
# Local tag: a hard check (always available). Remote tag / GitHub release / Hex: best-effort —
# a DETECTED collision aborts; an unavailable check is logged, never a silent fail-open. The
# authoritative pre-upload identity gate is re-run in R07 before any bytes leave.
if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
  die "local tag $tag already exists — $version identity is taken"
fi
if remote_tag="$(git ls-remote --tags origin "refs/tags/$tag" 2>/dev/null)"; then
  [[ -z "$remote_tag" ]] || die "remote tag $tag already exists on origin — $version identity is taken"
else
  log "WARN: could not reach origin to check remote tag $tag (offline?); R07 will re-check"
fi
if command -v gh >/dev/null 2>&1; then
  if gh release view "$tag" >/dev/null 2>&1; then
    die "GitHub release $tag already exists — $version identity is taken"
  else
    log "GitHub release $tag not found (expected)"
  fi
else
  log "WARN: gh CLI absent; skipped GitHub release check for $tag"
fi
if hex_out="$(mix hex.info replicant "$version" 2>/dev/null)"; then
  if echo "$hex_out" | grep -qiE 'Released|Config:'; then
    die "Hex already has replicant $version — refusing to re-mint a published version"
  fi
else
  log "Hex reports no replicant $version (expected) or is unreachable"
fi

# --- 2. Build ONCE from the archived commit into a throwaway tree. ---------------------------
build_tree="$(mktemp -d "${TMPDIR:-/tmp}/replicant-candidate.XXXXXX")"
cleanup() { rm -rf "$build_tree"; }
trap cleanup EXIT

git archive "$commit" | tar -x -C "$build_tree"
staged_tar="$build_tree/replicant-$version.tar"
( cd "$build_tree" && MIX_ENV=dev MIX_BUILD_PATH="$build_tree/_build" mix hex.build --output "$staged_tar" >/dev/null )
[[ -f "$staged_tar" ]] || die "hex.build produced no tarball"

digest="$(sha256_of "$staged_tar")"
size="$(wc -c < "$staged_tar" | tr -d ' ')"
lock_digest="$(sha256_of "$repo_root/mix.lock")"
log "built replicant-$version.tar  size=$size  sha256=$digest"

# --- 3. Verify the package boundary of the freshly minted bytes BEFORE retaining. ------------
EXPECTED_VERSION="$version" bash "$repo_root/scripts/release/verify_package.sh" "$staged_tar" >&2

# --- 4. Content-address, retain read-only, no-clobber, plus a durable backup. ----------------
primary="$artifacts_dir/replicant-$version.tar"
by_digest="$artifacts_dir/by-digest/$digest.tar"
backup="$artifacts_dir/replicant-$version.backup.tar"

if [[ -f "$primary" ]]; then
  existing="$(sha256_of "$primary")"
  if [[ "$existing" == "$digest" ]]; then
    log "candidate already retained with identical digest — idempotent re-run, keeping existing bytes"
  else
    die "a DIFFERENT replicant-$version.tar is already retained (digest $existing != $digest); a rebuilt tarball is NOT the same artifact — resolve the source drift"
  fi
else
  install -m 0444 "$staged_tar" "$primary"
fi
[[ -f "$by_digest" ]] || install -m 0444 "$staged_tar" "$by_digest"
[[ -f "$backup" ]] || install -m 0444 "$staged_tar" "$backup"
chmod 0444 "$primary" "$by_digest" "$backup"

# --- 5. Durable receipt (gitignored). --------------------------------------------------------
build_date="${BUILD_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
receipt="$artifacts_dir/replicant-$version-receipt.txt"
# The receipt is retained read-only (0444); an idempotent re-run must be able to regenerate it,
# so drop any prior copy before writing rather than `cat >`-ing onto a read-only file.
rm -f "$receipt"
cat > "$receipt" <<EOF
Replicant release candidate — R06 receipt
==========================================
version:          $version
source_commit:    $commit
tag_to_create:    $tag   (R07 only; requires explicit human authorization)
artifact:         $primary
artifact_backup:  $backup
artifact_bydigest:$by_digest
size_bytes:       $size
sha256:           $digest
mix_lock_sha256:  $lock_digest
elixir:           $(elixir --version 2>/dev/null | tail -1)
built_at:         $build_date
published:        NO — R06 never publishes/tags/creates a GitHub release/reads a credential.
EOF
chmod 0444 "$receipt"

log "receipt written: $receipt"
echo "$digest  $primary"

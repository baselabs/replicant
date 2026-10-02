#!/usr/bin/env bash
# Publish the docs for the default witnessed candidate, built from the witnessed
# source commit (never the working tree). Same authorization gate as the package
# publish: exact version:digest of the witnessed artifact, checked before the
# credential file is read. The package's check_build REQUIRES has_docs — a
# release without hexdocs is an incomplete release by this repo's own contract.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

die() { echo "::error::publish_docs: $*" >&2; exit 1; }

[[ $# -eq 0 ]] || die "this wrapper accepts no arguments"

version="$(grep -oE '@version "[^"]+"' mix.exs | head -1 | sed -E 's/@version "([^"]+)"/\1/')"
receipt=".kimosabe/artifacts/replicant-$version-receipt.txt"
[[ -r "$receipt" ]] || die "candidate receipt unavailable"

digest="$(sed -n 's/^sha256: //p' "$receipt")"
[[ "$digest" =~ ^[0-9a-f]{64}$ ]] || die "candidate receipt has no valid digest"
expected="$version:$digest"

[[ "${REPLICANT_PUBLISH_AUTHORIZED:-}" == "$expected" ]] || \
  die "publish requires exact version:digest authorization for the witnessed artifact"

env_file="$repo_root/.env"
source "$repo_root/scripts/release/credential_loader.sh"
replicant_load_hex_api_key "$env_file"

exec mix run --no-start "$repo_root/scripts/release/upload_docs.exs" --publish

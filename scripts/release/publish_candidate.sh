#!/usr/bin/env bash
# Publish the default witnessed candidate with the single HEX_API_KEY assignment from
# the project .env. Authorization is checked before the credential file is read.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

die() { echo "::error::publish_candidate: $*" >&2; exit 1; }

[[ $# -eq 0 ]] || die "this wrapper accepts no path or witness overrides"

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

exec mix run --no-start scripts/release/upload_candidate.exs --publish

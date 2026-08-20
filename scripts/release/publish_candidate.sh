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
[[ -r "$env_file" ]] || die "project credential file unavailable"

# Accept one non-empty, unquoted token assignment and nothing executable. Never print it.
awk '
  /^[[:space:]]*$/ { next }
  /^HEX_API_KEY=[A-Za-z0-9_-]+$/ { seen++; next }
  { bad++ }
  END { exit !(seen == 1 && bad == 0) }
' "$env_file" || die "project credential file must contain only one plain HEX_API_KEY assignment"

while IFS= read -r credential_line; do
  case "$credential_line" in
    HEX_API_KEY=*) HEX_API_KEY="${credential_line#HEX_API_KEY=}" ;;
  esac
done < "$env_file"
export HEX_API_KEY

exec mix run --no-start scripts/release/upload_candidate.exs --publish

#!/usr/bin/env bash
# Prove the Replicant archive from a FRESH, external consumer that touches only the
# extracted package bytes — not the repository checkout, not the repo's _build.
#
# Steps: Hex-checksum-validate + unpack the exact retained tar -> build a brand-new Mix consumer
# whose only Replicant source is the extraction -> clear MIX_PATH/ERL_LIBS -> fresh resolution -> compile
# with warnings-as-errors -> run a semantic R01-R05 smoke that asserts the loaded Replicant BEAM
# resolves under the scratch tree. A missing/corrupt packaged file is invisible from the checkout;
# it is caught here.
#
# Usage: consume_candidate.sh TARBALL [DOCS_ARCHIVE] [EXPECTED_SHA256]
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
log() { echo "consume_candidate: $*" >&2; }
die() { echo "::error::consume_candidate: $*" >&2; exit 1; }

version="$(grep -oE '@version "[^"]+"' "$repo_root/mix.exs" | head -1 | sed -E 's/@version "([^"]+)"/\1/')"
[[ $# -ge 1 && $# -le 3 ]] || die "usage: consume_candidate.sh TARBALL [DOCS_ARCHIVE] [EXPECTED_SHA256]"
tarball="$1"
docs_archive="${2:-}"
expected_digest="${3:-}"
[[ -f "$tarball" ]] || die "candidate tarball not found: $tarball (build it first with build_candidate.sh)"

scratch="$(mktemp -d "${TMPDIR:-/tmp}/replicant-consume.XXXXXX")"
scratch="$(cd "$scratch" && pwd -P)"
cleanup() { rm -rf "$scratch"; }
trap cleanup EXIT
src="$scratch/replicant_src"
consumer="$scratch/consumer"

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}';
  else shasum -a 256 "$1" | awk '{print $1}'; fi
}

if [[ -n "$expected_digest" ]]; then
  [[ "$expected_digest" =~ ^[0-9a-f]{64}$ ]] || die "expected digest must be 64 lowercase hex characters"
  actual_digest="$(sha256_of "$tarball")"
  [[ "$actual_digest" == "$expected_digest" ]] || \
    die "candidate digest mismatch: expected $expected_digest, got $actual_digest"
fi

# --- 1. Hex-checksum-validated unpack of the EXACT retained tar (no rebuild). ----------------
( cd "$repo_root" && mix run --no-start scripts/release/unpack_validated.exs "$tarball" "$src" ) >&2
[[ -f "$src/mix.exs" ]] || die "extraction produced no mix.exs at $src"

# --- 2. Audit, compile, and render docs from the exact extracted package source. ---------------
log "auditing and building the extracted package source"
# Reproduce the producer build with its committed lock. The separate consumer
# below deliberately starts without a lock to prove a fresh downstream install.
cp "$repo_root/mix.lock" "$src/mix.lock"
(
  cd "$src"
  env -u MIX_PATH -u ERL_LIBS -u REPLICANT_TEST_URL MIX_ENV=dev mix deps.get --check-locked >&2
  env -u MIX_PATH -u ERL_LIBS -u REPLICANT_TEST_URL MIX_ENV=dev mix audit >&2
  env -u MIX_PATH -u ERL_LIBS -u REPLICANT_TEST_URL MIX_ENV=dev mix compile --warnings-as-errors >&2
  env -u MIX_PATH -u ERL_LIBS -u REPLICANT_TEST_URL MIX_ENV=dev mix docs --warnings-as-errors >&2
)

# Archive and inspect the rendered docs before the credentialed step can run.
if [[ -n "$docs_archive" ]]; then
  [[ -f "$src/doc/index.html" ]] || die "docs have no index.html"
  [[ -z "$(find "$src/doc" ! -type f ! -type d -print -quit)" ]] || die "docs contain a link or special entry"
  tar -czf "$docs_archive" -C "$src/doc" .
  python3 - "$docs_archive" "$src/doc" <<'PYTHON'
import sys
import tarfile
from pathlib import Path, PurePosixPath

root = Path(sys.argv[2])
expected = {path.relative_to(root).as_posix(): path for path in root.rglob('*') if path.is_file()}
seen = set()
with tarfile.open(sys.argv[1], 'r:gz') as archive:
    for member in archive:
        path = PurePosixPath(member.name)
        if path.is_absolute() or '..' in path.parts or not (member.isfile() or member.isdir()):
            raise SystemExit('docs archive contains an unsafe entry')
        if member.isdir():
            continue
        name = path.as_posix()
        if name not in expected or name in seen:
            raise SystemExit('docs archive has an unexpected or duplicate file')
        if archive.extractfile(member).read() != expected[name].read_bytes():
            raise SystemExit('docs archive bytes differ from the rendered documentation')
        seen.add(name)
if seen != expected.keys() or 'index.html' not in seen:
    raise SystemExit('docs archive is missing rendered documentation')
print('consume_candidate: docs archive matches every rendered file')
PYTHON
fi

# --- 3. A brand-new consumer that depends ONLY on the extracted source. ----------------------
mkdir -p "$consumer/lib"
cat > "$consumer/mix.exs" <<EOF
defmodule ReplicantConsumerSmoke.MixProject do
  use Mix.Project

  def project do
    [app: :replicant_consumer_smoke, version: "0.0.0", elixir: "~> 1.20", deps: deps()]
  end

  def application, do: [extra_applications: [:logger]]

  defp deps do
    [{:replicant, path: "$src"}]
  end
end
EOF

cat > "$consumer/lib/replicant_consumer_smoke.ex" <<'EOF'
defmodule ReplicantConsumerSmoke do
  @moduledoc "Compile-only proof that the extracted Replicant package's public API is usable."
  def slot_query(v), do: Replicant.QueryBuilder.slot_invalidation_status("s", v)
end
EOF

# --- 4. Fresh resolution + compile from a clean environment (no repo _build, no MIX_PATH). -------
log "building fresh consumer against extracted source $src"
[[ ! -e "$consumer/mix.lock" ]] || die "fresh consumer must start without a lock"
(
  cd "$consumer"
  env -u MIX_PATH -u ERL_LIBS -u REPLICANT_TEST_URL MIX_ENV=dev \
    mix deps.get >&2
  env -u MIX_PATH -u ERL_LIBS -u REPLICANT_TEST_URL MIX_ENV=dev \
    mix hex.audit >&2
  # The consumer has no audit-tool dependency; use the root's locked tool on its lockfile.
  ( cd "$repo_root" && MIX_ENV=dev mix deps.audit --path "$consumer" ) >&2
  env -u MIX_PATH -u ERL_LIBS -u REPLICANT_TEST_URL MIX_ENV=dev \
    mix compile --warnings-as-errors >&2

  # Provenance: the resolved dependency source must be the scratch extraction, never the repo.
  dep_path="$(env -u MIX_PATH mix run --no-start -e 'IO.puts(Path.expand(Mix.Project.deps_paths()[:replicant]))')"
  case "$dep_path" in
    "$scratch"/*) : ;;
    *) echo "::error::consume_candidate: replicant dep resolved to $dep_path, not under scratch $scratch" >&2; exit 1 ;;
  esac
  echo "consume_candidate: replicant dep resolves to $dep_path" >&2

  # --- 5. Semantic R01-R05 smoke from the artifact-derived, freshly compiled modules. --------
  env -u MIX_PATH -u ERL_LIBS -u REPLICANT_TEST_URL MIX_ENV=dev SMOKE_SCRATCH="$scratch" \
    mix run --no-start "$repo_root/scripts/release/consumer_smoke.exs" >&2
)

log "OK — candidate $version consumed from a fresh external project ($tarball)"

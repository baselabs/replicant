#!/usr/bin/env bash
# Prove the retained Replicant candidate from a FRESH, external consumer that touches only the
# extracted package bytes — not the repository checkout, not the repo's _build.
#
# Steps: Hex-checksum-validate + unpack the exact retained tar -> build a brand-new Mix consumer
# whose only Replicant source is the extraction -> clear MIX_PATH/ERL_LIBS -> deps.get -> compile
# with warnings-as-errors -> run a semantic R01-R05 smoke that asserts the loaded Replicant BEAM
# resolves under the scratch tree. A missing/corrupt packaged file is invisible from the checkout;
# it is caught here.
#
# Usage: consume_candidate.sh [TARBALL]   (default: .kimosabe/artifacts/replicant-<version>.tar)
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
log() { echo "consume_candidate: $*" >&2; }
die() { echo "::error::consume_candidate: $*" >&2; exit 1; }

version="$(grep -oE '@version "[^"]+"' "$repo_root/mix.exs" | head -1 | sed -E 's/@version "([^"]+)"/\1/')"
tarball="${1:-$repo_root/.kimosabe/artifacts/replicant-$version.tar}"
[[ -f "$tarball" ]] || die "candidate tarball not found: $tarball (build it first with build_candidate.sh)"

scratch="$(mktemp -d "${TMPDIR:-/tmp}/replicant-consume.XXXXXX")"
scratch="$(cd "$scratch" && pwd -P)"
cleanup() { rm -rf "$scratch"; }
trap cleanup EXIT
src="$scratch/replicant_src"
consumer="$scratch/consumer"

# --- 1. Hex-checksum-validated unpack of the EXACT retained tar (no rebuild). ----------------
( cd "$repo_root" && mix run --no-start scripts/release/unpack_validated.exs "$tarball" "$src" ) >&2
[[ -f "$src/mix.exs" ]] || die "extraction produced no mix.exs at $src"

# --- 2. A brand-new consumer that depends ONLY on the extracted source. ----------------------
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

# --- 3. Fresh resolve + compile from a clean environment (no repo _build, no MIX_PATH). -------
log "building fresh consumer against extracted source $src"
(
  cd "$consumer"
  env -u MIX_PATH -u ERL_LIBS -u REPLICANT_TEST_URL MIX_ENV=dev \
    mix deps.get >&2
  env -u MIX_PATH -u ERL_LIBS -u REPLICANT_TEST_URL MIX_ENV=dev \
    mix compile --warnings-as-errors >&2

  # Provenance: the resolved dependency source must be the scratch extraction, never the repo.
  dep_path="$(env -u MIX_PATH mix run --no-start -e 'IO.puts(Path.expand(Mix.Project.deps_paths()[:replicant]))')"
  case "$dep_path" in
    "$scratch"/*) : ;;
    *) echo "::error::consume_candidate: replicant dep resolved to $dep_path, not under scratch $scratch" >&2; exit 1 ;;
  esac
  echo "consume_candidate: replicant dep resolves to $dep_path" >&2

  # --- 4. Semantic R01-R05 smoke from the artifact-derived, freshly compiled modules. --------
  env -u MIX_PATH -u ERL_LIBS -u REPLICANT_TEST_URL MIX_ENV=dev SMOKE_SCRATCH="$scratch" \
    mix run --no-start "$repo_root/scripts/release/consumer_smoke.exs" >&2
)

log "OK — candidate $version consumed from a fresh external project ($tarball)"

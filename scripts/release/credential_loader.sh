#!/usr/bin/env bash
# Sourced helper: load exactly one inert HEX_API_KEY assignment from a plain KEY=VALUE
# credential file. The file is only ever PARSED (never sourced), so the enforced
# property is inertness: every non-blank line is a plain assignment (a conservative
# value charset — no quoting, expansion, whitespace, or command substitution) or a
# full-line comment, and exactly one of them is HEX_API_KEY — the project .env may
# also carry the repo's other documented env vars (e.g. REPLICANT_TEST_URL). The key
# line may sit at any position; a final line without a newline is fine. An inherited
# key is always replaced.

replicant_load_hex_api_key() {
  local env_file="${1:?credential file required}" credential_line=""

  [[ -r "$env_file" ]] || {
    echo "::error::publish_candidate: project credential file unavailable" >&2
    return 1
  }

  awk '
    BEGIN {
      hex_line = "^HEX_API_KEY=[A-Za-z0-9_-]+$"
      plain_line = "^[A-Za-z_][A-Za-z0-9_]*=[A-Za-z0-9_.:@~%+/=-]*$"
    }
    /^[[:space:]]*$/ { next }
    /^#/ { next }
    $0 ~ hex_line { seen++; next }
    $0 ~ plain_line { next }
    { bad++ }
    END { exit !(seen == 1 && bad == 0) }
  ' "$env_file" || {
    echo "::error::publish_candidate: project credential file must contain only plain KEY=VALUE assignments (no quoting/expansion) with exactly one HEX_API_KEY" >&2
    return 1
  }

  unset HEX_API_KEY
  credential_line="$(awk 'BEGIN { p = "^HEX_API_KEY=[A-Za-z0-9_-]+$" } $0 ~ p { print; exit }' "$env_file")"
  HEX_API_KEY="${credential_line#HEX_API_KEY=}"
  export HEX_API_KEY
}

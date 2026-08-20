#!/usr/bin/env bash
# Sourced helper: load exactly one inert HEX_API_KEY assignment, including when the
# file's final line has no newline. It always replaces an inherited key.

replicant_load_hex_api_key() {
  local env_file="${1:?credential file required}" credential_line=""

  [[ -r "$env_file" ]] || {
    echo "::error::publish_candidate: project credential file unavailable" >&2
    return 1
  }

  awk '
    /^[[:space:]]*$/ { next }
    /^HEX_API_KEY=[A-Za-z0-9_-]+$/ { seen++; next }
    { bad++ }
    END { exit !(seen == 1 && bad == 0) }
  ' "$env_file" || {
    echo "::error::publish_candidate: project credential file must contain only one plain HEX_API_KEY assignment" >&2
    return 1
  }

  unset HEX_API_KEY
  IFS= read -r credential_line < "$env_file" || [[ -n "$credential_line" ]]
  HEX_API_KEY="${credential_line#HEX_API_KEY=}"
  export HEX_API_KEY
}

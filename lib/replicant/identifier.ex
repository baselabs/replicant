defmodule Replicant.Identifier do
  @moduledoc """
  Allowlist validation for Postgres identifiers `replicant` interpolates into slot
  and publication SQL (Critical Rule 2). A failure carries the invalid-SHAPE fact
  only, never the offending string — it may be attacker-controlled.

  Mirrors `arcadic`'s `Identifier`, tightened to the Postgres unquoted-identifier
  rule: lowercase letters, digits, underscore; first character a letter or
  underscore; at most 63 characters (`NAMEDATALEN - 1`). Postgres folds unquoted
  identifiers to lowercase and rejects the rest, so the allowlist accepts only
  what Postgres accepts without quoting — nothing that can break out of the
  identifier position.
  """

  # Postgres unquoted identifier: [a-z_][a-z0-9_]{0,62}  (63 chars total).
  # `\z` (not `\Z`) forbids a trailing newline: `\Z` would match before a final
  # "\n", letting `"orders\n; DROP"` slip past — a SQL-injection bypass.
  @pattern ~r/\A[a-z_][a-z0-9_]{0,62}\z/

  @doc "Returns `:ok` for a valid identifier, `{:error, :invalid_identifier}` otherwise."
  @spec validate(term()) :: :ok | {:error, :invalid_identifier}
  def validate(value) when is_binary(value) do
    if Regex.match?(@pattern, value), do: :ok, else: {:error, :invalid_identifier}
  end

  def validate(_value), do: {:error, :invalid_identifier}
end

defmodule Replicant.QueryBuilder do
  @moduledoc """
  Slot and publication SQL/command strings built from **validated** identifiers
  (Critical Rule 2). Hardens `walex`'s raw `'\#{publication}'` interpolation: every
  name passes through `Replicant.Identifier.validate/1` before it reaches the
  string; an invalid name returns `{:error, :invalid_identifier}` and builds
  nothing. `START_REPLICATION`/`CREATE_REPLICATION_SLOT` are replication commands
  (no bind parameters), so validated interpolation is the gate.
  """

  alias Replicant.Identifier

  @pgoutput "pgoutput"

  @doc """
  Replication command that starts streaming WAL from `start_lsn` for the publication.

  `opts[:start_lsn]` is a `Replicant.lsn/0` (`non_neg_integer`, default `0`). A
  non-integer or negative value raises (caller contract, not attacker input) —
  pass the uint64 WAL position from `checkpoint/0`.
  """
  @spec start_replication(String.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, :invalid_identifier}
  def start_replication(slot_name, publication, opts \\ []) do
    with :ok <- Identifier.validate(slot_name),
         :ok <- Identifier.validate(publication) do
      start_lsn = Keyword.get(opts, :start_lsn, 0)
      lsn_literal = Replicant.lsn_to_string(start_lsn)

      {:ok,
       "START_REPLICATION SLOT #{slot_name} LOGICAL #{lsn_literal} " <>
         "(proto_version '1', publication_names '#{publication}')"}
    end
  end

  @doc "Replication command to create a durable logical slot (NOEXPORT_SNAPSHOT)."
  @spec create_durable_slot(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def create_durable_slot(slot_name) do
    with :ok <- Identifier.validate(slot_name) do
      {:ok, "CREATE_REPLICATION_SLOT #{slot_name} LOGICAL #{@pgoutput} NOEXPORT_SNAPSHOT;"}
    end
  end

  @doc "Query returning `1` if the publication exists."
  @spec publication_exists(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def publication_exists(publication) do
    with :ok <- Identifier.validate(publication) do
      {:ok, "SELECT 1 FROM pg_publication WHERE pubname = '#{publication}' LIMIT 1;"}
    end
  end

  @doc "Query returning the `active` flag for the replication slot."
  @spec slot_exists(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def slot_exists(slot_name) do
    with :ok <- Identifier.validate(slot_name) do
      {:ok, "SELECT active FROM pg_replication_slots WHERE slot_name = '#{slot_name}' LIMIT 1;"}
    end
  end

  @doc """
  Query returning `wal_status` and `conflicting` for the replication slot — the
  PG16 invalidation signals (spec §8). `wal_status = 'lost'` means WAL the slot
  needs was removed (`max_slot_wal_keep_size` exceeded); `conflicting = true`
  means a standby recovery conflict invalidated the slot. Both are unrecoverable
  data gaps → fail-closed halt. (PG16 has no `invalidation_reason` column — that
  is PG17+; `wal_status`/`conflicting` are the PG16-correct signals.)
  """
  @spec slot_invalidation_status(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def slot_invalidation_status(slot_name) do
    with :ok <- Identifier.validate(slot_name) do
      {:ok,
       "SELECT wal_status, conflicting FROM pg_replication_slots " <>
         "WHERE slot_name = '#{slot_name}' LIMIT 1;"}
    end
  end

  @doc "Query returning `pg_is_in_recovery()` — `true` on a standby (spec §8 R-ISO advisory)."
  @spec is_in_recovery() :: String.t()
  # Name mirrors PostgreSQL's own `pg_is_in_recovery()` function (not an Elixir
  # boolean predicate — it builds a SQL string), so it is exempt from the `?` rule.
  # credo:disable-for-next-line Credo.Check.Readability.PredicateFunctionNames
  def is_in_recovery, do: "SELECT pg_is_in_recovery();"
end

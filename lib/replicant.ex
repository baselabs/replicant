defmodule Replicant do
  @moduledoc """
  Framework-agnostic Elixir CDC consumer for Postgres logical replication
  (`pgoutput`), delivering committed row changes to a pluggable **sink** with
  **zero data loss**: the replication slot advances only after the sink has
  durably persisted the transaction.

  `replicant` is **tenant-blind and classification-blind** — the reliable CDC
  consumer sibling to `arcadic` and `ash_age`. Multitenancy, classification, and
  Ash resources live one layer up, in a future `ash_replicant` sink adapter.

  ## LSN representation

  A Postgres LSN is exposed as a single `non_neg_integer` — the 64-bit value
  `(xlog_file <<< 32) ||| xlog_offset` — so that ordinary integer comparison is
  correct WAL ordering, and the same value feeds the wire-level standby status
  update. Use `lsn_to_string/1` for display (`"0/16E3778"`); LSNs are WAL
  positions, not row data, so they are permitted in telemetry metadata.

  ## This is the offline core

  This library version ships the decode / assemble / validate / redact core:
  the vendored `pgoutput` parser, the type-aware assembler, the
  identifier-validated SQL builder, and the pluggable `Replicant.Sink` behaviour.
  Live streaming (the `Postgrex.ReplicationConnection` that owns the slot and
  acks only after the sink commits) lands in the next slice.
  """

  @typedoc """
  A Postgres LSN as a single 64-bit integer `(file <<< 32) ||| offset`.

  Integer comparison (`<=`, `>`) is correct WAL ordering, so the exactly-once
  watermark is `txn.commit_lsn <= checkpoint`.
  """
  @type lsn :: non_neg_integer()

  @doc """
  The hex display form of an LSN, e.g. `lsn_to_string(0x16E3778) == "0/16E3778"`.
  Postgres displays `pg_lsn` as uppercase `file/offset` hex with no padding.
  """
  @spec lsn_to_string(lsn()) :: String.t()
  def lsn_to_string(lsn) when is_integer(lsn) and lsn >= 0 do
    file = Bitwise.bsr(lsn, 32)
    offset = Bitwise.band(lsn, 0xFFFFFFFF)
    "#{Integer.to_string(file, 16)}/#{Integer.to_string(offset, 16)}"
  end
end

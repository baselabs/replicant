defmodule Replicant.Test.SnapshotSink do
  @moduledoc """
  A snapshot-capable ledger sink for the integration suite. `handle_snapshot/2` upserts
  rows into `sink_orders` and, on `first_for_table? == true`, TRUNCATEs the table first
  (the redo-safety obligation, spec §6.1). `handle_snapshot_complete/1` durably sets the
  `_replicant_checkpoint` watermark. After the handoff, streaming uses the same watermark
  via `handle_transaction/1` (delegated to `LedgerSink`) so the whole run is one
  loss/dup-audited ledger.
  """
  @behaviour Replicant.Sink

  alias Replicant.{Change, Test.LedgerSink}

  @conn Replicant.Test.LedgerConn

  @impl true
  def checkpoint, do: LedgerSink.checkpoint()

  @impl true
  def handle_transaction(txn), do: LedgerSink.handle_transaction(txn)

  @impl true
  def handle_snapshot(changes, %{first_for_table?: first?}) do
    Postgrex.transaction(@conn, fn c ->
      if first?, do: Postgrex.query!(c, "TRUNCATE sink_orders", [])

      Enum.each(changes, fn %Change{record: r} ->
        Postgrex.query!(
          c,
          "INSERT INTO sink_orders (id, note) VALUES ($1, $2) " <>
            "ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note",
          [r["id"], r["note"]]
        )
      end)
    end)

    :ok
  rescue
    e -> {:error, e}
  end

  @impl true
  def handle_snapshot_complete(lsn) do
    Postgrex.query!(
      @conn,
      "INSERT INTO _replicant_checkpoint (id, lsn) VALUES (1, $1) " <>
        "ON CONFLICT (id) DO UPDATE SET lsn = EXCLUDED.lsn",
      [lsn]
    )

    {:ok, lsn}
  end
end

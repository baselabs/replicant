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
    Postgrex.transaction(
      @conn,
      fn c ->
        if first?, do: Postgrex.query!(c, "TRUNCATE sink_orders", [])

        Enum.each(changes, fn %Change{record: r} ->
          Postgrex.query!(
            c,
            "INSERT INTO sink_orders (id, note) VALUES ($1, $2) " <>
              "ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note",
            [r["id"], r["note"]]
          )
        end)
      end,
      timeout: 60_000
    )

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

defmodule Replicant.Test.SlowSnapshotSink do
  @moduledoc "SnapshotSink whose handle_snapshot sleeps, keeping the snapshot in-progress for orphan-lifetime tests."
  @behaviour Replicant.Sink

  alias Replicant.Test.SnapshotSink

  @impl true
  def checkpoint, do: SnapshotSink.checkpoint()
  @impl true
  def handle_transaction(txn), do: SnapshotSink.handle_transaction(txn)
  @impl true
  def handle_snapshot(changes, ctx) do
    # Signal (from INSIDE the snapshotter process) that the snapshot is genuinely
    # mid-flight, THEN block. Orphan-lifetime tests wait for this before tearing the
    # pipeline down so the kill lands while the snapshotter is truly in-progress.
    :telemetry.execute([:replicant, :test, :slow_snapshot_sleeping], %{}, %{})
    Process.sleep(5_000)
    SnapshotSink.handle_snapshot(changes, ctx)
  end

  @impl true
  def handle_snapshot_complete(lsn), do: SnapshotSink.handle_snapshot_complete(lsn)
end

defmodule Replicant.Test.WriteOnlySink do
  @moduledoc "A lib-mode sink: declares @behaviour, implements ONLY handle_transaction/1 (checkpoint/0 is optional)."
  @behaviour Replicant.Sink

  @impl Replicant.Sink
  def handle_transaction(%Replicant.Transaction{} = txn), do: {:ok, txn.commit_lsn}
end

defmodule Replicant.Test.CheckpointRecordingSink do
  @moduledoc """
  A NON-transactional recording sink for lib-mode integration tests. Appends each
  applied change's `id` to a `sink_ledger` table via a named Postgrex conn
  (`Replicant.Test.SinkLedgerConn`, started by the test), WITHOUT writing any
  checkpoint — the library owns it. It records BOTH streamed transactions (via
  `handle_transaction/1`) AND snapshot backfill batches (via `handle_snapshot/2`), so a
  `snapshot: true` lib-mode pipeline's whole surface — the backfill AND the post-handoff
  stream — lands in the one `sink_ledger`. `handle_snapshot_complete/1` exists only to
  satisfy `Replicant.Sink.supports_snapshot?/1` (the config gate for `snapshot: true`
  requires BOTH snapshot callbacks); it is inert in lib mode (the library writes the
  handoff to the checkpoint store, not the sink). Autocommit per call: there is no atomic
  data+checkpoint unit, so this exercises the dup-never-loss path.
  """
  @behaviour Replicant.Sink

  @conn Replicant.Test.SinkLedgerConn

  @impl Replicant.Sink
  def sink_kind, do: :append_log

  @impl Replicant.Sink
  def handle_transaction(%Replicant.Transaction{} = txn) do
    Enum.each(txn.changes, fn change ->
      id = change.record && change.record["id"]

      if id do
        Postgrex.query!(
          @conn,
          "INSERT INTO sink_ledger (commit_lsn, id) VALUES ($1, $2)",
          [txn.commit_lsn, id]
        )
      end
    end)

    {:ok, txn.commit_lsn}
  end

  # Snapshot (backfill) rows arrive here, not via handle_transaction/1: the Snapshotter
  # dispatches each batch through `sink.handle_snapshot/2` (lib.replicant.snapshotter
  # dispatch_batch!/6). Record each backfilled id into the same `sink_ledger` (stamped
  # with the snapshot's consistent point as commit_lsn) so the lib-mode sink observes the
  # backfill exactly as it observes streamed transactions. Implementing BOTH snapshot
  # callbacks flips `Replicant.Sink.supports_snapshot?/1` true, which is the config gate
  # for `snapshot: true`.
  @impl Replicant.Sink
  def handle_snapshot(changes, %{snapshot_lsn: snapshot_lsn}) do
    Enum.each(changes, fn change ->
      id = change.record && change.record["id"]

      if id do
        Postgrex.query!(
          @conn,
          "INSERT INTO sink_ledger (commit_lsn, id) VALUES ($1, $2)",
          [snapshot_lsn, id]
        )
      end
    end)

    :ok
  end

  # The snapshot handoff commit. In lib mode the LIBRARY owns the checkpoint (written to
  # the CheckpointStore by the Connection on {:snapshot_done, lsn}); this sink does not
  # persist it. Return {:ok, lsn} to satisfy the Snapshotter's sink-owned-shape contract
  # and to complete the callback pair (`supports_snapshot?/1`). The value is ignored on
  # the lib path (Snapshotter.complete/5 :lib clause never calls this), but the callback
  # must exist for `snapshot: true` to be accepted.
  @impl Replicant.Sink
  def handle_snapshot_complete(lsn), do: {:ok, lsn}
end

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
  checkpoint — the library owns it. Autocommit per `handle_transaction/1`: there is
  no atomic data+checkpoint unit, so this exercises the dup-never-loss path.
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
end

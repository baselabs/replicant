defmodule Replicant.Test.LedgerBatchSink do
  @moduledoc """
  A transactional batch-delivery sink (spec §6). `handle_batch/1` persists EVERY transaction's
  rows into `bd_sink_orders` AND `checkpoint := the batch's highest commit_lsn` into `bd_sink_cp`
  in ONE database transaction (the atomic obligation effect-once rests on). Rows upsert by PK; a
  re-delivered batch is deduped by `commit_lsn <= checkpoint`. Uses the named non-replication
  connection Replicant.Test.BatchDeliveryConn.
  """
  @behaviour Replicant.Sink

  alias Replicant.{Change, Transaction}

  @conn Replicant.Test.BatchDeliveryConn

  @impl true
  def checkpoint do
    case Postgrex.query(@conn, "SELECT lsn FROM bd_sink_cp WHERE id = 1", []) do
      {:ok, %Postgrex.Result{rows: [[lsn]]}} -> {:ok, lsn}
      {:ok, %Postgrex.Result{rows: []}} -> {:ok, nil}
      {:error, _reason} = err -> err
    end
  end

  @impl true
  def handle_batch(transactions) do
    highest = transactions |> Enum.map(& &1.commit_lsn) |> Enum.max()

    result =
      Postgrex.transaction(@conn, fn c ->
        Enum.each(transactions, fn %Transaction{commit_lsn: lsn, changes: changes} ->
          # record every delivered txn (for dup auditing) and apply its rows
          Postgrex.query!(c, "INSERT INTO bd_sink_calls (lsn) VALUES ($1)", [lsn])
          Enum.each(changes, &apply_change(c, &1))
        end)

        # atomic checkpoint at the batch's highest LSN — the CHECK-constraint fault target
        Postgrex.query!(
          c,
          "INSERT INTO bd_sink_cp (id, lsn) VALUES (1, $1) " <>
            "ON CONFLICT (id) DO UPDATE SET lsn = EXCLUDED.lsn",
          [highest]
        )
      end)

    case result do
      {:ok, _} -> {:ok, highest}
      {:error, reason} -> {:error, reason}
    end
  end

  defp apply_change(c, %Change{op: op, record: r}) when op in [:insert, :update] do
    Postgrex.query!(
      c,
      "INSERT INTO bd_sink_orders (id, note) VALUES ($1, $2) " <>
        "ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note",
      [r["id"], r["note"]]
    )
  end

  defp apply_change(c, %Change{op: :delete, old_record: old}),
    do: Postgrex.query!(c, "DELETE FROM bd_sink_orders WHERE id = $1", [old["id"]])
end

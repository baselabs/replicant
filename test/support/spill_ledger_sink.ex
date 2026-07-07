defmodule Replicant.Test.SpillLedgerSink do
  @moduledoc "Transactional spill test sink: consumes txn.changes LAZILY (single-pass) within the DB txn; rows+checkpoint atomic; append-only sp_sink_calls ledger per delivered txn."
  @behaviour Replicant.Sink

  alias Replicant.{Change, Transaction}

  @conn Replicant.Test.SpillConn

  @impl true
  def checkpoint do
    case Postgrex.query(@conn, "SELECT lsn FROM sp_sink_cp WHERE id = 1", []) do
      {:ok, %Postgrex.Result{rows: [[lsn]]}} -> {:ok, lsn}
      {:ok, %Postgrex.Result{rows: []}} -> {:ok, nil}
      {:error, _} = err -> err
    end
  end

  @impl true
  def handle_transaction(%Transaction{commit_lsn: lsn, changes: changes}) do
    result =
      Postgrex.transaction(
        @conn,
        fn c ->
          n =
            Enum.reduce(changes, 0, fn %Change{} = ch, acc ->
              apply_change(c, ch)
              acc + 1
            end)

          Postgrex.query!(c, "INSERT INTO sp_sink_calls (lsn, n) VALUES ($1, $2)", [lsn, n])

          Postgrex.query!(
            c,
            "INSERT INTO sp_sink_cp (id, lsn) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET lsn = EXCLUDED.lsn",
            [lsn]
          )
        end,
        timeout: 120_000
      )

    case result do
      {:ok, _} -> {:ok, lsn}
      {:error, reason} -> {:error, reason}
    end
  end

  defp apply_change(c, %Change{op: op, record: r}) when op in [:insert, :update],
    do:
      Postgrex.query!(
        c,
        "INSERT INTO sp_sink_rows (id) VALUES ($1) ON CONFLICT (id) DO NOTHING",
        [
          r["id"]
        ]
      )

  defp apply_change(c, %Change{op: :delete, old_record: old}),
    do: Postgrex.query!(c, "DELETE FROM sp_sink_rows WHERE id = $1", [old["id"]])

  defp apply_change(_c, _change), do: :ok
end

defmodule Replicant.Test.StreamingLedgerSink do
  @moduledoc "Transactional streaming test sink: rows + checkpoint atomic; append-only st_sink_calls ledger per delivered txn."
  @behaviour Replicant.Sink

  alias Replicant.{Change, Transaction}

  @conn Replicant.Test.StreamingConn

  @impl true
  def checkpoint do
    case Postgrex.query(@conn, "SELECT lsn FROM st_sink_cp WHERE id = 1", []) do
      {:ok, %Postgrex.Result{rows: [[lsn]]}} -> {:ok, lsn}
      {:ok, %Postgrex.Result{rows: []}} -> {:ok, nil}
      {:error, _} = err -> err
    end
  end

  @impl true
  def handle_transaction(%Transaction{commit_lsn: lsn, changes: changes}) do
    result =
      Postgrex.transaction(@conn, fn c ->
        Postgrex.query!(c, "INSERT INTO st_sink_calls (lsn, n) VALUES ($1, $2)", [
          lsn,
          length(changes)
        ])

        Enum.each(changes, &apply_change(c, &1))

        Postgrex.query!(
          c,
          "INSERT INTO st_sink_cp (id, lsn) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET lsn = EXCLUDED.lsn",
          [lsn]
        )
      end)

    case result do
      {:ok, _} -> {:ok, lsn}
      {:error, reason} -> {:error, reason}
    end
  end

  defp apply_change(c, %Change{op: op, record: r}) when op in [:insert, :update],
    do:
      Postgrex.query!(
        c,
        "INSERT INTO st_sink_rows (id) VALUES ($1) ON CONFLICT (id) DO NOTHING",
        [
          r["id"]
        ]
      )

  defp apply_change(c, %Change{op: :delete, old_record: old}),
    do: Postgrex.query!(c, "DELETE FROM st_sink_rows WHERE id = $1", [old["id"]])

  defp apply_change(_c, _change), do: :ok
end

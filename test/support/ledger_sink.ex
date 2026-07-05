defmodule Replicant.Test.LedgerSink do
  @moduledoc """
  A transactional LSN-watermark sink for the crash-injection suite (spec §6). Each
  `handle_transaction/1` persists the transaction's rows into `sink_orders` AND the
  checkpoint into `_replicant_checkpoint` in ONE database transaction (the
  checkpoint-after-persist-atomic contract), records the outcome in
  `_replicant_calls` (for loss/dup auditing), and dedups by
  `commit_lsn <= checkpoint` — a re-delivered transaction is recorded `skipped` and
  applied zero more times (effect-once). Rows upsert by PK.

  Uses a normal (non-replication) named Postgrex connection `Replicant.Test.LedgerConn`,
  started by the integration test setup.
  """
  @behaviour Replicant.Sink

  alias Replicant.{Change, Transaction}

  @conn Replicant.Test.LedgerConn

  @impl true
  def checkpoint do
    case Postgrex.query(@conn, "SELECT lsn FROM _replicant_checkpoint WHERE id = 1", []) do
      {:ok, %Postgrex.Result{rows: [[lsn]]}} -> {:ok, lsn}
      {:ok, %Postgrex.Result{rows: []}} -> {:ok, nil}
      {:error, _reason} = err -> err
    end
  end

  @impl true
  def handle_transaction(%Transaction{commit_lsn: lsn, changes: changes}) do
    result =
      Postgrex.transaction(@conn, fn c ->
        case current_checkpoint(c) do
          cp when is_integer(cp) and lsn <= cp ->
            record_call(c, lsn, "skipped")

          _not_yet_applied ->
            Enum.each(changes, &apply_change(c, &1))
            set_checkpoint(c, lsn)
            record_call(c, lsn, "applied")
        end
      end)

    case result do
      {:ok, _} -> {:ok, lsn}
      {:error, reason} -> {:error, reason}
    end
  end

  defp current_checkpoint(c) do
    case Postgrex.query!(c, "SELECT lsn FROM _replicant_checkpoint WHERE id = 1", []).rows do
      [[lsn]] -> lsn
      [] -> nil
    end
  end

  defp set_checkpoint(c, lsn) do
    Postgrex.query!(
      c,
      "INSERT INTO _replicant_checkpoint (id, lsn) VALUES (1, $1) " <>
        "ON CONFLICT (id) DO UPDATE SET lsn = EXCLUDED.lsn",
      [lsn]
    )
  end

  defp record_call(c, lsn, outcome) do
    Postgrex.query!(c, "INSERT INTO _replicant_calls (lsn, outcome) VALUES ($1, $2)", [
      lsn,
      outcome
    ])
  end

  defp apply_change(c, %Change{op: op, record: r}) when op in [:insert, :update] do
    Postgrex.query!(
      c,
      "INSERT INTO sink_orders (id, note) VALUES ($1, $2) " <>
        "ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note",
      [r["id"], r["note"]]
    )
  end

  defp apply_change(c, %Change{op: :delete, old_record: old}) do
    Postgrex.query!(c, "DELETE FROM sink_orders WHERE id = $1", [old["id"]])
  end

  defp apply_change(c, %Change{op: :truncate}) do
    Postgrex.query!(c, "TRUNCATE sink_orders", [])
  end
end

defmodule Replicant.Test.SlowLedgerSink do
  @moduledoc "LedgerSink with a per-transaction delay, to exercise §4 backpressure."
  @behaviour Replicant.Sink

  alias Replicant.Test.LedgerSink

  @impl true
  def checkpoint, do: LedgerSink.checkpoint()

  @impl true
  def handle_transaction(txn) do
    Process.sleep(25)
    LedgerSink.handle_transaction(txn)
  end
end

defmodule Replicant.Test.FailOpenLedgerSink do
  @moduledoc """
  A `LedgerSink` whose `checkpoint/0` reports `{:ok, nil}` — the spec §14.15
  checkpoint-read-fail-open condition. Used ONLY by the re-delivery-dedup test to
  exercise the sink idempotency contract (spec §6) end-to-end through the live
  pipeline.

  `checkpoint/0` is read in two places (Task 2/1): the `Connection` seeds its
  resume LSN from it, and the `Assembler` pre-skips a re-delivered transaction with
  it. Reporting `nil` makes the Connection resume from `0/0` (so PG re-delivers an
  already-applied transaction) AND disables the Assembler pre-skip (so the
  re-delivered transaction reaches the sink) — the ONLY path on which the sink's
  own watermark dedup is exercised. `handle_transaction/1` delegates to
  `LedgerSink`, which reads its DURABLE `_replicant_checkpoint` watermark inside the
  apply transaction and records the re-delivery as `skipped` (applied zero more
  times, effect-once). Rows still upsert by PK, so a bare re-delivery without the
  watermark would also be effect-safe; the watermark makes the dedup observable.

  This is the faithful realization of risk #1: a clean Connection crash cannot
  re-deliver an already-checkpointed transaction (the durable checkpoint IS the
  resume LSN, and `START_REPLICATION` skips transactions at/below it), so the sink
  dedup is only reachable when the checkpoint read itself fails open.
  """
  @behaviour Replicant.Sink

  alias Replicant.Test.LedgerSink

  @impl true
  def checkpoint, do: {:ok, nil}

  @impl true
  def handle_transaction(txn), do: LedgerSink.handle_transaction(txn)
end

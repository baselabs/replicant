defmodule ReplicationPipeline.Sink do
  @moduledoc """
  The example's transactional sink: the orders replica, a value-free receipts
  row per change, and the commit-LSN watermark are written in ONE destination
  transaction (effect-once, Critical Rule 3 — the `commit_lsn <= checkpoint`
  skip below IS the dedup; duplicates cannot reach this code twice for the
  same transaction).

  Delivery is at-least-once; the watermark skip + PK upsert make the EFFECTS
  exactly-once — the receipts ledger shows it: a receipt exists iff its
  transaction took effect.

  Record values arrive ALREADY TYPED — the Assembler casts tuple data through
  `Replicant.Casting.Types.cast_record/2` (int4 → integer, timestamptz →
  DateTime, text → binary), so they pass to Postgrex as parameters unchanged.
  """

  @behaviour Replicant.Sink

  @dest ReplicationPipeline.Dest

  # Column knowledge for the one published table. The sink knows its
  # destination schema; this list IS that knowledge.
  @orders_columns ["id", "note", "payload", "updated_at"]
  @orders_pk ["id"]

  @impl true
  def checkpoint do
    case Postgrex.query(@dest, "SELECT commit_lsn FROM pipeline_checkpoint WHERE id = 1", []) do
      {:ok, %Postgrex.Result{rows: [[lsn]]}} -> {:ok, lsn}
      {:ok, %Postgrex.Result{rows: []}} -> {:ok, nil}
      {:error, _reason} = error -> error
    end
  end

  # Session identity (ADR-0007): this callback runs on every connect BEFORE
  # checkpoint/0, with the identity read by IDENTIFY_SYSTEM on the exact
  # replication connection. First boot BINDS {system_identifier, database}
  # into the checkpoint row (a NULL commit_lsn = bound, nothing delivered);
  # every later connect COMPARES — a mismatching source (e.g. a rebuilt
  # source container = new system_identifier) rejects and the pipeline halts
  # fail-closed instead of silently resuming against a different database.
  @impl true
  def handle_session_identity(%Replicant.SessionIdentity{} = identity, _context) do
    case Postgrex.query(
           @dest,
           "SELECT system_identifier, database FROM pipeline_checkpoint WHERE id = 1",
           []
         ) do
      {:ok, %Postgrex.Result{rows: []}} ->
        bind_identity!(identity)

      {:ok, %Postgrex.Result{rows: [[stored_id, stored_db]]}} ->
        if stored_id == identity.system_identifier and stored_db == identity.database do
          :ok
        else
          {:error, :session_identity_rejected}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp bind_identity!(identity) do
    case Postgrex.query(
           @dest,
           """
           INSERT INTO pipeline_checkpoint (id, slot_name, system_identifier, database, commit_lsn, updated_at)
           VALUES (1, $1, $2, $3, NULL, now())
           ON CONFLICT (id) DO NOTHING
           """,
           [slot_name(), identity.system_identifier, identity.database]
         ) do
      {:ok, _result} ->
        # Bind, then VERIFY: a racing first connect with a different identity
        # wins the INSERT (ours hits DO NOTHING) — re-read and compare so the
        # loser does not silently accept itself.
        case Postgrex.query(
               @dest,
               "SELECT system_identifier, database FROM pipeline_checkpoint WHERE id = 1",
               []
             ) do
          {:ok, %Postgrex.Result{rows: [[stored_id, stored_db]]}} ->
            if stored_id == identity.system_identifier and stored_db == identity.database,
              do: :ok,
              else: {:error, :session_identity_rejected}

          {:error, _reason} = error ->
            error
        end

      {:error, _reason} = error ->
        error
    end
  end

  @impl true
  def handle_transaction(%Replicant.Transaction{commit_lsn: lsn, changes: changes}) do
    case Postgrex.transaction(@dest, fn conn ->
           watermark = lock_watermark!(conn)

           if lsn <= watermark do
             {:skip, watermark}
           else
             # SINGLE pass over changes: a spilled transaction's `changes` is a
             # single-pass disk-backed Reader (Replicant.Transaction) — apply
             # and collect the receipt row in one iteration. The accumulator is
             # METADATA ONLY ({schema, table, op} — never a value), so a spilled
             # transaction's memory bound is this small tuple list, not the txn.
             receipts =
               Enum.map(changes, fn change ->
                 apply_change!(conn, change)
                 {change.schema, change.table, Atom.to_string(change.op)}
               end)

             put_receipts!(conn, lsn, receipts)
             put_watermark!(conn, lsn)
             {:committed, lsn}
           end
         end) do
      {:ok, {:skip, watermark}} -> {:ok, watermark}
      {:ok, {:committed, lsn}} -> {:ok, lsn}
      # Postgrex errors are structural but their DETAIL can echo key values
      # (e.g. a constraint violation names the key); the library's delivery
      # boundary scrubs the returned term before any log or telemetry.
      {:error, %Postgrex.Error{} = error} -> {:error, error}
      {:error, _other} = error -> error
    end
  end

  # Locked FOR UPDATE so concurrent deliveries serialize on the watermark row
  # once it exists; an absent OR NULL watermark (identity bound, nothing
  # delivered yet) means 0. Safe because the library delivers one transaction
  # at a time per pipeline.
  defp lock_watermark!(conn) do
    {:ok, %Postgrex.Result{rows: rows}} =
      Postgrex.query(
        conn,
        "SELECT commit_lsn FROM pipeline_checkpoint WHERE id = 1 FOR UPDATE",
        []
      )

    case rows do
      [[lsn]] when is_integer(lsn) -> lsn
      [_unbound_or_absent] -> 0
      [] -> 0
    end
  end

  defp put_watermark!(conn, lsn) do
    {:ok, %Postgrex.Result{command: :insert}} =
      Postgrex.query(
        conn,
        """
        INSERT INTO pipeline_checkpoint (id, slot_name, commit_lsn, updated_at)
        VALUES (1, $1, $2, now())
        ON CONFLICT (id) DO UPDATE
        SET commit_lsn = EXCLUDED.commit_lsn, updated_at = now()
        """,
        [slot_name(), lsn]
      )

    :ok
  end

  # One value-free receipt PER CHANGE, written in the same destination
  # transaction as the data — a receipt exists iff its transaction took
  # effect (exactly-once at TRANSACTION granularity: the watermark skip above
  # suppresses every receipt of a re-delivered transaction together).
  defp put_receipts!(conn, lsn, receipts) do
    {:ok, %Postgrex.Result{command: :insert}} =
      Postgrex.query(
        conn,
        """
        INSERT INTO cdc_receipts (commit_lsn, schema_name, table_name, op)
        SELECT * FROM unnest($1::bigint[], $2::text[], $3::text[], $4::text[])
        """,
        [
          List.duplicate(lsn, length(receipts)),
          for({s, _, _} <- receipts, do: s),
          for({_, t, _} <- receipts, do: t),
          for({_, _, o} <- receipts, do: o)
        ]
      )

    :ok
  end

  defp apply_change!(conn, %Replicant.Change{table: "orders"} = change) do
    case change.op do
      :insert -> upsert!(conn, change)
      :update -> upsert!(conn, change)
      :delete -> delete!(conn, change)
    end
  end

  # The publication carries only `orders`; anything else is a configuration
  # drift the example refuses to apply silently.
  defp apply_change!(_conn, %Replicant.Change{table: table}) do
    raise ArgumentError, "unexpected table in example publication: #{inspect(table)}"
  end

  # Upsert by PK. An UPDATE that did not touch a TOASTed column sends a
  # sentinel that NEVER appears in `record` — the column is absent from BOTH
  # the SET clause AND the INSERT column list (a `Map.fetch!` on it would
  # crash), so the destination value survives (Critical Rule 4).
  defp upsert!(conn, %Replicant.Change{record: record, unchanged: unchanged}) do
    insert_cols = Enum.reject(@orders_columns, &(&1 in unchanged))

    set_cols = Enum.reject(insert_cols, &(&1 in @orders_pk))

    placeholders = Enum.map_join(1..length(insert_cols), ", ", &"$#{&1}")

    on_conflict =
      case set_cols do
        [] ->
          # Every non-PK column is an unchanged-TOAST sentinel: conflict on the
          # PK alone and touch nothing.
          "ON CONFLICT (id) DO NOTHING"

        _ ->
          "ON CONFLICT (id) DO UPDATE SET " <>
            Enum.map_join(set_cols, ", ", &"#{&1} = EXCLUDED.#{&1}")
      end

    sql =
      "INSERT INTO orders (#{Enum.join(insert_cols, ", ")}) VALUES (#{placeholders}) " <>
        on_conflict

    params = Enum.map(insert_cols, &Map.fetch!(record, &1))

    {:ok, %Postgrex.Result{command: :insert}} = Postgrex.query(conn, sql, params)
    :ok
  end

  # Under the default replica identity, a DELETE carries ONLY `old_record`
  # (key columns); `record` is nil for deletes. A replica identity so weak
  # that even the key is absent halts rather than deleting by guesswork.
  defp delete!(conn, %Replicant.Change{old_record: old_record}) when is_map(old_record) do
    {:ok, %Postgrex.Result{command: :delete}} =
      Postgrex.query(conn, "DELETE FROM orders WHERE id = $1", [Map.fetch!(old_record, "id")])

    :ok
  end

  defp delete!(_conn, %Replicant.Change{table: table}) do
    raise ArgumentError, "delete without a key-bearing old_record: #{inspect(table)}"
  end

  defp slot_name, do: System.fetch_env!("REPLICANT_SLOT_NAME")
end

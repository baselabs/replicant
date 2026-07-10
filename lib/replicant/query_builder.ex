defmodule Replicant.QueryBuilder do
  @moduledoc """
  Slot and publication SQL/command strings built from **validated** identifiers
  (Critical Rule 2). Hardens `walex`'s raw `'\#{publication}'` interpolation: every
  name passes through `Replicant.Identifier.validate/1` before it reaches the
  string; an invalid name returns `{:error, :invalid_identifier}` and builds
  nothing. `START_REPLICATION`/`CREATE_REPLICATION_SLOT` are replication commands
  (no bind parameters), so validated interpolation is the gate.
  """

  alias Replicant.Identifier

  @pgoutput "pgoutput"

  # PG exports a snapshot name as "%08X-%08X-%d". This allowlist forbids quotes and
  # whitespace so the name is safe inside the SET TRANSACTION SNAPSHOT '<name>' STRING
  # LITERAL — it is NOT an identifier position, so `Identifier.validate/1` (which rejects
  # uppercase hex and hyphens) is the WRONG guard here (spec §9).
  @snapshot_name ~r/\A[0-9A-Fa-f]{1,16}-[0-9A-Fa-f]{1,16}-\d{1,10}\z/

  @doc """
  Replication command that starts streaming WAL from `start_lsn` for the publication.

  `opts[:start_lsn]` is a `t:Replicant.lsn/0` (`non_neg_integer`, default `0`). A
  non-integer or negative value raises (caller contract, not attacker input) —
  pass the uint64 WAL position from `checkpoint/0`.

  `opts[:streaming]`, when truthy, selects `proto_version '2', streaming 'on'`
  (in-progress transaction streaming, spec §5). Absent or falsy (the default)
  emits the byte-for-byte v1 command (`proto_version '1'`, no streaming clause).
  """
  @spec start_replication(String.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, :invalid_identifier}
  def start_replication(slot_name, publication, opts \\ []) do
    with :ok <- Identifier.validate(slot_name),
         :ok <- Identifier.validate(publication) do
      start_lsn = Keyword.get(opts, :start_lsn, 0)
      lsn_literal = Replicant.lsn_to_string(start_lsn)

      proto =
        if Keyword.get(opts, :streaming),
          do: "proto_version '2', streaming 'on'",
          else: "proto_version '1'"

      {:ok,
       "START_REPLICATION SLOT #{slot_name} LOGICAL #{lsn_literal} " <>
         "(#{proto}, publication_names '#{publication}')"}
    end
  end

  @doc "Replication command to create a durable logical slot (NOEXPORT_SNAPSHOT)."
  @spec create_durable_slot(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def create_durable_slot(slot_name) do
    with :ok <- Identifier.validate(slot_name) do
      {:ok, "CREATE_REPLICATION_SLOT #{slot_name} LOGICAL #{@pgoutput} NOEXPORT_SNAPSHOT;"}
    end
  end

  @doc """
  Replication command to create a durable logical slot that EXPORTS a consistent
  snapshot (spec §4). The result row is `[slot_name, consistent_point, snapshot_name,
  output_plugin]` — the caller reads `consistent_point` (a `pg_lsn` string) and
  `snapshot_name` from it.
  """
  @spec create_export_slot(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def create_export_slot(slot_name) do
    with :ok <- Identifier.validate(slot_name) do
      {:ok, "CREATE_REPLICATION_SLOT #{slot_name} LOGICAL #{@pgoutput} EXPORT_SNAPSHOT;"}
    end
  end

  @doc """
  Command adopting an exported snapshot by name (spec §9). The name is validated as a
  snapshot-name LITERAL — not an identifier — before interpolation into the quoted
  string; a name with a quote/whitespace/other injection returns
  `{:error, :invalid_snapshot_name}` and builds nothing.
  """
  @spec set_transaction_snapshot(term()) :: {:ok, String.t()} | {:error, :invalid_snapshot_name}
  def set_transaction_snapshot(name) when is_binary(name) do
    if Regex.match?(@snapshot_name, name) do
      {:ok, "SET TRANSACTION SNAPSHOT '#{name}'"}
    else
      {:error, :invalid_snapshot_name}
    end
  end

  def set_transaction_snapshot(_name), do: {:error, :invalid_snapshot_name}

  @doc """
  Query returning each publication table's `schemaname`, `tablename`, and PG-quoted
  fully-qualified name (`format('%I.%I', …)`, spec §9). The quoted `qualified` column is
  interpolation-safe for any valid identifier (mixed-case/quoted tables the streaming
  path also supports); the raw `schemaname`/`tablename` fill the `%Change{}` fields. The
  publication name is validated (already an allowlisted identifier) then interpolated.
  """
  @spec publication_tables(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def publication_tables(publication) do
    with :ok <- Identifier.validate(publication) do
      {:ok,
       "SELECT schemaname, tablename, format('%I.%I', schemaname, tablename) AS qualified " <>
         "FROM pg_publication_tables WHERE pubname = '#{publication}'"}
    end
  end

  @doc "Query returning `1` if the publication exists."
  @spec publication_exists(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def publication_exists(publication) do
    with :ok <- Identifier.validate(publication) do
      {:ok, "SELECT 1 FROM pg_publication WHERE pubname = '#{publication}' LIMIT 1;"}
    end
  end

  @doc "Query returning the `active` flag for the replication slot."
  @spec slot_exists(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def slot_exists(slot_name) do
    with :ok <- Identifier.validate(slot_name) do
      {:ok, "SELECT active FROM pg_replication_slots WHERE slot_name = '#{slot_name}' LIMIT 1;"}
    end
  end

  @doc """
  Query returning `wal_status` and `conflicting` for the replication slot — the
  PG16 invalidation signals (spec §8). `wal_status = 'lost'` means WAL the slot
  needs was removed (`max_slot_wal_keep_size` exceeded); `conflicting = true`
  means a standby recovery conflict invalidated the slot. Both are unrecoverable
  data gaps → fail-closed halt. (PG16 has no `invalidation_reason` column — that
  is PG17+; `wal_status`/`conflicting` are the PG16-correct signals.)
  """
  @spec slot_invalidation_status(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def slot_invalidation_status(slot_name) do
    with :ok <- Identifier.validate(slot_name) do
      {:ok,
       "SELECT wal_status, conflicting FROM pg_replication_slots " <>
         "WHERE slot_name = '#{slot_name}' LIMIT 1;"}
    end
  end

  @doc """
  DDL creating the lib-owned checkpoint table if absent. `slot_name` is the PK, one
  row per slot; `commit_lsn` is a `bigint` (the `t:Replicant.lsn/0` integer — no
  `pg_lsn` text parse at the boundary). The table name is a validated identifier;
  `IF NOT EXISTS` is a name check only, so the caller MUST also shape-probe (see
  `checkpoint_column_probe/0`).
  """
  @spec checkpoint_ensure_table(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def checkpoint_ensure_table(table) do
    with :ok <- Identifier.validate(table) do
      {:ok,
       "CREATE TABLE IF NOT EXISTS #{table} " <>
         "(slot_name text PRIMARY KEY, commit_lsn bigint NOT NULL, " <>
         "updated_at timestamptz NOT NULL DEFAULT now())"}
    end
  end

  @doc "Query reading `commit_lsn` for a slot. `slot_name` is bound `$1`; only the validated table is interpolated."
  @spec checkpoint_read(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def checkpoint_read(table) do
    with :ok <- Identifier.validate(table) do
      {:ok, "SELECT commit_lsn FROM #{table} WHERE slot_name = $1"}
    end
  end

  @doc "Upsert of `commit_lsn` for a slot. `slot_name`/`commit_lsn` are bound `$1`/`$2`; only the validated table is interpolated."
  @spec checkpoint_upsert(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def checkpoint_upsert(table) do
    with :ok <- Identifier.validate(table) do
      {:ok,
       "INSERT INTO #{table} (slot_name, commit_lsn, updated_at) VALUES ($1, $2, now()) " <>
         "ON CONFLICT (slot_name) DO UPDATE SET commit_lsn = EXCLUDED.commit_lsn, updated_at = now()"}
    end
  end

  @doc """
  Query probing the `commit_lsn` column's `data_type`. The table name is bound `$1`
  (a string value in `information_schema`, not an identifier position), so no
  interpolation and no validation are needed here — the caller has already validated
  the table for the interpolating builders above.
  """
  @spec checkpoint_column_probe() :: String.t()
  def checkpoint_column_probe do
    "SELECT data_type FROM information_schema.columns " <>
      "WHERE table_name = $1 AND column_name = 'commit_lsn' LIMIT 1"
  end

  @doc "Query returning `pg_is_in_recovery()` — `true` on a standby (spec §8 R-ISO advisory)."
  @spec is_in_recovery() :: String.t()
  # Name mirrors PostgreSQL's own `pg_is_in_recovery()` function (not an Elixir
  # boolean predicate — it builds a SQL string), so it is exempt from the `?` rule.
  # credo:disable-for-next-line Credo.Check.Readability.PredicateFunctionNames
  def is_in_recovery, do: "SELECT pg_is_in_recovery();"

  @doc """
  Query returning, per publication table, its ordered PRIMARY KEY columns — BOTH the
  raw `attname` (to read `%Change{}.record` string keys) and the server-quoted form
  via `quote_ident` (the ONLY form ever interpolated into keyset SQL — spec §6.6,
  Critical Rule 2; the `format('%I')` precedent). Publication name is bound `$1`.
  A publication table with NO primary key returns no row here (PK-less fallback,
  spec §6.4). Row shape: `[schemaname, tablename, qualified, pk_raw, pk_quoted]`.

  Per-column TYPE oids come from `table_columns/0` (the full column set already covers
  the PK columns), so no per-PK type array is duplicated here.
  """
  @spec pk_columns() :: String.t()
  def pk_columns do
    "SELECT p.schemaname, p.tablename, format('%I.%I', p.schemaname, p.tablename) AS qualified, " <>
      "array_agg(a.attname ORDER BY k.ord) AS pk_raw, " <>
      "array_agg(quote_ident(a.attname) ORDER BY k.ord) AS pk_quoted " <>
      "FROM pg_publication_tables p " <>
      "JOIN pg_class c ON c.relname = p.tablename " <>
      "JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = p.schemaname " <>
      "JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary " <>
      "JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, ord) ON true " <>
      "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum " <>
      "WHERE p.pubname = $1 " <>
      "GROUP BY p.schemaname, p.tablename"
  end

  @doc """
  Query returning, per publication table, ALL its non-dropped user columns
  (`attnum > 0 AND NOT attisdropped`, the `SELECT *` column set) ordered by `attnum` —
  the raw `attname` (the `%Change{}.record` string key and the result column name), the
  server-quoted form via `quote_ident` (the ONLY form interpolated into the keyset/keyless
  `::text` projection — Critical Rule 2, the `format('%I')` precedent), and each column's
  `atttypid`. The reader maps the OID to a pgoutput type name (`OidDatabase.name_for_type_id/1`)
  and casts the `<col>::text` value through the SAME `Casting.Types.cast_record/2` path the
  stream uses, so snapshot and stream deliver byte-identical `%Change{}.record` values for
  EVERY column (spec §2 convergence — the F1 fix generalized from PK-only to all columns).
  Publication name is bound `$1`. Row shape:
  `[schemaname, tablename, qualified, col_raw, col_quoted, col_type_oids]`.
  """
  @spec table_columns() :: String.t()
  def table_columns do
    "SELECT p.schemaname, p.tablename, format('%I.%I', p.schemaname, p.tablename) AS qualified, " <>
      "array_agg(a.attname ORDER BY a.attnum) AS col_raw, " <>
      "array_agg(quote_ident(a.attname) ORDER BY a.attnum) AS col_quoted, " <>
      "array_agg(a.atttypid::int ORDER BY a.attnum) AS col_type_oids " <>
      "FROM pg_publication_tables p " <>
      "JOIN pg_class c ON c.relname = p.tablename " <>
      "JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = p.schemaname " <>
      "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped " <>
      "WHERE p.pubname = $1 " <>
      "GROUP BY p.schemaname, p.tablename"
  end

  @doc """
  Keyset chunk SELECT (spec §6.6). `col_quoted` (all columns) and `pk_quoted` (the PK
  subset) come from `table_columns/0`/`pk_columns/0` (server-quoted — never validated
  client-side, never raw). `bound_arity` is the number of PK columns in the resume bound:
  `0` emits the first-chunk form (no WHERE); `n > 0` emits a ROW() comparison whose bounds
  are BIND PARAMETERS `$2..$n+1` (`$1` is the LIMIT) — a literal bound would be
  simultaneously an injection surface and a Rule-1 leak into logged SQL text. An empty PK
  list is a caller error.

  EVERY column is projected as `<col>::text AS <col>` so each delivered value is the
  type's text output — byte-identical to pgoutput text, which the stream casts through the
  SAME `Casting.Types.cast_record/2` path (spec §2 convergence). The WHERE/ORDER BY reference
  the REAL typed PK columns — TABLE-QUALIFIED (`<qualified>.<pk>`, NOT bare `<pk>`) so they
  bind to the typed table columns, NEVER the same-named `::text` OUTPUT ALIAS: a bare
  `ORDER BY <pk>` resolves to the projected alias (SQL lets ORDER BY see output names),
  which would sort keyset pages LEXICOGRAPHICALLY (`1,10,100,…`) and silently skip rows.
  """
  @spec keyset_chunk(String.t(), [String.t()], [String.t()], non_neg_integer()) ::
          {:ok, String.t()} | {:error, :invalid_identifier}
  def keyset_chunk(_qualified, _col_quoted, [], _bound_arity), do: {:error, :invalid_identifier}

  def keyset_chunk(qualified, col_quoted, pk_quoted, 0) do
    {:ok,
     "SELECT #{cast_projection(col_quoted)}#{rpk_select(pk_quoted)} FROM #{qualified} " <>
       "ORDER BY #{pk_ref(qualified, pk_quoted)} LIMIT $1"}
  end

  def keyset_chunk(qualified, col_quoted, pk_quoted, bound_arity)
      when bound_arity == length(pk_quoted) do
    params = Enum.map_join(2..(bound_arity + 1), ", ", &"$#{&1}")

    {:ok,
     "SELECT #{cast_projection(col_quoted)}#{rpk_select(pk_quoted)} FROM #{qualified} " <>
       "WHERE (#{pk_ref(qualified, pk_quoted)}) > (#{params}) " <>
       "ORDER BY #{pk_ref(qualified, pk_quoted)} LIMIT $1"}
  end

  @doc """
  Whole-table scan for the PK-less fallback (spec §6.4). Every column is projected as
  `<col>::text AS <col>` (the SAME cast projection as `keyset_chunk/4`) so a PK-less
  table's snapshot rows converge with the stream too (the record values are cast; there
  is no keyset bound). `col_quoted` is server-quoted (`table_columns/0`), never raw.
  """
  @spec keyless_scan(String.t(), [String.t()]) :: String.t()
  def keyless_scan(qualified, col_quoted),
    do: "SELECT #{cast_projection(col_quoted)} FROM #{qualified}"

  # Cast projection: every column as `<quoted>::text AS <quoted>`. The `::text` output
  # equals pgoutput text (both use the type's output function), and the reader casts it
  # through `Casting.Types.cast_record/2` — the exact stream path — so the delivered
  # `%Change{}.record` is byte-identical to the stream's for every type (spec §2).
  defp cast_projection(col_quoted), do: Enum.map_join(col_quoted, ", ", &"#{&1}::text AS #{&1}")

  # TABLE-QUALIFIED PK column list (`<qualified>.<pk>, …`) for the keyset WHERE/ORDER BY. The
  # qualification is what makes these bind to the REAL typed table columns and not the
  # same-named `::text` output alias in the projection (which ORDER BY would otherwise pick,
  # sorting lexicographically). `qualified` is server-quoted (`format('%I.%I')`) and
  # `pk_quoted` is `quote_ident`-quoted — both interpolation-safe (Critical Rule 2).
  defp pk_ref(qualified, pk_quoted), do: Enum.map_join(pk_quoted, ", ", &"#{qualified}.#{&1}")

  # Trailing RAW (uncast) PK projections ("__rpk_1"…): they carry the keyset RESUME BOUND.
  # Unlike the cast record columns, the bound MUST ride as the NATIVE Postgrex value so it
  # binds back into the next chunk's `WHERE (pk) > ($2..)` — a cast uuid/timestamp value is
  # NOT bind-compatible with its typed column (Postgrex rejects a 36-byte dashed-string uuid
  # for a `uuid` param). `pk_canon` (the drop-set key) is derived separately from the CAST
  # record, so these projections carry ONLY the bind-compatible bound.
  defp rpk_select(pk_quoted) do
    pk_quoted
    |> Enum.with_index(1)
    |> Enum.map_join("", fn {col, i} -> ", #{col} AS __rpk_#{i}" end)
  end

  @doc """
  Read-only watermark position (spec §2/§4): `pg_current_wal_lsn()` on a primary,
  `pg_last_wal_replay_lsn()` on a standby (the chunk reader connects to the same host
  as the replication connection, so its snapshot visibility is bounded by replay).
  """
  @spec watermark_lsn(boolean()) :: String.t()
  def watermark_lsn(true), do: "SELECT pg_last_wal_replay_lsn()::text;"
  def watermark_lsn(false), do: "SELECT pg_current_wal_lsn()::text;"

  @doc "DDL creating the lib-owned snapshot-progress table if absent (token is an opaque bytea; spec §6.2)."
  @spec progress_ensure_table(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def progress_ensure_table(table) do
    with :ok <- Identifier.validate(table) do
      {:ok,
       "CREATE TABLE IF NOT EXISTS #{table} " <>
         "(slot_name text PRIMARY KEY, token bytea NOT NULL, " <>
         "updated_at timestamptz NOT NULL DEFAULT now())"}
    end
  end

  @doc "Query reading the progress token for a slot. `slot_name` bound `$1`; only the validated table is interpolated."
  @spec progress_read(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def progress_read(table) do
    with :ok <- Identifier.validate(table) do
      {:ok, "SELECT token FROM #{table} WHERE slot_name = $1"}
    end
  end

  @doc "Upsert of the progress token for a slot. `slot_name`/`token` bound `$1`/`$2`."
  @spec progress_upsert(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def progress_upsert(table) do
    with :ok <- Identifier.validate(table) do
      {:ok,
       "INSERT INTO #{table} (slot_name, token, updated_at) VALUES ($1, $2, now()) " <>
         "ON CONFLICT (slot_name) DO UPDATE SET token = EXCLUDED.token, updated_at = now()"}
    end
  end
end

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

  @doc "The replication-protocol command that identifies the exact source session."
  @spec identify_system() :: String.t()
  def identify_system, do: "IDENTIFY_SYSTEM"

  # PG 10+ exports a snapshot name as "%08X-%08X-%d"; PG 9.6 exports "%08X-%d" (observed
  # "00004E57-1" from CREATE_REPLICATION_SLOT ... LOGICAL wal2json on 9.6.24), so the
  # middle hex group is optional. This allowlist forbids quotes and whitespace so the name
  # is safe inside the SET TRANSACTION SNAPSHOT '<name>' STRING LITERAL — it is NOT an
  # identifier position, so `Identifier.validate/1` (which rejects uppercase hex and
  # hyphens) is the WRONG guard here (spec §9).
  @snapshot_name ~r/\A[0-9A-Fa-f]{1,16}(?:-[0-9A-Fa-f]{1,16})?-\d{1,10}\z/

  @doc """
  Replication command that starts streaming WAL from `start_lsn` for the publication set.

  `publication` is a non-empty list of validated identifiers (Critical Rule 2): a single-element
  list reproduces the published-0.1.0 command byte-for-byte; a multi-element list is comma-joined
  into `publication_names 'p1,p2'` (each name is an allowlisted bare identifier, so the bare
  comma-list is injection-safe — probe-verified). `opts[:start_lsn]` is a `t:Replicant.lsn/0`
  (`non_neg_integer`, default `0`).

  `opts[:streaming]`, when truthy, selects `proto_version '2', streaming 'on'` (spec §5).
  `opts[:messages]`, when truthy, adds `messages 'true'` (spec §6, A2). Absent or falsy (the
  default) emits the byte-for-byte v1 command. An `opts[:start_lsn]` that is not a
  non-negative integer returns `{:error, :invalid_start_lsn}` (1.3.0) — a tagged error
  like every other builder failure, never a raise.
  """
  @spec start_replication(String.t(), [String.t()], keyword()) ::
          {:ok, String.t()} | {:error, :invalid_identifier} | {:error, :invalid_start_lsn}
  def start_replication(slot_name, publications, opts \\ []) when is_list(publications) do
    with :ok <- Identifier.validate(slot_name),
         :ok <- validate_all(publications),
         :ok <- validate_start_lsn(Keyword.get(opts, :start_lsn, 0)) do
      start_lsn = Keyword.get(opts, :start_lsn, 0)
      lsn_literal = Replicant.lsn_to_string(start_lsn)

      proto =
        if Keyword.get(opts, :streaming),
          do: "proto_version '2', streaming 'on'",
          else: "proto_version '1'"

      pub_list = Enum.join(publications, ",")

      options =
        if Keyword.get(opts, :messages),
          do: "#{proto}, publication_names '#{pub_list}', messages 'true'",
          else: "#{proto}, publication_names '#{pub_list}'"

      {:ok, "START_REPLICATION SLOT #{slot_name} LOGICAL #{lsn_literal} (#{options})"}
    end
  end

  # Validate every name in a publication list (Critical Rule 2). Short-circuits on the first
  # invalid name; an empty list is invalid (the caller must supply at least one publication).
  defp validate_all([]), do: {:error, :invalid_identifier}

  defp validate_all(names) when is_list(names) do
    Enum.reduce_while(names, :ok, fn name, :ok ->
      case Identifier.validate(name) do
        :ok -> {:cont, :ok}
        {:error, :invalid_identifier} = err -> {:halt, err}
      end
    end)
  end

  @doc """
  Plugin-parameterized `START_REPLICATION` (ADR-0009 §2): renders the decoder plugin's
  ordered option list as `key 'value'` pairs inside the command's parentheses. The
  slot name and plugin name are `Identifier.validate`d; every option KEY matches
  `[a-z0-9_.-]+` and every option VALUE matches `[a-zA-Z0-9_.,*()-]*` (the option
  grammar the three shipped decoders emit — identifier lists, booleans, version
  numbers), so a quote or semicolon breakout is refused `{:error, :invalid_identifier}`
  and NEVER interpolated. The `#{@pgoutput}` rendering is byte-identical to
  `start_replication/3` (pinned by test).
  """
  @spec start_replication_for(
          String.t(),
          String.t(),
          [{String.t(), String.t()}],
          non_neg_integer()
        ) ::
          {:ok, String.t()} | {:error, :invalid_identifier} | {:error, :invalid_start_lsn}
  def start_replication_for(slot_name, plugin, options, start_lsn) do
    with :ok <- Identifier.validate(slot_name),
         :ok <- Identifier.validate(plugin),
         :ok <- validate_start_lsn(start_lsn),
         :ok <- validate_plugin_options(options) do
      lsn_literal = Replicant.lsn_to_string(start_lsn)
      rendered = Enum.map_join(options, ", ", fn {k, v} -> "#{k} '#{v}'" end)
      {:ok, "START_REPLICATION SLOT #{slot_name} LOGICAL #{lsn_literal} (#{rendered})"}
    end
  end

  @plugin_option_key ~r/\A[a-z0-9_.-]+\z/
  @plugin_option_key_quoted ~r/\A"[a-z0-9_.-]+"\z/
  @plugin_option_value ~r/\A[a-zA-Z0-9_.,*()-]*\z/

  # A key is either a bare option name or a DOUBLE-QUOTED identifier (pglogical's
  # dotted `"pglogical.replication_set_names"` — the walsender option grammar's only
  # spelling for it; OBSERVED from pglogical's own worker). Both forms are validated
  # so a quote or semicolon breakout can never ride an option into the command.
  defp plugin_option_key?(key) do
    Regex.match?(@plugin_option_key, key) or Regex.match?(@plugin_option_key_quoted, key)
  end

  defp validate_plugin_options(options) when is_list(options) do
    Enum.reduce_while(options, :ok, fn {k, v}, :ok ->
      if is_binary(k) and is_binary(v) and plugin_option_key?(k) and
           Regex.match?(@plugin_option_value, v) do
        {:cont, :ok}
      else
        {:halt, {:error, :invalid_identifier}}
      end
    end)
  end

  # start_lsn must be a t:Replicant.lsn/0 (non_neg_integer) — 1.3.0: tagged error
  # instead of a FunctionClauseError out of lsn_to_string/1.
  defp validate_start_lsn(lsn) when is_integer(lsn) and lsn >= 0, do: :ok
  defp validate_start_lsn(_), do: {:error, :invalid_start_lsn}

  @doc """
  Replication command to create a durable logical slot with NO exported snapshot. `failover?:
  false` (the published default) emits the legacy `NOEXPORT_SNAPSHOT` keyword byte-for-byte;
  `failover?: true` emits the PG17 parenthesized `(FAILOVER, SNAPSHOT 'nothing')` grammar (spec
  §6). The caller has already gated failover-on-PG16 at connect, so `failover? == true` implies PG17.
  """
  @spec create_durable_slot(String.t(), boolean()) ::
          {:ok, String.t()} | {:error, :invalid_identifier}
  def create_durable_slot(slot_name, failover?) do
    create_durable_slot(slot_name, failover?, @pgoutput)
  end

  @doc """
  `create_durable_slot/2` with the decoder's output plugin (ADR-0009 §2). The default
  `#{@pgoutput}` emits the published 1.3.0 string byte-for-byte; `pglogical_output` and
  `wal2json` name their plugin. The plugin name passes `Identifier.validate/1` before
  interpolation exactly like a slot name (Critical Rule 2).
  """
  @spec create_durable_slot(String.t(), boolean(), String.t()) ::
          {:ok, String.t()} | {:error, :invalid_identifier}
  def create_durable_slot(slot_name, failover?, plugin) do
    create_durable_slot(slot_name, failover?, plugin, 150_000)
  end

  @doc """
  `create_durable_slot/3` with the server version: `NOEXPORT_SNAPSHOT` (and the
  parenthesized snapshot grammar) exist from PG 15 — on 9.6 to 14 the durable-slot
  form is the bare `CREATE_REPLICATION_SLOT … LOGICAL plugin;` (the server exports a
  snapshot, which is simply discarded). PG15+ — and an unknown version (0, a
  directly-constructed state) — emits the published strings unchanged.
  """
  @spec create_durable_slot(String.t(), boolean(), String.t(), non_neg_integer()) ::
          {:ok, String.t()} | {:error, :invalid_identifier}
  def create_durable_slot(slot_name, failover?, plugin, version) do
    with :ok <- Identifier.validate(slot_name),
         :ok <- Identifier.validate(plugin) do
      tail =
        cond do
          version > 0 and version < 150_000 -> ""
          failover? -> " (FAILOVER, SNAPSHOT 'nothing')"
          true -> " NOEXPORT_SNAPSHOT"
        end

      {:ok, "CREATE_REPLICATION_SLOT #{slot_name} LOGICAL #{plugin}#{tail};"}
    end
  end

  @doc """
  Replication command to create a durable logical slot that EXPORTS a consistent snapshot (spec
  §4). `failover?: false` emits the legacy `EXPORT_SNAPSHOT` keyword byte-for-byte; `failover?:
  true` emits `(FAILOVER, SNAPSHOT 'export')` (spec §6). The result row is `[slot_name,
  consistent_point, snapshot_name, output_plugin]` in both forms.
  """
  @spec create_export_slot(String.t(), boolean()) ::
          {:ok, String.t()} | {:error, :invalid_identifier}
  def create_export_slot(slot_name, failover?) do
    create_export_slot(slot_name, failover?, @pgoutput)
  end

  @doc "`create_export_slot/2` with the decoder's output plugin (see `create_durable_slot/3`)."
  @spec create_export_slot(String.t(), boolean(), String.t()) ::
          {:ok, String.t()} | {:error, :invalid_identifier}
  def create_export_slot(slot_name, failover?, plugin) do
    create_export_slot(slot_name, failover?, plugin, 150_000)
  end

  @doc """
  `create_export_slot/3` with the server version: `EXPORT_SNAPSHOT` (and the
  parenthesized snapshot grammar) exist from PG 15 — on 9.6 to 14 the form is the
  bare `CREATE_REPLICATION_SLOT … LOGICAL plugin;` (the server exports the snapshot
  by default; the result row shape is unchanged). The plugin-decoder + `snapshot:
  true` combination on 9.6/12 rides exactly this pre-15 form.
  """
  @spec create_export_slot(String.t(), boolean(), String.t(), non_neg_integer()) ::
          {:ok, String.t()} | {:error, :invalid_identifier}
  def create_export_slot(slot_name, failover?, plugin, version) do
    with :ok <- Identifier.validate(slot_name),
         :ok <- Identifier.validate(plugin) do
      tail =
        cond do
          version < 150_000 -> ""
          failover? -> " (FAILOVER, SNAPSHOT 'export')"
          true -> " EXPORT_SNAPSHOT"
        end

      {:ok, "CREATE_REPLICATION_SLOT #{slot_name} LOGICAL #{plugin}#{tail};"}
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
  Query returning each DISTINCT publication table's `schemaname`, `tablename`, and PG-quoted
  fully-qualified name across the publication LIST (spec §5.3 / decision #19). The `DISTINCT`
  collapses the pubname dimension BEFORE it fans the join, so a table in two publications yields
  ONE row (not two). The publication names are bound `$1` (an array), so no interpolation.
  """
  @spec publication_tables([String.t()]) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def publication_tables(publications) when is_list(publications) do
    with :ok <- validate_all(publications) do
      {:ok,
       "SELECT DISTINCT schemaname, tablename, format('%I.%I', schemaname, tablename) AS qualified " <>
         "FROM pg_publication_tables WHERE pubname = ANY($1)"}
    end
  end

  @doc """
  Query returning the `pubname`s that exist for any name in the validated list (spec §5.3 /
  decision #18). The caller halts fail-closed if the returned set ≠ the requested set —
  `START_REPLICATION` with a missing publication silently streams the existing subset, so it
  CANNOT be the fail-closed gate (probe-verified).

  Unlike `publication_tables/1` / `pk_columns/0` / `table_columns/0` (which run via
  `Postgrex.query!/3` and bind `$1`), this query runs in the `Postgrex.ReplicationConnection`
  connect chain, whose `{:query, sql, state}` dispatch uses the SIMPLE query protocol — which
  cannot bind `$1`. So the validated names are interpolated into a single-quoted `IN (...)` list,
  the exact precedent `slot_invalidation_status/2` sets with `slot_name`. Injection-safe per
  Critical Rule 2: every name passed `Identifier.validate/1` (`[a-z_][a-z0-9_]{0,62}`) BEFORE
  reaching here — the allowlist excludes `'`, `)`, `;`, and every other breakout character, so
  the interpolated literal cannot escape the `IN (...)` position.
  """
  @spec publication_exists([String.t()]) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def publication_exists(publications) when is_list(publications) do
    with :ok <- validate_all(publications) do
      names = Enum.map_join(publications, ",", &"'#{&1}'")
      {:ok, "SELECT pubname FROM pg_publication WHERE pubname IN (#{names})"}
    end
  end

  # ---- ADR-0009: per-decoder table-set discovery ----

  @doc """
  The `:publication_check` existence gate for the `:pglogical` decoder: the replication
  sets that exist in pglogical's own catalog for the validated names. Runs in the
  replication connect chain's SIMPLE protocol, so the validated names are interpolated
  exactly as `publication_exists/1` does (Critical Rule 2). A missing set halts
  fail-closed at connect — pglogical silently streams the intersection otherwise.
  """
  @spec replication_set_exists([String.t()]) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def replication_set_exists(sets) when is_list(sets) do
    with :ok <- validate_all(sets) do
      names = Enum.map_join(sets, ",", &"'#{&1}'")
      {:ok, "SELECT set_name FROM pglogical.replication_set WHERE set_name IN (#{names})"}
    end
  end

  @doc """
  The `:pglogical` decoder's table-set discovery (ADR-0009 §2): the DISTINCT tables
  across the validated replication-set list, from pglogical's own membership catalog
  (OBSERVED columns: `relid`, `nspname`, `relname`, `set_name` — the names ride the
  membership row itself, no catalog joins needed). Same row shape as
  `publication_tables/1` (`schemaname`, `tablename`, qualified).
  """
  @spec replication_set_tables([String.t()]) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def replication_set_tables(sets) when is_list(sets) do
    with :ok <- validate_all(sets) do
      names = Enum.map_join(sets, ",", &"'#{&1}'")

      {:ok,
       "SELECT DISTINCT nspname AS schemaname, relname AS tablename, " <>
         "format('%I.%I', nspname, relname) AS qualified " <>
         "FROM pglogical.tables WHERE set_name IN (#{names})"}
    end
  end

  @doc """
  The relation-info catalog read for the `:pglogical` and `:wal2json` decoders
  (ADR-0009 §5): per configured table, its `pg_class` oid, `relreplident`, and every
  non-dropped column with type oid + typmod — everything the wire does NOT carry
  (pglogical relations have no types and no replica identity; wal2json has no relation
  message at all). Runs in the replication connect chain's SIMPLE protocol; both parts
  of every pair are `Identifier.validate`d before interpolation. A configured table
  with no rows here is absent on the server — the caller halts fail-closed. Row shape:
  `[schemaname, tablename, oid, relreplident, attnum, attname, atttypid, atttypmod]`.
  """
  @spec configured_table_info([{String.t(), String.t()}]) ::
          {:ok, String.t()} | {:error, :invalid_identifier}
  def configured_table_info(tables) when is_list(tables) and tables != [] do
    if Enum.all?(tables, fn {s, t} -> is_binary(s) and is_binary(t) end) do
      validate_table_pairs(tables)
    else
      {:error, :invalid_identifier}
    end
  end

  def configured_table_info(_other), do: {:error, :invalid_identifier}

  defp validate_table_pairs(tables) do
    tables
    |> Enum.reduce_while(:ok, fn {schema, table}, :ok ->
      with :ok <- Identifier.validate(schema),
           :ok <- Identifier.validate(table) do
        {:cont, :ok}
      else
        {:error, :invalid_identifier} = err -> {:halt, err}
      end
    end)
    |> case do
      :ok -> {:ok, table_info_sql(tables)}
      err -> err
    end
  end

  defp table_info_sql(tables) do
    values = Enum.map_join(tables, ", ", fn {s, t} -> "('#{s}','#{t}')" end)

    # `identity_key` marks the columns of the table's replica-identity index —
    # the PK under `d`, the index flagged `indisreplident` under `i`, nothing
    # under `f`/`n` — the same `:key` flag pgoutput's Relation carries and the
    # assembler's old-record key projection keys on. (Resolved via pg_index
    # directly: pg_get_replica_identity_index only exists from PG 10, and this
    # read also serves the 9.6 substrate.)
    "SELECT n.nspname, c.relname, c.oid::int, c.relreplident, a.attnum, a.attname, " <>
      "a.atttypid::int, a.atttypmod::int, " <>
      "(ri.indexrelid IS NOT NULL AND a.attnum = ANY(ri.indkey)) AS identity_key " <>
      "FROM (VALUES #{values}) AS t(nsp, rel) " <>
      "JOIN pg_namespace n ON n.nspname = t.nsp " <>
      "JOIN pg_class c ON c.relname = t.rel AND c.relnamespace = n.oid " <>
      "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped " <>
      "LEFT JOIN LATERAL (SELECT i.indexrelid, i.indkey FROM pg_index i " <>
      "WHERE i.indrelid = c.oid AND " <>
      "(c.relreplident = 'd' AND i.indisprimary OR c.relreplident = 'i' AND i.indisreplident) " <>
      "LIMIT 1) ri ON true " <>
      "ORDER BY n.nspname, c.relname, a.attnum"
  end

  @doc """
  ADR-0009 §4 — the pre-flight output-plugin option probe, against the pipeline's
  OWN slot with the decoder's EXACT option list. A build that cannot express an
  option rejects the call
  with `invalid_parameter_value` / `feature_not_supported` (OBSERVED: wal2json format
  bounds use 0A000, an unknown option uses 22023) — the caller halts
  `:decoder_option_unsupported` instead of feeding the stream-command reconnect loop
  (postgrex surfaces a rejected START_REPLICATION only as a disconnect, and a rejected
  or erroring peek on the WALSENDER itself never returns ReadyForQuery). Any OTHER
  error — pglogical's "produces binary output" (XX000) above all — proves the options
  PARSED, so the caller proceeds. Option values are validated exactly like
  `start_replication_for/4`'s; names are `Identifier.validate`d and interpolated (the
  simple-query protocol cannot bind).
  """
  @spec decoder_option_probe(String.t(), [{String.t(), String.t()}]) ::
          {:ok, String.t()} | {:error, :invalid_identifier}
  def decoder_option_probe(slot_name, options) do
    with :ok <- Identifier.validate(slot_name),
         :ok <- validate_plugin_options(options) do
      pairs =
        Enum.map_join(options, ", ", fn {k, v} ->
          # keys may be double-quoted identifiers; strip the quotes for the peek's
          # text-array form and single-quote the value
          "'#{String.replace(k, "\"", "")}', '#{v}'"
        end)

      {:ok,
       "SELECT count(*) FROM pg_logical_slot_peek_changes('#{slot_name}', NULL, 1, " <>
         "#{pairs})"}
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
  Query returning the slot's invalidation signals (spec §5/§8), gated by the numeric server
  version because the available `pg_replication_slots` columns differ per major (probe-confirmed
  on live PG 15/16/17/18):

    * **PG 15** (`version < 160000`) — `wal_status` ONLY. `conflicting` was added in PG16, so
      selecting it on PG15 errors `column "conflicting" does not exist`. `wal_status = 'lost'`
      is PG15's sole invalidation signal.
    * **PG 16** (`160000 <= version < 170000`) — `wal_status` + `conflicting`
      (`invalidation_reason`/`synced` were added in PG17 and error here).
    * **PG 17+** (`version >= 170000`) — also `invalidation_reason` (Postgres's authoritative
      invalidation field) and `synced` (true on a standby holding a slot synced from the primary).
    * **PG 9.6 to 12** (`version < 130000`) — NO invalidation column (ADR-0009 §9):
      `wal_status` and `max_slot_wal_keep_size` arrive in PG13; before that a slot cannot
      be invalidated by size and the only loss signal is a removed WAL segment, which
      surfaces as a `START_REPLICATION` failure (the command-error watchdog). The
      constant `NULL::text` keeps the row-present/absent signal the connect chain keys on.

  `wal_status = 'lost'` = WAL removed; `conflicting = true` = standby recovery conflict; any
  non-null `invalidation_reason` = invalidated. All are unrecoverable → fail-closed halt.
  """
  @spec slot_invalidation_status(String.t(), non_neg_integer()) ::
          {:ok, String.t()} | {:error, :invalid_identifier}
  def slot_invalidation_status(slot_name, version) do
    with :ok <- Identifier.validate(slot_name) do
      cols =
        cond do
          version >= 170_000 -> "wal_status, conflicting, invalidation_reason, synced"
          version >= 160_000 -> "wal_status, conflicting"
          version >= 130_000 -> "wal_status"
          true -> "NULL::text"
        end

      {:ok,
       "SELECT #{cols} FROM pg_replication_slots " <>
         "WHERE slot_name = '#{slot_name}' LIMIT 1;"}
    end
  end

  @doc """
  Query returning a reused logical slot's `confirmed_flush_lsn` in the current database. This is
  one input to the origin a go-forward stream resumes at, exposed to append consumers via
  `handle_slot_origin/2` (R04). Runs in the replication connect chain's SIMPLE query protocol (no
  `$1` bind), so the Identifier-validated slot name is interpolated — the
  `slot_invalidation_status/2` / `slot_exists/1` precedent (Critical Rule 2). A missing row or NULL
  is not a logical-slot origin and is rejected fail-closed by the connection.
  """
  @spec slot_confirmed_flush(String.t()) :: {:ok, String.t()} | {:error, :invalid_identifier}
  def slot_confirmed_flush(slot_name) do
    with :ok <- Identifier.validate(slot_name) do
      {:ok,
       "SELECT confirmed_flush_lsn FROM pg_replication_slots " <>
         "WHERE slot_name = '#{slot_name}' AND slot_type = 'logical' " <>
         "AND database = current_database() LIMIT 1;"}
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
  Connect-time probe: `pg_is_in_recovery()` (standby detection, spec §8) plus the numeric
  server version (`server_version_num`, e.g. `170010`) in one round trip on the replication
  connection. The version gates the PG17+ invalidation columns (spec §5) and the `FAILOVER`
  slot grammar (spec §6). No identifier to validate — a constant query.
  """
  @spec recovery_and_version() :: String.t()
  def recovery_and_version,
    do: "SELECT pg_is_in_recovery(), current_setting('server_version_num')::int;"

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
      "FROM (SELECT DISTINCT schemaname, tablename FROM pg_publication_tables WHERE pubname = ANY($1)) p " <>
      "JOIN pg_class c ON c.relname = p.tablename " <>
      "JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = p.schemaname " <>
      "JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary " <>
      "JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, ord) ON true " <>
      "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum " <>
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
      "FROM (SELECT DISTINCT schemaname, tablename FROM pg_publication_tables WHERE pubname = ANY($1)) p " <>
      "JOIN pg_class c ON c.relname = p.tablename " <>
      "JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = p.schemaname " <>
      "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped " <>
      "GROUP BY p.schemaname, p.tablename"
  end

  @doc """
  The `table_columns/0` row shape for an EXPLICIT table list (the pglogical/wal2json
  decoders' snapshot path, ADR-0009 §8): same columns, same `attnum` order, but the
  table set comes from the caller (`VALUES` — both parts of every pair are
  `Identifier.validate`d) instead of `pg_publication_tables`.
  """
  @spec table_columns_for([{String.t(), String.t()}]) ::
          {:ok, String.t()} | {:error, :invalid_identifier}
  def table_columns_for(tables) when is_list(tables) and tables != [] do
    with :ok <- ensure_valid_pairs(tables) do
      values = table_values(tables)

      {:ok,
       "SELECT n.nspname AS schemaname, c.relname AS tablename, " <>
         "format('%I.%I', n.nspname, c.relname) AS qualified, " <>
         "array_agg(a.attname ORDER BY a.attnum) AS col_raw, " <>
         "array_agg(quote_ident(a.attname) ORDER BY a.attnum) AS col_quoted, " <>
         "array_agg(a.atttypid::int ORDER BY a.attnum) AS col_type_oids " <>
         "FROM (VALUES #{values}) AS p0(nsp, rel) " <>
         "JOIN pg_namespace n ON n.nspname = p0.nsp " <>
         "JOIN pg_class c ON c.relname = p0.rel AND c.relnamespace = n.oid " <>
         "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped " <>
         "GROUP BY n.nspname, c.relname"}
    end
  end

  def table_columns_for(_other), do: {:error, :invalid_identifier}

  @doc """
  The `pk_columns/0` row shape for an EXPLICIT table list (the pglogical/wal2json
  decoders' incremental path, ADR-0009 §8). Same columns and PK ordering; the table
  set is the caller's validated list.
  """
  @spec pk_columns_for([{String.t(), String.t()}]) ::
          {:ok, String.t()} | {:error, :invalid_identifier}
  def pk_columns_for(tables) when is_list(tables) and tables != [] do
    with :ok <- ensure_valid_pairs(tables) do
      values = table_values(tables)

      {:ok,
       "SELECT n.nspname AS schemaname, c.relname AS tablename, " <>
         "format('%I.%I', n.nspname, c.relname) AS qualified, " <>
         "array_agg(a.attname ORDER BY k.ord) AS pk_raw, " <>
         "array_agg(quote_ident(a.attname) ORDER BY k.ord) AS pk_quoted " <>
         "FROM (VALUES #{values}) AS p0(nsp, rel) " <>
         "JOIN pg_namespace n ON n.nspname = p0.nsp " <>
         "JOIN pg_class c ON c.relname = p0.rel AND c.relnamespace = n.oid " <>
         "JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary " <>
         "JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, ord) ON true " <>
         "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum " <>
         "GROUP BY n.nspname, c.relname"}
    end
  end

  def pk_columns_for(_other), do: {:error, :invalid_identifier}

  defp table_values(tables),
    do: Enum.map_join(tables, ", ", fn {schema, table} -> "('#{schema}','#{table}')" end)

  # :ok-only pair validator (the *_for builders emit their own SQL bodies).
  defp ensure_valid_pairs(tables) do
    Enum.reduce_while(tables, :ok, fn {schema, table}, :ok ->
      with :ok <- Identifier.validate(schema),
           :ok <- Identifier.validate(table) do
        {:cont, :ok}
      else
        {:error, :invalid_identifier} = err -> {:halt, err}
      end
    end)
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
  The `/3` form is version-gated (ADR-0009 §9): PG 9.6 names the current-LSN function
  `pg_current_xlog_location` (the `pg_current_wal_lsn` rename landed in PG 10).
  """
  @spec watermark_lsn(boolean(), non_neg_integer()) :: String.t()
  def watermark_lsn(in_recovery, version \\ 100_021)

  def watermark_lsn(true, _version), do: "SELECT pg_last_wal_replay_lsn()::text;"

  def watermark_lsn(false, version) when version < 100_000,
    do: "SELECT pg_current_xlog_location()::text;"

  def watermark_lsn(false, _version), do: "SELECT pg_current_wal_lsn()::text;"

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

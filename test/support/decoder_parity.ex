defmodule Replicant.Test.DecoderParity do
  @moduledoc """
  Shared live fixture for comparing pgoutput and pglogical. The transaction
  includes cast types, an unchanged TOAST column, and replica-identity variants.
  """

  @doc """
  Connection opts parsed from a `REPLICANT_*_URL` string (the shared integration
  pattern; keeps the password, unlike a naive userinfo split).
  """
  @spec conn_opts(String.t()) :: keyword()
  def conn_opts(url) do
    uri = URI.parse(url)

    base = [
      hostname: uri.host || "localhost",
      port: uri.port || 5432,
      database: String.trim_leading(uri.path || "/postgres", "/"),
      username: (uri.userinfo && String.split(uri.userinfo, ":") |> hd()) || "postgres"
    ]

    case uri.userinfo && String.split(uri.userinfo, ":", parts: 2) do
      [_, pass] -> base ++ [password: pass]
      _ -> base
    end
  end

  @doc """
  The DDL + per-decoder table-set setup. Idempotent per run (fresh suffixes come from
  the caller's slot naming; this drops and recreates the fixture tables).
  """
  def setup!(conn, decoder, extra \\ []) do
    Postgrex.query!(
      conn,
      "DROP TABLE IF EXISTS parity_all, parity_full, parity_idx, parity_nothing CASCADE",
      []
    )

    Enum.each(ddl_statements(), &Postgrex.query!(conn, &1, []))

    case decoder do
      :pgoutput ->
        pub = Keyword.fetch!(extra, :publication)
        Postgrex.query!(conn, "DROP PUBLICATION IF EXISTS #{pub}", [])
        Postgrex.query!(conn, "DROP PUBLICATION IF EXISTS #{pub}_nothing", [])

        # parity_nothing rides an INSERT-ONLY publication: a default publication
        # (publishes updates) makes the server REFUSE the fixture's keyless
        # UPDATE/DELETE outright (55000 — the refusal the halt test proves on its
        # own leg), aborting the whole fixture transaction before any leg
        # delivers. Insert-only delivery mirrors pglogical's default_insert_only
        # set, so the parity fixture can compare inserts on both decoders.
        Postgrex.query!(
          conn,
          "CREATE PUBLICATION #{pub} FOR TABLE parity_all, parity_full, parity_idx",
          []
        )

        Postgrex.query!(
          conn,
          "CREATE PUBLICATION #{pub}_nothing FOR TABLE parity_nothing WITH (publish = 'insert')",
          []
        )

      :pglogical ->
        # OBSERVED pglogical constraint: a table whose replica identity carries no
        # usable identity index (FULL, NOTHING) cannot join a set that replicates
        # updates/deletes (pglogical demands a PK identity index), so those tables ride
        # the insert-only set — their update/delete legs are pgoutput-only
        # (the parity test compares per table; NOTHING behavior asserted in halts).
        Enum.each(["parity_all", "parity_idx"], fn t ->
          Postgrex.query!(
            conn,
            "SELECT pglogical.replication_set_add_table('default', 'public.#{t}', false)",
            []
          )
        end)

        Enum.each(["parity_full", "parity_nothing"], fn t ->
          Postgrex.query!(
            conn,
            "SELECT pglogical.replication_set_add_table('default_insert_only', 'public.#{t}', false)",
            []
          )
        end)
    end
  end

  @doc "Applies the same fixture transaction on each decoder's source."
  def apply_fixture!(conn) do
    Postgrex.transaction(conn, fn c ->
      p = &Postgrex.query!(c, &1, [])

      p.(~s"""
      INSERT INTO parity_all (id, c_bool, c_int2, c_int4, c_int8, c_float4, c_float8, c_numeric,
             c_text, c_varchar, c_char, c_bytea, c_date, c_time, c_timetz, c_ts, c_tstz,
             c_interval, c_json, c_jsonb, c_uuid, c_money, c_arr_int, c_arr_text,
             c_arr_numeric, c_blob)
         VALUES (1, true, 2, 3, 4, 1.5, 2.25, '1.10',
             'text', 'varchar', 'abc', decode('54617069727573','hex'), '2026-09-29', '12:34:56', '12:34:56-05',
             '2026-09-29 12:00:00', '2026-09-29 12:00:00+00', '1 day 2 hours',
             '{"k": 1}', '{"j": 2}', '00000000-0000-0000-0000-000000000001', '$1,234.56',
             '{1,2,NULL}', '{a,b}', '{1.10,NULL}', repeat('x', 10000))
      """)

      # NULL scalars and extreme-magnitude floating-point values.
      p.("""
      INSERT INTO parity_all (id, c_bool, c_int2, c_int4, c_int8, c_float4, c_float8,
             c_numeric, c_text, c_varchar, c_char, c_bytea, c_date, c_time, c_timetz,
             c_ts, c_tstz, c_interval, c_json, c_jsonb, c_uuid, c_money, c_arr_int,
             c_arr_text, c_arr_numeric, c_blob)
         VALUES (2, NULL, NULL, NULL, NULL, -2.5e-7, 1e20,
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
             NULL, NULL, NULL)
      """)

      # unchanged TOAST: update a non-TOAST column; c_blob stays sentinel
      p.("UPDATE parity_all SET c_text = 'changed' WHERE id = 1")

      # replica identities: DEFAULT (PK), FULL, USING INDEX
      p.("INSERT INTO parity_full VALUES (1, 'a', 'b')")
      p.("UPDATE parity_full SET a = 'a2' WHERE id = 1")
      p.("DELETE FROM parity_full WHERE id = 1")

      p.("INSERT INTO parity_idx VALUES (1, 'a', 'b')")
      p.("UPDATE parity_idx SET b = 'b2' WHERE id = 1")
      p.("DELETE FROM parity_idx WHERE id = 1")

      # The keyless table is configured for insert-only delivery on both legs.
      p.("INSERT INTO parity_nothing VALUES (1, 'a')")
      p.("UPDATE parity_nothing SET a = 'a2' WHERE id = 1")
      p.("DELETE FROM parity_nothing WHERE id = 1")
    end)
  end

  @doc "Normalize a delivery for the byte-identical diff (ADR-0009: LSNs, xids, timestamps)."
  def normalize(txns) do
    Enum.map(txns, fn txn ->
      %{
        changes:
          Enum.map(txn.changes, fn ch ->
            %{
              op: ch.op,
              schema: ch.schema,
              table: ch.table,
              record: ch.record,
              old_record: ch.old_record,
              unchanged: Enum.sort(ch.unchanged || []),
              columns: ch.columns
            }
          end),
        messages:
          Enum.map(txn.messages || [], fn m ->
            %{transactional?: m.transactional?, prefix: m.prefix, content: m.content}
          end)
      }
    end)
  end

  defp ddl_statements do
    ddl()
    |> String.split(";")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  defp ddl do
    """
    CREATE TABLE parity_all (
      id bigint PRIMARY KEY,
      c_bool boolean, c_int2 int2, c_int4 int4, c_int8 int8,
      c_float4 float4, c_float8 float8, c_numeric numeric(10,2),
      c_text text, c_varchar varchar(20), c_char char(3),
      c_bytea bytea, c_date date, c_time time, c_timetz timetz,
      c_ts timestamp, c_tstz timestamptz, c_interval interval,
      c_json json, c_jsonb jsonb, c_uuid uuid, c_money money,
      c_arr_int integer[], c_arr_text text[], c_arr_numeric numeric(3,2)[],
      c_blob text
    );
    CREATE TABLE parity_full (id int PRIMARY KEY, a text, b text);
    ALTER TABLE parity_full REPLICA IDENTITY FULL;
    CREATE TABLE parity_idx (id int PRIMARY KEY, a text, b text);
    ALTER TABLE parity_idx ALTER COLUMN b SET NOT NULL;
    CREATE UNIQUE INDEX parity_idx_b ON parity_idx(b);
    ALTER TABLE parity_idx REPLICA IDENTITY USING INDEX parity_idx_b;
    CREATE TABLE parity_nothing (id int PRIMARY KEY, a text);
    ALTER TABLE parity_nothing REPLICA IDENTITY NOTHING;
    ALTER TABLE parity_all ALTER COLUMN c_blob SET STORAGE EXTERNAL;
    """
  end
end

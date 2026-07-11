defmodule Replicant.QueryBuilderTest do
  use ExUnit.Case, async: true

  alias Replicant.QueryBuilder

  describe "start_replication/3" do
    test "builds the proto-v1 START_REPLICATION command from validated names" do
      {:ok, sql} =
        QueryBuilder.start_replication("orders_slot", "orders_pub", start_lsn: 0x16E3778)

      assert sql =~ "START_REPLICATION SLOT orders_slot LOGICAL 0/16E3778"
      assert sql =~ "proto_version '1'"
      assert sql =~ "publication_names 'orders_pub'"
    end

    test "rejects a hostile slot name with a value-free error, builds nothing" do
      assert {:error, :invalid_identifier} =
               QueryBuilder.start_replication("x; DROP", "ok_pub", [])
    end

    test "rejects a hostile publication name" do
      assert {:error, :invalid_identifier} =
               QueryBuilder.start_replication("ok_slot", "p '--", [])
    end
  end

  describe "start_replication streaming (spec §5)" do
    alias Replicant.QueryBuilder

    test "defaults to proto_version 1 with no streaming clause" do
      assert {:ok, sql} = QueryBuilder.start_replication("s", "p", start_lsn: 0)
      assert sql =~ "proto_version '1'"
      refute sql =~ "streaming"
    end

    test "streaming: true selects proto_version 2 and streaming 'on'" do
      assert {:ok, sql} = QueryBuilder.start_replication("s", "p", start_lsn: 0, streaming: true)
      assert sql =~ "proto_version '2'"
      assert sql =~ "streaming 'on'"
      assert sql =~ "publication_names 'p'"
    end
  end

  describe "create_durable_slot/1 + publication_exists/1 + slot_exists/1" do
    test "validated names produce slot/publication commands" do
      {:ok, a} = QueryBuilder.create_durable_slot("orders_slot")
      assert a =~ "CREATE_REPLICATION_SLOT orders_slot LOGICAL pgoutput NOEXPORT_SNAPSHOT"
      {:ok, b} = QueryBuilder.publication_exists("orders_pub")
      assert b =~ "pg_publication" and b =~ "orders_pub"
      {:ok, c} = QueryBuilder.slot_exists("orders_slot")
      assert c =~ "pg_replication_slots" and c =~ "orders_slot"
    end

    test "invalid names never build a command" do
      assert {:error, :invalid_identifier} = QueryBuilder.create_durable_slot("bad name")
      assert {:error, :invalid_identifier} = QueryBuilder.publication_exists("bad'name")
      assert {:error, :invalid_identifier} = QueryBuilder.slot_exists("x;--")
    end
  end

  describe "slot_invalidation_status/1" do
    test "selects the PG16 invalidation signals (wal_status + conflicting), validated" do
      assert {:ok, sql} = QueryBuilder.slot_invalidation_status("replicant_orders")
      # PG16 real columns (probed against 16.13): wal_status text + conflicting bool.
      # invalidation_reason is PG17+ and must NOT appear (it errors on PG16).
      assert sql =~ "wal_status"
      assert sql =~ "conflicting"
      refute sql =~ "invalidation_reason"
      assert sql =~ "pg_replication_slots"
      assert sql =~ "slot_name = 'replicant_orders'"
    end

    test "rejects an invalid slot name (no raw interpolation into SQL)" do
      assert {:error, :invalid_identifier} =
               QueryBuilder.slot_invalidation_status("orders'; DROP")
    end
  end

  describe "is_in_recovery/0" do
    test "returns the pg_is_in_recovery() probe (no identifier to validate)" do
      assert QueryBuilder.is_in_recovery() == "SELECT pg_is_in_recovery();"
    end
  end

  describe "recovery_and_version/0" do
    test "reads recovery status and the numeric server version in one round trip" do
      sql = QueryBuilder.recovery_and_version()
      assert sql =~ "pg_is_in_recovery()"
      assert sql =~ "current_setting('server_version_num')"
      assert sql =~ "::int"
    end
  end

  describe "create_export_slot/1" do
    test "builds the EXPORT_SNAPSHOT variant from a validated slot name" do
      {:ok, sql} = QueryBuilder.create_export_slot("orders_slot")
      assert sql =~ "CREATE_REPLICATION_SLOT orders_slot LOGICAL pgoutput EXPORT_SNAPSHOT"
      refute sql =~ "NOEXPORT"
    end

    test "rejects a hostile slot name, builds nothing" do
      assert {:error, :invalid_identifier} = QueryBuilder.create_export_slot("x; DROP")
    end
  end

  describe "set_transaction_snapshot/1" do
    test "adopts a real PG exported-snapshot name (uppercase hex + hyphens)" do
      {:ok, sql} = QueryBuilder.set_transaction_snapshot("00000003-0000DD8A-1")
      assert sql == "SET TRANSACTION SNAPSHOT '00000003-0000DD8A-1'"
    end

    test "rejects a name with a quote/whitespace/injection (string-literal guard)" do
      for bad <- ["00000003'; DROP--", "00 00", "abc", "'", "1-2-3; DROP", ""] do
        assert {:error, :invalid_snapshot_name} = QueryBuilder.set_transaction_snapshot(bad)
      end
    end

    test "rejects a non-binary" do
      assert {:error, :invalid_snapshot_name} = QueryBuilder.set_transaction_snapshot(nil)
    end
  end

  describe "publication_tables/1" do
    test "selects schema, table, and PG-quoted qualified name for the validated publication" do
      {:ok, sql} = QueryBuilder.publication_tables("orders_pub")

      assert sql ==
               "SELECT schemaname, tablename, format('%I.%I', schemaname, tablename) AS qualified FROM pg_publication_tables WHERE pubname = 'orders_pub'"
    end

    test "rejects a hostile publication name" do
      assert {:error, :invalid_identifier} = QueryBuilder.publication_tables("p'; DROP")
    end
  end

  describe "checkpoint store builders" do
    test "checkpoint_ensure_table/1 validates the identifier and builds CREATE TABLE IF NOT EXISTS" do
      assert {:ok, sql} = QueryBuilder.checkpoint_ensure_table("replicant_checkpoints")
      assert sql =~ "CREATE TABLE IF NOT EXISTS replicant_checkpoints"
      assert sql =~ "slot_name text PRIMARY KEY"
      assert sql =~ "commit_lsn bigint NOT NULL"

      assert {:error, :invalid_identifier} =
               QueryBuilder.checkpoint_ensure_table("bad; DROP TABLE x")

      assert {:error, :invalid_identifier} = QueryBuilder.checkpoint_ensure_table("Uppercase")
    end

    test "checkpoint_read/1 and checkpoint_upsert/1 interpolate only the validated table; values are $n" do
      assert {:ok, read} = QueryBuilder.checkpoint_read("cp")
      assert read == "SELECT commit_lsn FROM cp WHERE slot_name = $1"
      assert {:ok, up} = QueryBuilder.checkpoint_upsert("cp")

      assert up ==
               "INSERT INTO cp (slot_name, commit_lsn, updated_at) VALUES ($1, $2, now()) " <>
                 "ON CONFLICT (slot_name) DO UPDATE SET commit_lsn = EXCLUDED.commit_lsn, updated_at = now()"

      assert {:error, :invalid_identifier} = QueryBuilder.checkpoint_read("a b")
      assert {:error, :invalid_identifier} = QueryBuilder.checkpoint_upsert("a b")
    end

    test "checkpoint_column_probe/0 binds the table name (no interpolation)" do
      sql = QueryBuilder.checkpoint_column_probe()
      assert sql =~ "SELECT data_type"
      assert sql =~ "FROM information_schema.columns"
      assert sql =~ "table_name = $1"
      assert sql =~ "column_name = 'commit_lsn'"
    end
  end

  describe "pk_columns/0" do
    test "discovers ordered PK columns with server-quoted names, keyed by qualified table" do
      sql = QueryBuilder.pk_columns()
      assert sql =~ "pg_index"
      assert sql =~ "indisprimary"
      assert sql =~ "WITH ORDINALITY"
      assert sql =~ "quote_ident(a.attname)"
      # joins against pg_publication_tables by the bound publication name
      assert sql =~ "pg_publication_tables"
      assert sql =~ "pubname = $1"
      # Per-column TYPE oids now come from table_columns/0 (the full column set covers the
      # PK columns), so pk_columns/0 no longer duplicates a per-PK type array.
      refute sql =~ "atttypid"
    end
  end

  describe "table_columns/0" do
    test "discovers ALL non-dropped columns ordered by attnum with server-quoted names + type oids" do
      sql = QueryBuilder.table_columns()
      assert sql =~ "pg_attribute"
      assert sql =~ "attnum > 0"
      assert sql =~ "NOT a.attisdropped"
      assert sql =~ "ORDER BY a.attnum"
      assert sql =~ "quote_ident(a.attname)"
      assert sql =~ "atttypid"
      assert sql =~ "pg_publication_tables"
      assert sql =~ "pubname = $1"
      # NOT restricted to primary-key columns (that is pk_columns/0's job).
      refute sql =~ "indisprimary"
    end
  end

  describe "keyset_chunk/4" do
    test "first chunk (no bound): every column cast to ::text + RAW PK bound projections, TABLE-QUALIFIED ORDER BY" do
      {:ok, sql} =
        QueryBuilder.keyset_chunk(
          ~s(public."Orders"),
          [~s("id"), ~s("region"), ~s("amount")],
          [~s("id"), ~s("region")],
          0
        )

      assert sql ==
               ~s(SELECT "id"::text AS "id", "region"::text AS "region", "amount"::text AS "amount", ) <>
                 ~s("id" AS __rpk_1, "region" AS __rpk_2 ) <>
                 ~s(FROM public."Orders" ) <>
                 ~s(ORDER BY public."Orders"."id", public."Orders"."region" LIMIT $1)

      # ORDER BY is TABLE-QUALIFIED so it binds the typed int/uuid columns, never the
      # same-named ::text output alias (a bare `ORDER BY "id"` sorts lexicographically and
      # silently skips keyset pages — the marquee-caught convergence loss).
      refute sql =~ ~s(ORDER BY "id",)
    end

    test "subsequent chunk: ROW() comparison with BOUND parameters on the TABLE-QUALIFIED typed PK columns" do
      {:ok, sql} =
        QueryBuilder.keyset_chunk(
          ~s(public."Orders"),
          [~s("id"), ~s("region"), ~s("amount")],
          [~s("id"), ~s("region")],
          2
        )

      assert sql ==
               ~s(SELECT "id"::text AS "id", "region"::text AS "region", "amount"::text AS "amount", ) <>
                 ~s("id" AS __rpk_1, "region" AS __rpk_2 ) <>
                 ~s(FROM public."Orders" ) <>
                 ~s{WHERE (public."Orders"."id", public."Orders"."region") > ($2, $3) } <>
                 ~s(ORDER BY public."Orders"."id", public."Orders"."region" LIMIT $1)

      refute sql =~ "ROW(1"
      # The keyset compares/orders the REAL typed PK columns (type-correct ordering), never
      # the ::text projection alias — a ::text keyset would break numeric/int pagination.
      refute sql =~ ~s|"id"::text) >|
    end

    test "rejects an empty pk list" do
      assert {:error, :invalid_identifier} =
               QueryBuilder.keyset_chunk("public.t", [~s("c")], [], 0)
    end
  end

  describe "keyless_scan/2" do
    test "casts every column to ::text (no bound, no ORDER BY) for the PK-less fallback" do
      sql = QueryBuilder.keyless_scan(~s(public."Log"), [~s("id"), ~s("body")])

      assert sql ==
               ~s(SELECT "id"::text AS "id", "body"::text AS "body" FROM public."Log")
    end
  end

  describe "watermark_lsn/1" do
    test "primary uses pg_current_wal_lsn, standby uses pg_last_wal_replay_lsn" do
      assert QueryBuilder.watermark_lsn(false) == "SELECT pg_current_wal_lsn()::text;"
      assert QueryBuilder.watermark_lsn(true) == "SELECT pg_last_wal_replay_lsn()::text;"
    end
  end

  describe "snapshot progress table builders" do
    test "ensure/read/upsert follow the checkpoint-table shape with a bytea token" do
      assert {:ok, ddl} = QueryBuilder.progress_ensure_table("replicant_snapshot_progress")
      assert ddl =~ "CREATE TABLE IF NOT EXISTS replicant_snapshot_progress"
      assert ddl =~ "slot_name text PRIMARY KEY"
      assert ddl =~ "token bytea NOT NULL"

      assert {:ok, read} = QueryBuilder.progress_read("replicant_snapshot_progress")
      assert read == "SELECT token FROM replicant_snapshot_progress WHERE slot_name = $1"

      assert {:ok, up} = QueryBuilder.progress_upsert("replicant_snapshot_progress")
      assert up =~ "INSERT INTO replicant_snapshot_progress (slot_name, token, updated_at)"
      assert up =~ "ON CONFLICT (slot_name) DO UPDATE SET token = EXCLUDED.token"
    end

    test "table names still pass the identifier allowlist" do
      assert {:error, :invalid_identifier} = QueryBuilder.progress_read(~s(bad"name))
    end
  end
end

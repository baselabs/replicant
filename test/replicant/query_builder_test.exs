defmodule Replicant.QueryBuilderTest do
  use ExUnit.Case, async: true

  alias Replicant.Decoder.{Pglogical, Wal2json}
  alias Replicant.Decoder.PgOutput

  alias Replicant.QueryBuilder

  describe "start_replication/3" do
    test "builds the v1 command for a single-element publication list (byte-unchanged from 0.1.0)" do
      {:ok, sql} =
        QueryBuilder.start_replication("orders_slot", ["orders_pub"], start_lsn: 0x16E3778)

      assert sql ==
               "START_REPLICATION SLOT orders_slot LOGICAL 0/16E3778 " <>
                 "(proto_version '1', publication_names 'orders_pub')"
    end

    test "comma-joins a multi-element publication list into publication_names" do
      {:ok, sql} = QueryBuilder.start_replication("orders_slot", ["p1", "p2"], start_lsn: 0)

      assert sql =~ "publication_names 'p1,p2'"
    end

    test "validates every publication name in the list" do
      assert {:error, :invalid_identifier} =
               QueryBuilder.start_replication("ok_slot", ["ok", "bad'name"], [])

      assert {:error, :invalid_identifier} =
               QueryBuilder.start_replication("ok_slot", [], [])
    end

    test "rejects a hostile slot name" do
      assert {:error, :invalid_identifier} =
               QueryBuilder.start_replication("x; DROP", ["ok_pub"], [])
    end

    # 1.3.0 — a non-integer or negative start_lsn is a tagged error like every other
    # builder failure, not a FunctionClauseError out of lsn_to_string/1.
    test "rejects an invalid start_lsn with {:error, :invalid_start_lsn}" do
      assert {:error, :invalid_start_lsn} =
               QueryBuilder.start_replication("ok_slot", ["ok_pub"], start_lsn: -1)

      assert {:error, :invalid_start_lsn} =
               QueryBuilder.start_replication("ok_slot", ["ok_pub"], start_lsn: "0/0")
    end

    test "messages: true adds the messages 'true' option (A2, after Task 2 carry-forward)" do
      {:ok, sql} = QueryBuilder.start_replication("s", ["p"], start_lsn: 0, messages: true)
      assert sql =~ "messages 'true'"
      assert sql =~ "publication_names 'p'"

      # composition with streaming (the load-bearing combination)
      {:ok, sql2} =
        QueryBuilder.start_replication("s", ["p"], start_lsn: 0, streaming: true, messages: true)

      assert sql2 =~ "proto_version '2', streaming 'on'"
      assert sql2 =~ "messages 'true'"
      # option order: messages comes AFTER publication_names
      [_, after_pub] = String.split(sql2, "publication_names 'p', ", parts: 2)
      assert String.starts_with?(after_pub, "messages 'true'")
    end
  end

  describe "start_replication streaming (spec §5)" do
    alias Replicant.QueryBuilder

    test "defaults to proto_version 1 with no streaming clause" do
      assert {:ok, sql} = QueryBuilder.start_replication("s", ["p"], start_lsn: 0)
      assert sql =~ "proto_version '1'"
      refute sql =~ "streaming"
    end

    test "streaming: true selects proto_version 2 and streaming 'on'" do
      assert {:ok, sql} =
               QueryBuilder.start_replication("s", ["p"], start_lsn: 0, streaming: true)

      assert sql =~ "proto_version '2'"
      assert sql =~ "streaming 'on'"
      assert sql =~ "publication_names 'p'"
    end
  end

  describe "create_durable_slot/2 + publication_exists/1 + slot_exists/1" do
    test "failover?: false is the unchanged legacy NOEXPORT command (published default path)" do
      {:ok, a} = QueryBuilder.create_durable_slot("orders_slot", false)
      assert a =~ "CREATE_REPLICATION_SLOT orders_slot LOGICAL pgoutput NOEXPORT_SNAPSHOT"
      refute a =~ "FAILOVER"
      {:ok, b} = QueryBuilder.publication_exists(["orders_pub"])
      # interpolated single-quoted IN list (the connect-chain simple-query protocol cannot bind
      # $1; Identifier-validated names are interpolated, the slot_invalidation_status precedent).
      assert b =~ "SELECT pubname FROM pg_publication WHERE pubname IN ('orders_pub')"
      {:ok, c} = QueryBuilder.slot_exists("orders_slot")
      assert c =~ "pg_replication_slots" and c =~ "orders_slot"
    end

    test "failover?: true emits the PG17 parenthesized FAILOVER + SNAPSHOT 'nothing' grammar" do
      {:ok, a} = QueryBuilder.create_durable_slot("orders_slot", true)

      assert a =~
               "CREATE_REPLICATION_SLOT orders_slot LOGICAL pgoutput (FAILOVER, SNAPSHOT 'nothing')"

      refute a =~ "NOEXPORT_SNAPSHOT"
    end

    test "invalid names never build a command" do
      assert {:error, :invalid_identifier} = QueryBuilder.create_durable_slot("bad name", false)
      assert {:error, :invalid_identifier} = QueryBuilder.create_durable_slot("bad name", true)
      assert {:error, :invalid_identifier} = QueryBuilder.publication_exists(["bad'name"])
      assert {:error, :invalid_identifier} = QueryBuilder.slot_exists("x;--")
    end
  end

  describe "slot_confirmed_flush/1 (R04 reused-slot origin)" do
    test "reads confirmed_flush_lsn from pg_replication_slots for the validated slot" do
      assert {:ok, sql} = QueryBuilder.slot_confirmed_flush("replicant_orders")
      assert sql =~ "confirmed_flush_lsn"
      assert sql =~ "pg_replication_slots"
      assert sql =~ "slot_name = 'replicant_orders'"
      assert sql =~ "slot_type = 'logical'"
      assert sql =~ "database = current_database()"
    end

    test "rejects an invalid slot name (no raw interpolation into SQL)" do
      assert {:error, :invalid_identifier} = QueryBuilder.slot_confirmed_flush("orders'; DROP")
    end
  end

  describe "slot_invalidation_status/2" do
    test "PG15 (version < 160000) selects ONLY wal_status (conflicting errors on PG15)" do
      # `conflicting` was added in PG16; on PG15 `SELECT ... conflicting` errors
      # `column \"conflicting\" does not exist` (probe-confirmed). PG15's sole invalidation
      # signal is `wal_status = 'lost'`.
      assert {:ok, sql} = QueryBuilder.slot_invalidation_status("replicant_orders", 150_019)
      assert sql =~ "wal_status"
      refute sql =~ "conflicting"
      refute sql =~ "invalidation_reason"
      refute sql =~ "synced"
      assert sql =~ "pg_replication_slots"
      assert sql =~ "slot_name = 'replicant_orders'"
    end

    test "PG16 (160000 <= version < 170000) selects wal_status + conflicting (invalidation_reason errors on PG16)" do
      assert {:ok, sql} = QueryBuilder.slot_invalidation_status("replicant_orders", 160_014)
      assert sql =~ "wal_status"
      assert sql =~ "conflicting"
      refute sql =~ "invalidation_reason"
      refute sql =~ "synced"
      assert sql =~ "pg_replication_slots"
      assert sql =~ "slot_name = 'replicant_orders'"
    end

    test "PG17+ (version >= 170000) additionally selects invalidation_reason + synced" do
      assert {:ok, sql} = QueryBuilder.slot_invalidation_status("replicant_orders", 170_010)
      assert sql =~ "wal_status"
      assert sql =~ "conflicting"
      assert sql =~ "invalidation_reason"
      assert sql =~ "synced"
    end

    test "rejects an invalid slot name (no raw interpolation into SQL)" do
      assert {:error, :invalid_identifier} =
               QueryBuilder.slot_invalidation_status("orders'; DROP", 170_010)
    end
  end

  # ADR-0009 §9 — the below-130000 tier: wal_status (and max_slot_wal_keep_size) arrive
  # in PG 13; on 9.6 to 12 a slot cannot be invalidated by size and the only loss signal
  # is a removed WAL segment (a START_REPLICATION failure → the command-error watchdog),
  # so the query selects NO invalidation column (a constant NULL placeholder keeps the
  # row-present/absent signal the connect chain keys on).
  describe "slot_invalidation_status/2 below-130000 tier (ADR-0009)" do
    test "PG 9.6 to 12 selects no invalidation column" do
      for version <- [90_602, 100_021, 110_017, 120_008, 129_999] do
        assert {:ok, sql} = QueryBuilder.slot_invalidation_status("replicant_orders", version)
        refute sql =~ "wal_status"
        refute sql =~ "conflicting"
        refute sql =~ "invalidation_reason"
        assert sql =~ "SELECT NULL::text FROM pg_replication_slots"
        assert sql =~ "slot_name = 'replicant_orders'"
      end
    end
  end

  # ADR-0009 §9 — on 9.6 the current-LSN function is pg_current_xlog_location (the
  # pg_current_wal_lsn rename landed in PG 10). The watermark reader is version-gated.
  describe "watermark_lsn/3 version gate (ADR-0009)" do
    test "PG 9.6 selects pg_current_xlog_location" do
      assert QueryBuilder.watermark_lsn(false, 90_602) ==
               "SELECT pg_current_xlog_location()::text;"
    end

    test "PG 10+ selects pg_current_wal_lsn (unchanged)" do
      for version <- [100_021, 150_019, 180_002] do
        assert QueryBuilder.watermark_lsn(false, version) == "SELECT pg_current_wal_lsn()::text;"
      end
    end

    test "standby selects the replay LSN on every version" do
      assert QueryBuilder.watermark_lsn(true, 90_602) == "SELECT pg_last_wal_replay_lsn()::text;"
      assert QueryBuilder.watermark_lsn(true, 150_019) == "SELECT pg_last_wal_replay_lsn()::text;"
    end
  end

  # ADR-0009 §2 — slot creation carries the decoder's plugin; the default (pgoutput)
  # emits the published 1.3.0 strings byte-for-byte.
  describe "plugin-parameterized slot creation (ADR-0009)" do
    test "create_durable_slot/3 with pgoutput is byte-identical to the /2 form" do
      assert {:ok, a} = QueryBuilder.create_durable_slot("orders_slot", false, "pgoutput")
      assert {:ok, b} = QueryBuilder.create_durable_slot("orders_slot", false)
      assert a == b
      assert a == "CREATE_REPLICATION_SLOT orders_slot LOGICAL pgoutput NOEXPORT_SNAPSHOT;"
    end

    test "create_durable_slot/3 with the plugin decoders names their output plugin" do
      assert {:ok, a} = QueryBuilder.create_durable_slot("s", false, "pglogical_output")
      assert a == "CREATE_REPLICATION_SLOT s LOGICAL pglogical_output NOEXPORT_SNAPSHOT;"

      assert {:ok, b} = QueryBuilder.create_durable_slot("s", false, "wal2json")
      assert b == "CREATE_REPLICATION_SLOT s LOGICAL wal2json NOEXPORT_SNAPSHOT;"
    end

    test "create_export_slot/3 mirrors the plugin parameter (failover grammar intact)" do
      assert {:ok, a} = QueryBuilder.create_export_slot("s", false, "wal2json")
      assert a == "CREATE_REPLICATION_SLOT s LOGICAL wal2json EXPORT_SNAPSHOT;"

      assert {:ok, b} = QueryBuilder.create_export_slot("s", true, "pgoutput")
      assert b == "CREATE_REPLICATION_SLOT s LOGICAL pgoutput (FAILOVER, SNAPSHOT 'export');"
    end

    test "an invalid plugin name is refused (Critical Rule 2)" do
      assert {:error, :invalid_identifier} =
               QueryBuilder.create_durable_slot("s", false, "pgoutput; DROP")

      assert {:error, :invalid_identifier} = QueryBuilder.create_export_slot("s", false, "")
    end

    test "below PG15 the durable-slot form is bare (NOEXPORT_SNAPSHOT arrives in 15)" do
      assert {:ok, sql} =
               QueryBuilder.create_durable_slot("s", false, "pglogical_output", 120_008)

      assert sql == "CREATE_REPLICATION_SLOT s LOGICAL pglogical_output;"

      assert {:ok, sql96} = QueryBuilder.create_durable_slot("s", false, "wal2json", 90_602)
      assert sql96 == "CREATE_REPLICATION_SLOT s LOGICAL wal2json;"
    end
  end

  # ADR-0009 §2/§3/§4 — START_REPLICATION carries the decoder's option list; every
  # option value was Identifier-validated upstream (Config), and the builder refuses a
  # plugin name or option value carrying a quote breakout before interpolating.
  describe "plugin-parameterized START_REPLICATION (ADR-0009)" do
    test "pgoutput renders the plugin's option list byte-identically to start_replication/3" do
      opts = [publications: ["orders_pub"], streaming: false, messages: false]
      options = PgOutput.start_options(opts)

      assert {:ok, a} = QueryBuilder.start_replication_for("s", "pgoutput", options, 0)
      assert {:ok, b} = QueryBuilder.start_replication("s", ["orders_pub"], start_lsn: 0)
      assert a == b
    end

    test "pgoutput streaming + messages options compose identically too" do
      opts = [publications: ["p"], streaming: true, messages: true]
      options = PgOutput.start_options(opts)

      assert {:ok, a} = QueryBuilder.start_replication_for("s", "pgoutput", options, 0)

      assert {:ok, b} =
               QueryBuilder.start_replication("s", ["p"],
                 start_lsn: 0,
                 streaming: true,
                 messages: true
               )

      assert a == b
    end

    test "pglogical renders the startup negotiation and replication-set list" do
      options = Pglogical.start_options(replication_sets: ["alpha", "beta"])

      assert {:ok, sql} = QueryBuilder.start_replication_for("s", "pglogical_output", options, 0)

      assert sql ==
               "START_REPLICATION SLOT s LOGICAL 0/0 " <>
                 "(startup_params_format '1', min_proto_version '1', max_proto_version '1', " <>
                 ~s(proto_format 'native', "pglogical.replication_set_names" 'alpha,beta') <>
                 ")"
    end

    test "wal2json renders format 2 and the full option set with add-tables" do
      options = Wal2json.start_options(tables: [{"public", "orders"}])

      assert {:ok, sql} = QueryBuilder.start_replication_for("s", "wal2json", options, 0)

      assert sql ==
               "START_REPLICATION SLOT s LOGICAL 0/0 " <>
                 ~s(("format-version" '2', "include-transaction" 'true', "include-lsn" 'true', ) <>
                 ~s("include-xids" 'true', "include-timestamp" 'true', "include-types" 'true', ) <>
                 ~s("include-type-oids" 'true', "include-pk" 'true', ) <>
                 ~s("numeric-data-types-as-string" 'true', ) <>
                 ~s("add-tables" 'public.orders') <>
                 ")"
    end

    test "an option value with a quote breakout is refused, never interpolated" do
      assert {:error, :invalid_identifier} =
               QueryBuilder.start_replication_for(
                 "s",
                 "wal2json",
                 [{"add-tables", "x'; DROP"}],
                 0
               )

      assert {:error, :invalid_identifier} =
               QueryBuilder.start_replication_for("s", "pgoutput; DROP", [], 0)
    end
  end

  # ADR-0009 §2/§5 — per-decoder table-set discovery. pglogical: replication-set
  # existence + set membership in pglogical's own catalog. wal2json/pglogical: the
  # relation-info catalog read (relreplident + columns + type oids) in ONE query.
  describe "per-decoder discovery queries (ADR-0009)" do
    test "replication_set_exists: pglogical.replication_set IN list on the simple protocol" do
      assert {:ok, sql} = QueryBuilder.replication_set_exists(["alpha", "beta"])

      assert sql ==
               "SELECT set_name FROM pglogical.replication_set WHERE set_name IN ('alpha','beta')"
    end

    test "replication_set_exists validates every name" do
      assert {:error, :invalid_identifier} = QueryBuilder.replication_set_exists(["bad'name"])
      assert {:error, :invalid_identifier} = QueryBuilder.replication_set_exists([])
    end

    test "replication_set_tables: pglogical's own membership catalog (OBSERVED columns)" do
      assert {:ok, sql} = QueryBuilder.replication_set_tables(["alpha"])
      assert sql =~ "FROM pglogical.tables"
      assert sql =~ "set_name IN ('alpha')"
      assert sql =~ "format('%I.%I', nspname, relname)"
    end

    test "configured_table_info: relreplident + columns + type oids for the decoder cache" do
      assert {:ok, sql} = QueryBuilder.configured_table_info([{"public", "orders"}])

      assert sql =~ "c.relreplident"
      assert sql =~ "VALUES ('public','orders')"
      assert sql =~ "a.atttypid"
      assert sql =~ "NOT a.attisdropped"
      assert sql =~ "ORDER BY"
    end

    test "configured_table_info validates every schema and table name" do
      assert {:error, :invalid_identifier} =
               QueryBuilder.configured_table_info([{"public", "bad'name"}])
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

  describe "identify_system/0" do
    test "uses the replication protocol command verbatim" do
      assert QueryBuilder.identify_system() == "IDENTIFY_SYSTEM"
    end
  end

  describe "create_export_slot/2" do
    test "failover?: false is the unchanged legacy EXPORT command" do
      {:ok, sql} = QueryBuilder.create_export_slot("orders_slot", false)
      assert sql =~ "CREATE_REPLICATION_SLOT orders_slot LOGICAL pgoutput EXPORT_SNAPSHOT"
      refute sql =~ "FAILOVER"
    end

    test "failover?: true emits the PG17 parenthesized FAILOVER + SNAPSHOT 'export' grammar" do
      {:ok, sql} = QueryBuilder.create_export_slot("orders_slot", true)

      assert sql =~
               "CREATE_REPLICATION_SLOT orders_slot LOGICAL pgoutput (FAILOVER, SNAPSHOT 'export')"

      refute sql =~ "EXPORT_SNAPSHOT"
    end

    test "rejects an invalid slot name" do
      assert {:error, :invalid_identifier} = QueryBuilder.create_export_slot("x; DROP", false)
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
    test "selects DISTINCT schema/table/qualified bound by ANY($1) for the validated publication list" do
      {:ok, sql} = QueryBuilder.publication_tables(["orders_pub"])

      assert sql =~ "pg_publication_tables"
      assert sql =~ "pubname = ANY($1)"
      assert sql =~ "SELECT DISTINCT"
      assert sql =~ "format('%I.%I', schemaname, tablename) AS qualified"
      refute sql =~ "orders_pub"
    end

    test "rejects a list with a hostile publication name" do
      assert {:error, :invalid_identifier} = QueryBuilder.publication_tables(["ok", "p'; DROP"])
    end
  end

  describe "publication_exists/1 (multi-publication existence, spec §5.3)" do
    test "interpolates the validated names into a single-quoted IN list" do
      {:ok, sql} = QueryBuilder.publication_exists(["p1", "p2"])
      # connect-chain simple-query protocol can't bind $1; the Identifier-validated names are
      # interpolated into IN ('p1','p2') — the slot_invalidation_status precedent. Injection-safe
      # because every name passed the [a-z_][a-z0-9_]* allowlist (no quote/paren/semicolon).
      assert sql =~ "SELECT pubname FROM pg_publication WHERE pubname IN ('p1','p2')"
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
      assert sql =~ "pubname = ANY($1)"
      # Per-column TYPE oids now come from table_columns/0 (the full column set covers the
      # PK columns), so pk_columns/0 no longer duplicates a per-PK type array.
      refute sql =~ "atttypid"
      # multi-publication dedup: a table in two publications must NOT duplicate its array_agg
      # entries (decision #19): drive from a (SELECT DISTINCT schemaname, tablename ...) set.
      assert sql =~ "SELECT DISTINCT"
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
      assert sql =~ "pubname = ANY($1)"
      # NOT restricted to primary-key columns (that is pk_columns/0's job).
      refute sql =~ "indisprimary"
      # multi-publication dedup: a table in two publications must NOT duplicate its array_agg
      # entries (decision #19): drive from a (SELECT DISTINCT schemaname, tablename ...) set.
      assert sql =~ "SELECT DISTINCT"
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

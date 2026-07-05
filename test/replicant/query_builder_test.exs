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
      assert sql =~ "pg_publication_tables"
      assert sql =~ "pubname = 'orders_pub'"
      assert sql =~ "format('%I.%I', schemaname, tablename)"
      assert sql =~ "schemaname"
      assert sql =~ "tablename"
    end

    test "rejects a hostile publication name" do
      assert {:error, :invalid_identifier} = QueryBuilder.publication_tables("p'; DROP")
    end
  end
end

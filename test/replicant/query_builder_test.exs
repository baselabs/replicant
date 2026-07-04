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
end

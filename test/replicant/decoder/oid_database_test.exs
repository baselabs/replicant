defmodule Replicant.Decoder.OidDatabaseTest do
  use ExUnit.Case, async: true

  alias Replicant.Decoder.OidDatabase

  # OID constants are part of the Postgres wire/catalog contract (pg_type),
  # documented at https://www.postgresql.org/docs/current/datatype-oid.html.
  describe "name_for_type_id/1" do
    test "maps documented catalog OIDs to type names" do
      assert OidDatabase.name_for_type_id(16) == "bool"
      assert OidDatabase.name_for_type_id(20) == "int8"
      assert OidDatabase.name_for_type_id(23) == "int4"
      assert OidDatabase.name_for_type_id(25) == "text"
      assert OidDatabase.name_for_type_id(700) == "float4"
      assert OidDatabase.name_for_type_id(701) == "float8"
      assert OidDatabase.name_for_type_id(1082) == "date"
      assert OidDatabase.name_for_type_id(1114) == "timestamp"
      assert OidDatabase.name_for_type_id(1184) == "timestamptz"
      assert OidDatabase.name_for_type_id(1700) == "numeric"
      assert OidDatabase.name_for_type_id(2950) == "uuid"
      assert OidDatabase.name_for_type_id(3802) == "jsonb"
    end

    test "unknown OID falls through to the type id itself (decoder stays total)" do
      assert OidDatabase.name_for_type_id(999_999) == 999_999
    end
  end
end

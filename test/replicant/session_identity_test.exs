defmodule Replicant.SessionIdentityTest do
  use ExUnit.Case, async: true

  alias Replicant.SessionIdentity

  describe "from_result/1" do
    test "normalizes the exact IDENTIFY_SYSTEM result shape" do
      result = %Postgrex.Result{
        rows: [["7436598280501831754", "7", "0/16B6C50", "source_db"]]
      }

      assert {:ok,
              %SessionIdentity{
                system_identifier: "7436598280501831754",
                timeline_id: 7,
                current_lsn: 0x16B6C50,
                database: "source_db"
              }} = SessionIdentity.from_result(result)
    end

    test "accepts the full non-negative PostgreSQL int8 timeline range" do
      result = %Postgrex.Result{
        rows: [["7436598280501831754", "9223372036854775807", "0/1", "source_db"]]
      }

      assert {:ok, %SessionIdentity{timeline_id: 9_223_372_036_854_775_807}} =
               SessionIdentity.from_result(result)
    end

    test "rejects every malformed or incomplete result without coercing identity" do
      malformed = [
        %Postgrex.Result{rows: []},
        %Postgrex.Result{rows: [["1", "2", "0/1"]]},
        %Postgrex.Result{rows: [["", "2", "0/1", "db"]]},
        %Postgrex.Result{rows: [["1", "bad", "0/1", "db"]]},
        %Postgrex.Result{rows: [["1", "2", "bad", "db"]]},
        %Postgrex.Result{rows: [["18446744073709551616", "2", "0/1", "db"]]},
        %Postgrex.Result{rows: [["1", "9223372036854775808", "0/1", "db"]]},
        %Postgrex.Result{rows: [["1", "2", "100000000/1", "db"]]},
        %Postgrex.Result{rows: [["1", "2", "0/1", nil]]},
        %Postgrex.Result{rows: [["1", "2", "0/1", "db"], ["2", "3", "0/2", "db"]]}
      ]

      assert Enum.all?(
               malformed,
               &(SessionIdentity.from_result(&1) == {:error, :invalid_session_identity})
             )
    end
  end
end

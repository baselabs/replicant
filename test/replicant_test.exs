defmodule ReplicantTest do
  use ExUnit.Case, async: true

  describe "lsn_to_string/1" do
    test "formats file/offset as uppercase hex" do
      # file=0, offset=0x16E3778
      assert Replicant.lsn_to_string(0x16E3778) == "0/16E3778"
    end

    test "high file dominates a maximal offset (correct LSN ordering)" do
      # (1, 0) must sort AFTER (0, 4294967295): file dominates offset. Exercise the
      # SUT so a wrong bsr/band split is caught, not just an integer-arithmetic fact.
      assert Replicant.lsn_to_string(Bitwise.bsl(1, 32)) == "1/0"
      assert Replicant.lsn_to_string(0xFFFFFFFF) == "0/FFFFFFFF"
      assert Bitwise.bsl(1, 32) > 0xFFFFFFFF
    end
  end
end

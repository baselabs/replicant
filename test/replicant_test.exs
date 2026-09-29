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

  describe "lsn_from_string/1" do
    test "inverts lsn_to_string/1 (pg_lsn display form ↔ uint64)" do
      assert {:ok, 0x16E3778} = Replicant.lsn_from_string("0/16E3778")
      assert {:ok, 0x100000000} = Replicant.lsn_from_string("1/0")
      assert {:ok, 0} = Replicant.lsn_from_string("0/0")

      for lsn <- [0, 0x16E3778, 0x100000000, 0xFFFFFFFFFF, 0xABCDEF12] do
        assert {:ok, ^lsn} = lsn |> Replicant.lsn_to_string() |> Replicant.lsn_from_string()
      end
    end

    # 1.3.0 — a PUBLIC function must never raise with the caller's input embedded in
    # the exception message (a crash report is a log surface; Critical Rule 1). Every
    # invalid shape — including file/offset halves that exceed 32 bits — returns the
    # value-free tagged error instead.
    test "invalid input returns {:error, :invalid_lsn} — never a raise" do
      for bad <- [
            "garbage",
            "",
            "0/",
            "/1",
            "0/ZZ",
            "0/0/0",
            "0/-1",
            "100000000/0",
            "0/100000000"
          ] do
        assert {:error, :invalid_lsn} = Replicant.lsn_from_string(bad)
      end
    end
  end
end

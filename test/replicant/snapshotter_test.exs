defmodule Replicant.SnapshotterTest do
  use ExUnit.Case, async: true

  alias Replicant.Snapshotter

  describe "build_change/5 (spec §6.2, §2 convergence)" do
    test "casts ::text values through the stream's path to a string-keyed %Change{op: :snapshot}" do
      # Values arrive as TEXT (the ::text projection); build_change casts them.
      change =
        Snapshotter.build_change("public", "orders", ["id", "note"], ["int4", "text"], ["1", "hi"])

      assert change.op == :snapshot
      assert change.schema == "public"
      assert change.table == "orders"
      assert change.record == %{"id" => 1, "note" => "hi"}
      assert change.commit_lsn == nil
      assert change.columns == []
      assert Enum.all?(Map.keys(change.record), &is_binary/1)
    end

    test "carries a nil column value through as a real NULL (not dropped)" do
      change =
        Snapshotter.build_change("public", "orders", ["id", "note"], ["int4", "text"], ["1", nil])

      assert change.record == %{"id" => 1, "note" => nil}
    end
  end

  describe "snapshot_error/1 (Critical Rule 1, spec §9)" do
    test "scrubs a Postgrex-style error to a value-free :snapshot_failed, no row value leaks" do
      err = Snapshotter.snapshot_error(%Postgrex.Error{message: "syntax for numeric: SECRET"})
      assert err.reason == :snapshot_failed
      assert err.shape == "Postgrex.Error"
      refute Exception.message(err) =~ "SECRET"
    end

    test "scrubs a non-struct fault to a bare value-free :snapshot_failed" do
      err = Snapshotter.snapshot_error(:some_atom_reason)
      assert %Replicant.Error{reason: :snapshot_failed, shape: nil} = err
    end
  end
end

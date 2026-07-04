defmodule Replicant.ChangeTest do
  use ExUnit.Case, async: true

  alias Replicant.Change

  describe "unchanged TOAST contract (spec §7)" do
    test "unchanged column is listed, NOT placed in record" do
      change = %Change{
        op: :update,
        record: %{"id" => 1, "status" => "paid"},
        unchanged: ["payload"]
      }

      assert "payload" in change.unchanged
      refute Map.has_key?(change.record, "payload")
    end
  end

  describe "struct defaults (spec §7 contract shape)" do
    test "list/count fields default to enumerable zero values, never nil" do
      # A bare Change has empty lists / zero ordinal / nil record so downstream
      # Enum + watermark code stays total. Column.flags defaults to [] (not nil),
      # matching its [atom()] type. (The spec §10 "never String.to_atom" guarantee
      # is a producer invariant, verified end-to-end where keys are actually built:
      # the Assembler, Task 13 — a struct cannot constrain its map key types.)
      change = %Change{op: :insert}
      assert change.unchanged == []
      assert change.columns == []
      assert change.ordinal == 0
      assert change.record == nil
      assert %Change.Column{}.flags == []
    end
  end
end

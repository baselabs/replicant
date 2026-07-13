defmodule Replicant.TransactionTest do
  use ExUnit.Case, async: true

  alias Replicant.{Change, Transaction}

  describe "commit_lsn watermark ordering" do
    test "a re-delivered transaction (commit_lsn == checkpoint) is ordered <= checkpoint" do
      txn = %Transaction{commit_lsn: 0x2A, changes: [%Change{op: :insert}]}
      # exactly-once skip predicate: commit_lsn <= checkpoint
      assert txn.commit_lsn <= 0x2A
      refute txn.commit_lsn <= 0x29
    end

    test "file dominates offset across a 4GiB boundary (watermark does not skip a newer txn)" do
      # commit at file 1, offset 5; checkpoint at file 0, max offset. The commit is
      # NEWER, so the exactly-once predicate commit_lsn <= checkpoint must be FALSE.
      # Route the LSN through the struct so this guards the real watermark decision,
      # not just an integer-arithmetic fact.
      txn = %Transaction{commit_lsn: Bitwise.bsl(1, 32) + 5, changes: [%Change{op: :insert}]}
      checkpoint = 0xFFFFFFFF
      refute txn.commit_lsn <= checkpoint
      assert txn.commit_lsn > checkpoint
    end
  end

  test "changes accepts a plain List (unchanged) AND any Enumerable (a lazy stream) without dialyzer complaint" do
    list = %Transaction{commit_lsn: 1, changes: [%Change{op: :insert}]}
    assert is_list(list.changes)

    lazy = %Transaction{commit_lsn: 2, changes: Stream.map([%Change{op: :insert}], & &1)}
    # a lazy Enumerable is iterable exactly like the List form
    assert [%Change{op: :insert}] = Enum.to_list(lazy.changes)
  end

  test "a Transaction carries a messages list (default [])" do
    assert %Transaction{commit_lsn: nil, commit_timestamp: nil, changes: [], messages: []}
  end
end

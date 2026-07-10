defmodule Replicant.SnapshotWindowTest do
  use ExUnit.Case, async: true

  alias Replicant.SnapshotWindow, as: W

  defp change(table, id),
    do: %Replicant.Change{op: :update, schema: "public", table: table, record: %{"id" => id}}

  # Same logical PK, DIFFERENT non-PK column value — two of these are distinct row
  # IMAGES pre-chunk (keyed on {:record, record}) but collapse to one PK tuple after
  # rebind_pk_raw. Used by the pre-chunk multiplicity tripwire.
  defp change_v(table, id, v),
    do: %Replicant.Change{
      op: :update,
      schema: "public",
      table: table,
      record: %{"id" => id, "v" => v}
    }

  defp chunk(table, ids, hw, opts \\ []) do
    %{
      qualified: "public.#{table}",
      schema: "public",
      table: table,
      pk_raw: ["id"],
      pk_canon: Enum.map(ids, &[&1]),
      changes: Enum.map(ids, &change(table, &1)),
      hw: hw,
      first?: Keyword.get(opts, :first?, false),
      complete?: Keyword.get(opts, :complete?, false),
      progress: Keyword.get(opts, :progress, <<>>),
      bound: Keyword.get(opts, :bound, nil)
    }
  end

  # A PK-less chunk: pk_raw == [] (no keyset drop-set — convergence rests on redo, spec §6.4),
  # so pk_canon is empty too.
  defp keyless_chunk(table, ids, hw),
    do: %{chunk(table, ids, hw) | pk_raw: [], pk_canon: []}

  test "open_window/2 starts tracking; track/2 records same-table PKs; other tables untouched" do
    w = W.new(epoch: 1, drop_cap: 100, max_pending: 4)
    w = W.open_window(w, "public.orders")
    # deliver a chunk first so pk_raw is late-bound onto the tracking entry (the
    # pre-chunk placeholder form is exercised by the tracking-start tripwire below)
    {w, :ok} = W.add_chunk(w, chunk("orders", [], 1))
    w = W.track(w, [change("orders", 7), change("other", 7)])
    assert W.tracked?(w, "public.orders", [7])
    refute W.tracked?(w, "public.orders", [8])
    refute W.tracked?(w, "public.other", [7])
  end

  test "TRIPWIRE tracking-start precedes LW: a PK tracked BEFORE the chunk arrives is dropped at apply" do
    w =
      W.new(epoch: 1, drop_cap: 100, max_pending: 4)
      |> W.open_window("public.orders")
      |> W.track([change("orders", 42)])

    {w, _} = W.add_chunk(w, chunk("orders", [41, 42, 43], 1_000))
    w = W.set_frontier(w, 1, 1_000)
    assert {:apply, kept, _chunk_meta, _w} = W.pop_ready(w)
    assert Enum.map(kept, & &1.record["id"]) == [41, 43]
  end

  test "TRIPWIRE closure: a chunk is NOT ready while frontier < HW, becomes ready at frontier >= HW" do
    w = W.new(epoch: 1, drop_cap: 100, max_pending: 4) |> W.open_window("public.orders")
    {w, _} = W.add_chunk(w, chunk("orders", [1], 5_000))
    w = W.set_frontier(w, 1, 4_999)
    assert :none = W.pop_ready(w)
    w = W.set_frontier(w, 1, 5_000)
    assert {:apply, _, _, _} = W.pop_ready(w)
  end

  test "TRIPWIRE stale-epoch frontier casts cannot close a fresh window (85672f1 class)" do
    w = W.new(epoch: 2, drop_cap: 100, max_pending: 4) |> W.open_window("public.orders")
    {w, _} = W.add_chunk(w, chunk("orders", [1], 5_000))
    w = W.set_frontier(w, 1, 9_999)
    assert :none = W.pop_ready(w)
  end

  test "backpressure: add_chunk reports :at_capacity at max_pending and does NOT append" do
    w = W.new(epoch: 1, drop_cap: 100, max_pending: 1) |> W.open_window("public.orders")
    {w, :ok} = W.add_chunk(w, chunk("orders", [1], 10))
    assert length(w.pending) == 1
    {w2, :at_capacity} = W.add_chunk(w, chunk("orders", [2], 11))
    # regression guard: appending the chunk anyway while still returning :at_capacity goes red
    assert length(w2.pending) == 1
  end

  test "drop-cap breach discards the table's pending chunks and reports re-read" do
    w = W.new(epoch: 1, drop_cap: 2, max_pending: 4) |> W.open_window("public.orders")
    {w, :ok} = W.add_chunk(w, chunk("orders", [1, 2, 3], 10))
    {w, discarded} = W.track_capped(w, Enum.map(1..3, &change("orders", &1)))
    assert discarded == ["public.orders"]
    assert :none = W.pop_ready(W.set_frontier(w, 1, 10))
  end

  test "TRIPWIRE pre-chunk placeholder cap counts distinct row IMAGES; rebind collapses to one PK" do
    # Phase 1 — BEFORE any chunk, PKs are unknown, so each row image is its own tracking
    # entry. Two UPDATEs to the SAME id with DIFFERENT non-PK columns are two entries;
    # against drop_cap 1 that is a multiplicity breach. (RED if the pre-chunk key were
    # wrongly collapsed to the logical PK — size would be 1 and nothing would be discarded.)
    {_w, discarded} =
      W.new(epoch: 1, drop_cap: 1, max_pending: 4)
      |> W.open_window("public.orders")
      |> W.track_capped([change_v("orders", 1, "a"), change_v("orders", 1, "b")])

    assert discarded == ["public.orders"]

    # Phase 2 — once the first chunk supplies pk_raw, rebind_pk_raw normalizes the
    # whole-record placeholders to PK tuples: the SAME id tracked twice collapses to ONE
    # entry. (RED if rebind did NOT normalize — tracked?/3 on [1] would be false, or the
    # extra same-id track would push the set to 3 entries and breach drop_cap 2.)
    w2 =
      W.new(epoch: 1, drop_cap: 2, max_pending: 4)
      |> W.open_window("public.orders")

    {w2, disc0} = W.track_capped(w2, [change_v("orders", 1, "a"), change_v("orders", 1, "b")])
    # two distinct images == drop_cap 2, not yet a breach
    assert disc0 == []

    {w2, :ok} = W.add_chunk(w2, chunk("orders", [], 5))
    # placeholders collapsed onto the canonical PK tuple
    assert W.tracked?(w2, "public.orders", [1])

    # tracking the same id again is now the SAME single entry — no breach against drop_cap 2
    {_w2, disc1} = W.track_capped(w2, [change_v("orders", 1, "c")])
    assert disc1 == []
  end

  test "TRIPWIRE multi-table simultaneous breach discards BOTH tables' pending chunks in one call" do
    w =
      W.new(epoch: 1, drop_cap: 1, max_pending: 4)
      |> W.open_window("public.orders")
      |> W.open_window("public.items")

    {w, :ok} = W.add_chunk(w, chunk("orders", [1, 2], 10))
    {w, :ok} = W.add_chunk(w, chunk("items", [1, 2], 20))

    {w, discarded} =
      W.track_capped(w, [
        change("orders", 1),
        change("orders", 2),
        change("items", 1),
        change("items", 2)
      ])

    # RED if the breach comprehension discarded only one table
    assert Enum.sort(discarded) == ["public.items", "public.orders"]
    # both tables' pending chunks are gone even with the frontier past both HWs
    assert w.pending == []
    assert :none = W.pop_ready(W.set_frontier(w, 1, 100))
  end

  test "reset/2 discards ALL pending chunks + tracking and bumps the epoch (reconnect semantics)" do
    w = W.new(epoch: 1, drop_cap: 100, max_pending: 4) |> W.open_window("public.orders")
    {w, :ok} = W.add_chunk(w, chunk("orders", [1], 10))
    w = W.reset(w, 2)
    assert :none = W.pop_ready(W.set_frontier(w, 2, 999_999))
    refute W.tracked?(w, "public.orders", [1])
    assert w.epoch == 2
  end

  test "close_table/2 stops tracking once a table's chunks are done (memory hygiene)" do
    w =
      W.new(epoch: 1, drop_cap: 100, max_pending: 4)
      |> W.open_window("public.orders")
      |> W.track([change("orders", 1)])
      |> W.close_table("public.orders")

    refute W.tracked?(w, "public.orders", [1])
  end

  test "taint_table/2 discards a tainted table's pending chunk + resets tracking; other tables untouched" do
    w =
      W.new(epoch: 1, drop_cap: 100, max_pending: 4)
      |> W.open_window("public.orders")
      |> W.open_window("public.items")
      |> W.track([change("orders", 7)])

    {w, :ok} = W.add_chunk(w, chunk("orders", [1], 10))
    {w, :ok} = W.add_chunk(w, chunk("items", [2], 20))
    assert length(w.pending) == 2

    w = W.taint_table(w, "public.orders")

    # the tainted table's tracking is reset (drop-set now unknowable → force re-read)
    refute W.tracked?(w, "public.orders", [7])
    # only the tainted table's pending chunk is discarded
    assert Enum.map(w.pending, & &1.qualified) == ["public.items"]

    # frontier past BOTH HWs: only the untouched items chunk pops; orders is gone
    w2 = W.set_frontier(w, 1, 100)
    assert {:apply, _kept, %{qualified: "public.items"}, w3} = W.pop_ready(w2)
    assert :none = W.pop_ready(w3)

    # a no-op for a table that is not being tracked
    assert W.taint_table(w, "public.ghost") == w
  end

  test "frontier also advances from applied commit LSNs (max semantics, monotone)" do
    w = W.new(epoch: 1, drop_cap: 100, max_pending: 4)
    w = W.observe_applied(w, 100)
    w = W.set_frontier(w, 1, 50)
    assert w.frontier == 100
  end

  describe "contention discard signal (spec §4/§6.4)" do
    test "a PK-less table is DISCARDED on ANY tracked write (drop_cap can never fire for keyless)" do
      w =
        W.new(epoch: 1, drop_cap: 100, max_pending: 4)
        |> W.open_window("public.nopk")

      # The first PK-less chunk binds pk_raw == [] onto the tracking entry.
      {w, :ok} = W.add_chunk(w, keyless_chunk("nopk", [], 10))
      refute W.discarded?(w, "public.nopk")

      # ONE tracked write (its PK tuple collapses to []) — never a drop_cap breach — still discards.
      {w, discarded} = W.track_capped(w, [change("nopk", 7)])
      # RED without the keyless-contended arm: discarded == [] and discarded?/2 is false.
      assert discarded == ["public.nopk"]
      assert W.discarded?(w, "public.nopk")
      # its pending chunk is gone (discarded), even with the frontier past its HW
      assert :none = W.pop_ready(W.set_frontier(w, 1, 10))
    end

    test "a PK-less table keeps pk_raw == [] across the discard reset (stays contention-detectable)" do
      w =
        W.new(epoch: 1, drop_cap: 100, max_pending: 4)
        |> W.open_window("public.nopk")

      {w, :ok} = W.add_chunk(w, keyless_chunk("nopk", [], 10))
      {w, _} = W.track_capped(w, [change("nopk", 1)])
      # A SECOND write after the reset is STILL detected as contention (pk_raw stayed []).
      {_w, discarded} = W.track_capped(w, [change("nopk", 2)])
      assert discarded == ["public.nopk"]
    end

    test "a KEYED drop-cap breach is recorded as needs-re-read (discarded?/2), not just returned" do
      w = W.new(epoch: 1, drop_cap: 2, max_pending: 4) |> W.open_window("public.orders")
      {w, :ok} = W.add_chunk(w, chunk("orders", [1, 2, 3], 10))
      {w, discarded} = W.track_capped(w, Enum.map(1..3, &change("orders", &1)))
      assert discarded == ["public.orders"]
      # RED if apply_contention does not fold the breach into w.discarded.
      assert W.discarded?(w, "public.orders")
    end

    test "pop_ready DISCARDS a PK-less chunk whose table saw a write — never drop-filters it to empty" do
      # A placeholder write BEFORE the first keyless chunk binds pk_raw (tracked as {:record, _}).
      w =
        W.new(epoch: 1, drop_cap: 100, max_pending: 4)
        |> W.open_window("public.nopk")
        |> W.track([change("nopk", 1)])

      # add_chunk binds pk_raw == [] and collapses the placeholder → a non-empty tracking set.
      {w, :ok} = W.add_chunk(w, keyless_chunk("nopk", [1, 2, 3], 10))
      w = W.set_frontier(w, 1, 10)

      # RED without the keyless arm: pop_ready returns {:apply, [], _, _} (the WHOLE batch
      # drop-filtered to empty = the confirmed data loss). The fix DISCARDS it whole + flags re-read.
      assert {:discard, %{qualified: "public.nopk"}, w2} = W.pop_ready(w)
      assert W.discarded?(w2, "public.nopk")
    end

    test "pop_ready DISCARDS a chunk whose table is already flagged needs-re-read (fail-closed backstop)" do
      w = W.new(epoch: 1, drop_cap: 100, max_pending: 4) |> W.open_window("public.orders")
      {w, :ok} = W.add_chunk(w, chunk("orders", [1, 2], 10))
      w = %{w | discarded: Map.put(w.discarded, "public.orders", true)}
      w = W.set_frontier(w, 1, 10)
      assert {:discard, %{qualified: "public.orders"}, _w} = W.pop_ready(w)
    end

    test "an UNCONTENDED PK-less chunk (no tracked write) applies WHOLE (drop-set does not empty it)" do
      w = W.new(epoch: 1, drop_cap: 100, max_pending: 4) |> W.open_window("public.nopk")
      {w, :ok} = W.add_chunk(w, keyless_chunk("nopk", [1, 2, 3], 10))
      w = W.set_frontier(w, 1, 10)
      assert {:apply, kept, %{qualified: "public.nopk"}, _w} = W.pop_ready(w)
      assert length(kept) == 3
    end

    test "table_pending?/2 reflects a table's buffered chunks" do
      w = W.new(epoch: 1, drop_cap: 100, max_pending: 4) |> W.open_window("public.orders")
      refute W.table_pending?(w, "public.orders")
      {w, :ok} = W.add_chunk(w, chunk("orders", [1], 10))
      assert W.table_pending?(w, "public.orders")
      refute W.table_pending?(w, "public.other")
    end

    test "clear_discarded/2 removes one table's flag; reset/2 clears ALL (reconnect)" do
      w = W.new(epoch: 1, drop_cap: 2, max_pending: 4) |> W.open_window("public.orders")
      {w, :ok} = W.add_chunk(w, chunk("orders", [1, 2, 3], 10))
      {w, _} = W.track_capped(w, Enum.map(1..3, &change("orders", &1)))
      assert W.discarded?(w, "public.orders")

      refute W.discarded?(W.clear_discarded(w, "public.orders"), "public.orders")
      refute W.discarded?(W.reset(w, 2), "public.orders")
    end
  end
end

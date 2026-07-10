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

  test "frontier also advances from applied commit LSNs (max semantics, monotone)" do
    w = W.new(epoch: 1, drop_cap: 100, max_pending: 4)
    w = W.observe_applied(w, 100)
    w = W.set_frontier(w, 1, 50)
    assert w.frontier == 100
  end
end

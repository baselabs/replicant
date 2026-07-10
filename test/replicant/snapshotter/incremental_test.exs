defmodule Replicant.Snapshotter.IncrementalTest do
  use ExUnit.Case, async: true

  alias Replicant.Casting.Types
  alias Replicant.SnapshotProgress
  alias Replicant.Snapshotter.Incremental, as: Inc

  test "parse_pk_rows/3 builds table_refs (full column list + type names) and sorts PK-less tables LAST" do
    pk_rows = [
      ["public", "orders", "public.orders", ["id"], [~s("id")]]
    ]

    # table_columns/0 rows: every column, quoted names + atttypid (int4 = 23, text = 25).
    col_rows = [
      ["public", "orders", "public.orders", ["id", "note"], [~s("id"), ~s("note")], [23, 25]],
      ["public", "nopk", "public.nopk", ["x"], [~s("x")], [25]]
    ]

    all_rows = [
      ["public", "orders", "public.orders"],
      ["public", "nopk", "public.nopk"]
    ]

    refs = Inc.parse_pk_rows(pk_rows, col_rows, all_rows)

    assert [
             %{
               qualified: "public.orders",
               pk_raw: ["id"],
               col_quoted: [~s("id"), ~s("note")],
               col_type_names: %{"id" => "int4", "note" => "text"}
             },
             %{
               qualified: "public.nopk",
               pk_raw: [],
               col_quoted: [~s("x")],
               col_type_names: %{"x" => "text"}
             }
           ] = refs
  end

  test "build_changes/2 casts each ::text value via col_type_names → op: :snapshot, string keys, nil commit_lsn" do
    table = %{
      schema: "public",
      table: "orders",
      col_type_names: %{"id" => "int4", "note" => "text"}
    }

    # Values arrive as TEXT (the ::text projection); build_changes casts them.
    [c] = Inc.build_changes(table, {["id", "note"], [["7", "x"]]})

    assert %Replicant.Change{op: :snapshot, record: %{"id" => 7, "note" => "x"}, commit_lsn: nil} =
             c
  end

  test "build_changes/2 keeps a NULL column nil (mirrors the stream's cast_value(nil,_))" do
    table = %{schema: "s", table: "t", col_type_names: %{"id" => "int4", "note" => "text"}}
    [c] = Inc.build_changes(table, {["id", "note"], [["7", nil]]})
    assert c.record == %{"id" => 7, "note" => nil}
  end

  test "split_changes/3 casts ALL columns, derives cast pk_canon, and returns the RAW keyset bound" do
    table = %{
      schema: "public",
      table: "t",
      qualified: "public.t",
      pk_raw: ["id"],
      pk_quoted: [~s("id")],
      col_quoted: [~s("id"), ~s("note")],
      col_type_names: %{"id" => "int4", "note" => "text"}
    }

    # Real columns arrive as ::text ("7","x"); __rpk_1 is the RAW native PK (int stays int).
    cols = ["id", "note", "__rpk_1"]
    rows = [["7", "x", 7]]

    {[change], [canon], last_bound} = Inc.split_changes(table, cols, rows)

    # EVERY delivered column is cast to its Elixir type (text "7" → 7).
    assert change.record == %{"id" => 7, "note" => "x"}
    # pk_canon is derived from the CAST record — the SAME term the stream tracks.
    assert canon == [Types.cast_record("7", "int4")]
    assert canon == [7]
    # The keyset resume bound is the RAW native PK (bind-compatible), from __rpk_*.
    assert last_bound == [7]
  end

  test "split_changes/3 delivers a uuid as the stream-IDENTICAL dashed STRING; bound stays raw (convergence fix; RED under old Postgrex-* path)" do
    uuid_text = "550e8400-e29b-41d4-a716-446655440000"

    raw16 =
      <<0x55, 0x0E, 0x84, 0x00, 0xE2, 0x9B, 0x41, 0xD4, 0xA7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
        0x00>>

    table = %{
      schema: "public",
      table: "t",
      qualified: "public.t",
      pk_raw: ["id"],
      pk_quoted: [~s("id")],
      col_quoted: [~s("id")],
      col_type_names: %{"id" => "uuid"}
    }

    # id::text AS id → the dashed string; __rpk_1 → the RAW 16-byte uuid (for keyset binding).
    cols = ["id", "__rpk_1"]
    rows = [[uuid_text, raw16]]

    {[change], [canon], last_bound} = Inc.split_changes(table, cols, rows)

    # THE FIX: the delivered record's uuid is the dashed STRING — byte-identical to what the
    # STREAM delivers (assembler cast_value → Types.cast_record(pgoutput_text, "uuid")).
    assert change.record["id"] == uuid_text
    assert change.record["id"] == Types.cast_record(uuid_text, "uuid")
    # RED discriminator: the OLD Postgrex-`*` path delivered THIS raw 16-byte binary here, so
    # a state-mirror sink keyed by PK would see two different keys → NO convergence (spec §2).
    refute change.record["id"] == raw16
    # pk_canon matches the stream's tracked PK (the cast dashed string).
    assert canon == [uuid_text]
    # The keyset bound stays the RAW 16-byte binary — the ONLY form that binds to a `uuid`
    # WHERE column (a dashed string raises Postgrex EncodeError, proven live).
    assert last_bound == [raw16]
  end

  test "split_changes/3 RAISES on an unexpected __rpk_ column count (F1 fail-closed prefix collision)" do
    # A single-PK table: pk_raw has ONE entry, so the keyset SQL emits ONE __rpk_N.
    table = %{
      schema: "public",
      table: "t",
      qualified: "public.t",
      pk_raw: ["id"],
      pk_quoted: [~s("id")],
      col_quoted: [~s("id"), ~s("note")],
      col_type_names: %{"id" => "int4", "note" => "text"}
    }

    # A user column literally named `__rpk_9` injects a SECOND __rpk_ column. Without the
    # guard it is misclassified into rpk_cols, shortens n_real, misaligns Enum.split, and
    # silently mis-builds the drop-set — a WRONG result, NO raise. The guard fails closed
    # (n_rpk = 2 ≠ pk arity 1).
    cols = ["id", "note", "__rpk_9", "__rpk_1"]
    rows = [["7", "x", "99", "7"]]

    assert_raise RuntimeError, ~r/rpk/, fn -> Inc.split_changes(table, cols, rows) end
  end

  test "reconcile_resume/2 sources identifiers from FRESH discovery, never the token (pk_quoted injection defense)" do
    # Freshly re-discovered metadata: pk_quoted/qualified come from server quote_ident.
    fresh = SnapshotProgress.new([fresh_ref("orders")], 0)

    # An attacker-persisted token: same table by qualified NAME, but its pk_quoted/pk_raw
    # carry a SQL-injection payload and it asks to resume "orders" at bound [5].
    evil = ~s("id"; DROP TABLE users; --)

    tampered = %SnapshotProgress{
      floor_lsn: 0,
      pending: [],
      current: %{
        schema: "public",
        table: "orders",
        qualified: "public.orders",
        pk_raw: [evil],
        pk_quoted: [evil]
      },
      bound: [5],
      done: [],
      complete?: false
    }

    reconciled = Inc.reconcile_resume(fresh, tampered)
    {:table, table, bound, _sp} = SnapshotProgress.next(reconciled)

    # Every identifier that reaches keyset SQL comes from FRESH discovery, never the token.
    assert table.qualified == "public.orders"
    assert table.pk_quoted == [~s("id")]
    assert table.pk_raw == ["id"]
    refute Enum.any?(table.pk_quoted, &String.contains?(&1, "DROP"))
    # The token still supplies the resume POSITION (an arity-checked bind-param bound).
    assert bound == [5]
  end

  test "reconcile_resume/2 drops a token bound whose arity mismatches the fresh PK (fail-closed)" do
    fresh = SnapshotProgress.new([fresh_ref("orders")], 0)

    tampered = %SnapshotProgress{
      floor_lsn: 0,
      pending: [],
      current: %{
        schema: "public",
        table: "orders",
        qualified: "public.orders",
        pk_raw: ["id"],
        pk_quoted: [~s("id")]
      },
      # 2-element bound against a 1-column PK: malformed → re-read from the start.
      bound: [5, 9],
      done: [],
      complete?: false
    }

    reconciled = Inc.reconcile_resume(fresh, tampered)
    {:table, _table, bound, _sp} = SnapshotProgress.next(reconciled)
    assert bound == nil
  end

  test "reconcile_resume/2 (multi-table) preserves the done set + matches current + queues the rest (F3.1)" do
    fresh =
      SnapshotProgress.new(
        [fresh_ref("a"), fresh_ref("b"), fresh_ref("c"), fresh_ref("d")],
        0
      )

    # a,b done; c in-progress at [5]; d still pending. The token's `current` carries an
    # injection payload in pk_quoted — it MUST be ignored (identifiers come from fresh).
    token = %SnapshotProgress{
      floor_lsn: 0,
      pending: [],
      done: ["public.a", "public.b"],
      current: %{
        schema: "public",
        table: "c",
        qualified: "public.c",
        pk_raw: [~s("id"; DROP TABLE users; --)],
        pk_quoted: [~s("id"; DROP TABLE users; --)]
      },
      bound: [5],
      complete?: false
    }

    r = Inc.reconcile_resume(fresh, token)

    # Done set preserved by qualified NAME, in fresh order.
    assert r.done == ["public.a", "public.b"]
    # Current matched by name; identifiers come from FRESH, never the token.
    assert r.current.qualified == "public.c"
    assert r.current.pk_quoted == [~s("id")]
    refute Enum.any?(r.current.pk_quoted, &String.contains?(&1, "DROP"))
    # The arity-checked bound rides through as a bind-param position.
    assert r.bound == [5]
    # d remains queued and will be backfilled from FRESH metadata.
    assert Enum.map(r.pending, & &1.qualified) == ["public.d"]
    refute r.complete?
  end

  test "reconcile_resume/2 silently removes token tables ABSENT from fresh (dropped) + resumes from queue head (F3.2)" do
    fresh = SnapshotProgress.new([fresh_ref("a"), fresh_ref("b")], 0)

    # Token references two tables no longer in fresh discovery: x_dropped (in done) and
    # c_dropped (current). Neither exists in the re-discovered publication.
    token = %SnapshotProgress{
      floor_lsn: 0,
      pending: [],
      done: ["public.a", "public.x_dropped"],
      current: %{
        schema: "public",
        table: "c_dropped",
        qualified: "public.c_dropped",
        pk_raw: ["id"],
        pk_quoted: [~s("id")]
      },
      bound: [5],
      complete?: false
    }

    r = Inc.reconcile_resume(fresh, token)

    # Dropped tables vanish: x_dropped drops out of done, c_dropped is not adopted as current.
    assert r.done == ["public.a"]
    assert r.current == nil
    assert r.bound == nil
    # No crash; resume proceeds from the queue head (b) with FRESH metadata.
    assert {:table, table, nil, _sp} = SnapshotProgress.next(r)
    assert table.qualified == "public.b"
    assert table.pk_quoted == [~s("id")]
  end

  test "reconcile_resume/2 lands a NEW fresh table (added since the token) in pending so it IS backfilled (F3.3)" do
    fresh = SnapshotProgress.new([fresh_ref("a"), fresh_ref("b"), fresh_ref("c")], 0)

    # Token knows only a (done) and b (current); c was ADDED to the publication since.
    token = %SnapshotProgress{
      floor_lsn: 0,
      pending: [],
      done: ["public.a"],
      current: %{
        schema: "public",
        table: "b",
        qualified: "public.b",
        pk_raw: ["id"],
        pk_quoted: [~s("id")]
      },
      bound: [5],
      complete?: false
    }

    r = Inc.reconcile_resume(fresh, token)

    # c lands in pending with FRESH metadata (never token-sourced).
    assert Enum.map(r.pending, & &1.qualified) == ["public.c"]
    assert hd(r.pending).pk_quoted == [~s("id")]
    # Prove it is actually backfilled: after b finishes, the loop yields c as the next unit.
    {:table, next_table, nil, _sp} =
      r |> SnapshotProgress.finish_table() |> SnapshotProgress.next()

    assert next_table.qualified == "public.c"
  end

  test "reconcile_resume/2 with a complete? token is terminal + marks ALL fresh tables done (F3.4/F4)" do
    fresh = SnapshotProgress.new([fresh_ref("a"), fresh_ref("b")], 0)
    token = %SnapshotProgress{floor_lsn: 0, complete?: true}

    r = Inc.reconcile_resume(fresh, token)

    assert r.complete?
    assert r.current == nil
    assert r.bound == nil
    assert r.pending == []
    # ALL freshly-discovered tables are marked done — a table added post-completion is NOT
    # historically backfilled (F4 terminality; operator remedy = fresh re-snapshot).
    assert r.done == ["public.a", "public.b"]
    # The reconciled token is terminal.
    assert SnapshotProgress.next(r) == :complete
  end

  test "value-free boundary: reader faults scrub to :snapshot_failed shape-only" do
    e = Inc.reader_error(%Postgrex.Error{message: "secret 42"})
    assert %Replicant.Error{reason: :snapshot_failed, shape: "Postgrex.Error"} = e
    refute inspect(e) =~ "42"
  end

  test "keyless_batch_progress/1 carries the IN-PROGRESS token, NOT finish_table (data-gap fix)" do
    # A token with ONE in-progress keyless table: current set, no bound, nothing done.
    table = %{
      schema: "public",
      table: "nopk",
      qualified: "public.nopk",
      pk_raw: [],
      pk_quoted: []
    }

    sp = %SnapshotProgress{
      floor_lsn: 42,
      pending: [],
      current: table,
      bound: nil,
      done: [],
      complete?: false
    }

    # The token EVERY provisional keyless batch delivers (and lib mode persists per applied
    # chunk). Keyless batches apply independently by HW, so if this marked the table done,
    # batch-1's durable persist would resume-skip batch-2..N = DATA GAP.
    {:ok, batch_token} = SnapshotProgress.decode(Inc.keyless_batch_progress(sp))

    # Still CURRENT, NOT done: a crash after this batch persists resumes with the WHOLE
    # table re-read (effect-once), never skipped.
    assert batch_token.current == table
    assert batch_token.done == []
    refute batch_token.complete?

    # RED discriminator — the loop-advance token (finish_table) DOES mark the table done.
    # The OLD bug delivered THIS token on every batch, so the assertions above would FAIL
    # (batch_token.current would be nil and batch_token.done would be ["public.nopk"]).
    {:ok, advance_token} =
      SnapshotProgress.decode(SnapshotProgress.encode(SnapshotProgress.finish_table(sp)))

    assert advance_token.current == nil
    assert advance_token.done == ["public.nopk"]
    # The two tokens are genuinely different — the batch token is not just finish_table
    # by another name.
    refute Inc.keyless_batch_progress(sp) ==
             SnapshotProgress.encode(SnapshotProgress.finish_table(sp))
  end

  # A freshly re-discovered keyed table_ref (server-quoted identifiers, single int4 PK), as
  # `discover/1` builds them — carries the enriched `col_quoted`/`col_type_names`, which the
  # SnapshotProgress table_ref shape does not. Used to prove reconcile_resume/2 sources
  # identifiers from FRESH only (the col enrichment is never token-trusted).
  defp fresh_ref(name) do
    %{
      schema: "public",
      table: name,
      qualified: "public.#{name}",
      pk_raw: ["id"],
      pk_quoted: [~s("id")],
      col_quoted: [~s("id")],
      col_type_names: %{"id" => "int4"}
    }
  end
end

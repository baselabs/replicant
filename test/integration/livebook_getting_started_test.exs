defmodule Replicant.Integration.LivebookGettingStartedTest do
  @moduledoc """
  Executes `notebooks/getting_started.livemd` end to end against a LIVE PG16 so the notebook
  can never silently rot: this test IS the notebook's CI (it rides `mix test`, which runs on the
  PG16 + PG17 matrix — see `.github/workflows/ci.yml`).

  It parses the `.livemd`, extracts its ` ```elixir ` code cells, drops the `Mix.install/1` setup
  cell (deps are already loaded in the test VM), concatenates the rest, and evaluates them with
  `Code.eval_string/3`. Presentation (`Kino.*`) is a headless stub (`test/support/kino_stub.ex`);
  the load-bearing CDC logic runs against the real `Replicant` pipeline + Postgres. The notebook's
  final cell returns a summary map, and this test asserts on the OBSERVED CDC values it carries —
  so a green run means the demonstrated behavior actually happened, not merely that no cell raised.

  Non-vacuity guards: a minimum runnable-cell floor (catches a moved/renamed notebook or a parser
  miss that would otherwise "pass" with zero cells) plus concrete value assertions on the summary.
  """
  use ExUnit.Case, async: false
  @moduletag :integration
  # The notebook starts three live pipelines (core / snapshot / messages), each with real async
  # WAL round-trips; match the streaming/messages suite ceiling rather than the 60s default.
  @moduletag timeout: 120_000

  alias Replicant.Test.PG16

  @notebook Path.expand("../../notebooks/getting_started.livemd", __DIR__)

  # The notebook's fixed demo slots (fixed, not per-run, so setup/teardown can enumerate + drop
  # them — the notebook's own setup is likewise self-healing across re-runs).
  @demo_slots ~w(replicant_lb_core replicant_lb_snapshot replicant_lb_messages)
  @demo_publications ~w(lb_pub lb_snapshot_pub lb_msg_pub)

  # A floor, NOT an exact count — its job is to fail RED if the extractor finds ~nothing (moved
  # file / broken fence), never to pin the notebook's exact structure. The real correctness signal
  # is the summary-value assertions below.
  @min_runnable_cells 12

  setup do
    if PG16.enabled?() do
      on_exit(fn ->
        Enum.each(@demo_slots, &Replicant.stop/1)
        {:ok, c} = Postgrex.start_link(PG16.pg_opts())
        Enum.each(@demo_slots, &drop_slot(c, &1))

        Enum.each(@demo_publications, fn pub ->
          Postgrex.query(c, "DROP PUBLICATION IF EXISTS #{pub}", [])
        end)

        GenServer.stop(c)
      end)
    end

    :ok
  end

  test "the getting_started notebook's code cells run green against live PG16" do
    if PG16.enabled?() do
      source = File.read!(@notebook)
      cells = extract_elixir_cells(source)

      # The setup cell (the only one carrying Mix.install/1) re-resolves deps; drop it — the test
      # VM already has replicant + postgrex, and Kino resolves to the headless stub.
      runnable = Enum.reject(cells, &String.contains?(&1, "Mix.install"))

      # Gate integrity: EXACTLY one cell (the setup cell) may be dropped. A stray future
      # `Mix.install` in another cell would silently skip that cell from the tested path.
      assert length(cells) - length(runnable) == 1,
             "expected exactly one Mix.install (setup) cell to be dropped; " <>
               "dropped #{length(cells) - length(runnable)} — a non-setup cell contains Mix.install"

      assert length(runnable) >= @min_runnable_cells,
             "extracted only #{length(runnable)} runnable elixir cells from #{@notebook} " <>
               "(floor #{@min_runnable_cells}) — the notebook moved, or the ```elixir fences changed"

      # Concatenate + evaluate as one top-to-bottom script (aliases/imports/bindings flow exactly
      # as Livebook runs the cells in order). The final cell's value is the summary map.
      script = Enum.join(runnable, "\n\n")
      {summary, _binding} = Code.eval_string(script, [], file: @notebook)

      assert is_map(summary),
             "the notebook's final cell must return a summary map; got: #{inspect(summary)}"

      # --- Observed CDC behavior (the notebook actually did these against live Postgres) ---

      # Core CRUD: inserted ids 1,2,3; updated 1; deleted 2; TOAST demo added id 10 → mirror {1,3,10}.
      assert summary.core_final_ids == [1, 3, 10],
             "core mirror should be [1, 3, 10] after insert/update/delete + TOAST insert; " <>
               "got #{inspect(summary.core_final_ids)}"

      # Effect-once: every applied transaction landed exactly once (append-only ledger, no PK).
      assert summary.core_dup_count == 0,
             "a transaction was delivered/applied more than once (dup=#{summary.core_dup_count})"

      # TOAST sentinel: an UPDATE that didn't touch the large `memo` column surfaces it as unchanged.
      assert summary.toast_unchanged == ["memo"],
             "expected the unchanged TOASTed column [\"memo\"]; got #{inspect(summary.toast_unchanged)}"

      # Snapshot/backfill: 5 pre-seeded rows reached the mirror through handle_snapshot/2.
      assert summary.snapshot_mirror_count == 5,
             "snapshot backfill should mirror 5 seeded rows; got #{summary.snapshot_mirror_count}"

      # Logical-decoding message: a non-transactional message reached handle_message/2.
      assert summary.nontxn_message == {"lb_heartbeat", "tick"},
             "expected the non-txn message {\"lb_heartbeat\", \"tick\"}; got #{inspect(summary.nontxn_message)}"
    end
  end

  # ---- helpers ----

  # Extract the bodies of ```elixir fenced code cells, in document order. A line-scan (not a regex)
  # so nested fences / indentation never confuse it; matches the Livebook `.livemd` serialization.
  defp extract_elixir_cells(source) do
    {cells, _open} =
      source
      |> String.split("\n")
      |> Enum.reduce({[], nil}, fn line, {cells, open} ->
        trimmed = String.trim_trailing(line)

        cond do
          is_nil(open) and trimmed == "```elixir" -> {cells, []}
          not is_nil(open) and trimmed == "```" -> {[join_cell(open) | cells], nil}
          not is_nil(open) -> {cells, [line | open]}
          true -> {cells, open}
        end
      end)

    Enum.reverse(cells)
  end

  defp join_cell(reversed_lines), do: reversed_lines |> Enum.reverse() |> Enum.join("\n")

  defp drop_slot(conn, slot, tries \\ 20)

  defp drop_slot(conn, slot, 0) do
    Postgrex.query!(
      conn,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  end

  defp drop_slot(conn, slot, tries) do
    Postgrex.query!(
      conn,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  rescue
    _ ->
      Process.sleep(50)
      drop_slot(conn, slot, tries - 1)
  end
end

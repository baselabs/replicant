defmodule Replicant.CommandErrorTest do
  # A6 — replication-command-error watchdog, live proof against real PG16.
  # async: false — it exhausts the shared server's replication-slot capacity, so it must
  # never run beside another slot-creating test.
  use ExUnit.Case, async: false
  @moduletag :integration

  alias Replicant.Test.PG16

  # A minimal, process-free sink: the pipeline never streams (create_slot fails first), so
  # handle_transaction is never called; checkpoint/0 returns a fixed value so no Agent setup
  # is needed. go_forward_only: true (below) bypasses the empty-checkpoint start refusal so the
  # chain actually reaches create_slot.
  defmodule A6Sink do
    @behaviour Replicant.Sink
    @impl true
    def checkpoint, do: {:ok, nil}
    @impl true
    def handle_transaction(_txn), do: {:ok, 0}
  end

  # A control connection to provision the publication and exhaust the slot table. Fillers are
  # PERMANENT (not temporary): an unconsumed logical slot is never `active`, so it drops cleanly
  # from any connection with no session-death race — unlike a temporary slot, which reads
  # `active` for its owning session and cannot be dropped cross-connection. Crash-leak is
  # mitigated by the clearly-named `a6_filler_%` prefix + the pre-clean at setup start (below)
  # + the on_exit sweep.
  setup do
    {:ok, ctrl} =
      Postgrex.start_link(PG16.pg_opts() ++ [name: Replicant.Test.A6Ctrl, pool_size: 1])

    drop_fillers(ctrl)
    Postgrex.query!(ctrl, "DROP PUBLICATION IF EXISTS a6_pub", [])
    Postgrex.query!(ctrl, "DROP TABLE IF EXISTS a6_orders", [])
    Postgrex.query!(ctrl, "CREATE TABLE a6_orders (id bigint primary key)", [])
    Postgrex.query!(ctrl, "CREATE PUBLICATION a6_pub FOR TABLE a6_orders", [])

    slot = "rep_a6_#{System.unique_integer([:positive])}"
    drop_slot(ctrl, slot)

    on_exit(fn ->
      Replicant.stop(slot)
      # Permanent fillers are droppable from any connection (never active) — sweep them by name.
      {:ok, c} = Postgrex.start_link(PG16.pg_opts())
      drop_slot(c, slot)
      drop_fillers(c)
      GenServer.stop(c)
    end)

    %{ctrl: ctrl, slot: slot}
  end

  # Fill EVERY free replication slot on the shared server so the pipeline's own create_slot
  # step fails persistently with a %Postgrex.Error{} — the exact pre-frame command-error the
  # watchdog bounds. Fillers are temporary (session-scoped to `ctrl`) AND dropped by name in
  # teardown.
  defp exhaust_slots!(ctrl) do
    max = int1(ctrl, "SELECT current_setting('max_replication_slots')::int")
    used = int1(ctrl, "SELECT count(*) FROM pg_replication_slots")

    for n <- 1..(max - used)//1 do
      Postgrex.query!(
        ctrl,
        "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
        ["a6_filler_#{n}"]
      )
    end

    assert int1(ctrl, "SELECT count(*) FROM pg_replication_slots") == max
  end

  test "a persistent create-slot error halts after exactly max_command_retries+1 attempts", %{
    ctrl: ctrl,
    slot: slot
  } do
    if PG16.enabled?() do
      exhaust_slots!(ctrl)
      attach_halt(self())

      {:ok, _} =
        Replicant.start_link(
          connection: PG16.pg_opts(),
          slot_name: slot,
          publication: "a6_pub",
          sink: __MODULE__.A6Sink,
          go_forward_only: true,
          max_command_retries: 2
        )

      # Halt fires at attempt = budget + 1 (2 recovery attempts, halt on the 3rd fault),
      # value-free (slot_name + ints only).
      assert_receive {:cmd_halt, %{attempt: 3, max_retries: 2, slot_name: ^slot}}, 15_000

      # Stay-idle halt: the pipeline stops, does NOT keep reconnecting.
      PG16.wait_until(
        fn -> Registry.lookup(Replicant.Registry, {slot, :pipeline}) == [] end,
        400
      )
    end
  end

  test "the halt attempt count SCALES with the budget (budget-gated, not unconditional)", %{
    ctrl: ctrl,
    slot: slot
  } do
    if PG16.enabled?() do
      exhaust_slots!(ctrl)
      attach_halt(self())

      {:ok, _} =
        Replicant.start_link(
          connection: PG16.pg_opts(),
          slot_name: slot,
          publication: "a6_pub",
          sink: __MODULE__.A6Sink,
          go_forward_only: true,
          max_command_retries: 5
        )

      # budget 5 → halt at attempt 6 (vs budget 2 → attempt 3 in the sibling test): the halt
      # is gated on the budget, not a fixed unconditional halt and not a livelock.
      assert_receive {:cmd_halt, %{attempt: 6, max_retries: 5, slot_name: ^slot}}, 20_000
    end
  end

  defp attach_halt(pid) do
    ref = "a6-cmd-halt-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      ref,
      [:replicant, :connection, :command_error_halt],
      fn _e, _meas, meta, _ -> send(pid, {:cmd_halt, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(ref) end)
  end

  defp int1(conn, sql), do: Postgrex.query!(conn, sql, []).rows |> hd() |> hd()

  defp drop_slot(conn, slot) do
    Postgrex.query!(
      conn,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
      [slot]
    )
  end

  defp drop_fillers(conn) do
    Postgrex.query!(
      conn,
      "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'a6_filler_%'",
      []
    )
  end
end

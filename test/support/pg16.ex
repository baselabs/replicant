defmodule Replicant.Test.PG16 do
  @moduledoc "Integration-substrate helpers: connection opts from REPLICANT_TEST_URL + enablement."

  @doc "True when a live PG16 (wal_level=logical) is configured via REPLICANT_TEST_URL."
  def enabled?, do: System.get_env("REPLICANT_TEST_URL") not in [nil, ""]

  @doc "Postgrex connection keyword opts parsed from REPLICANT_TEST_URL."
  def pg_opts do
    uri = URI.parse(System.fetch_env!("REPLICANT_TEST_URL"))
    {user, pass} = userinfo(uri.userinfo)

    base = [
      hostname: uri.host || "localhost",
      port: uri.port || 5432,
      database: String.trim_leading(uri.path || "/postgres", "/"),
      username: user
    ]

    if pass, do: base ++ [password: pass], else: base
  end

  @doc """
  Start a NAMED Postgrex pool for an integration test, with per-test cleanup.

  ExUnit runs an `async: false` module's tests in ONE long-lived process. A bare
  `Postgrex.start_link(name: X)` therefore returns `{:error, {:already_started, _}}` on the
  SECOND test's setup (the first test's pool is still registered), cascading every later
  test to a setup failure — masking the module's real coverage. Two wrong fixes exist:
  reusing one shared pool across tests (corrupts under crash-injection — a killed sibling
  connection makes the next checkout `(EXIT) shutdown`), or stopping the pool from the
  test's own `on_exit` (the pool's `:shutdown` exit propagates and fails the callback).

  The correct fix is **per-test isolation with helper-managed cleanup**: start the pool
  UNLINKED from the test process (so a test-process crash cannot double-own it, and its
  `:shutdown` exit cannot reach the test), and register an `on_exit` HERE that stops it
  after the test. Because the registered `on_exit` runs after each test (LIFO, before the
  test's own pipeline-stop `on_exit` only if registered later — but either order is safe:
  the pool is independent of the pipeline), the next test's setup always finds the name
  free and starts a FRESH pool — no cascade, no shared state. The `whereis` reuse branch is
  a defensive fallback for the rare case a prior `on_exit` did not run.

  `opts` carries the remaining pool knobs (e.g. `pool_size: 5`).
  """
  @spec named_conn(atom(), keyword()) :: {:ok, pid()}
  def named_conn(name, opts \\ []) do
    case Process.whereis(name) do
      nil ->
        {:ok, pid} = Postgrex.start_link(pg_opts() ++ [name: name] ++ opts)
        # Unlink so the pool survives a test-process crash (its on_exit below stops it)
        # and its :shutdown exit never reaches the test process.
        Process.unlink(pid)
        ExUnit.Callbacks.on_exit({__MODULE__, :named_conn, name}, fn -> stop_conn(pid) end)
        {:ok, pid}

      pid ->
        {:ok, pid}
    end
  end

  @doc false
  # Stop a named pool, swallowing the :shutdown exit a DBConnection pool raises from its
  # terminate callback (the pool IS stopped; the exit is benign).
  def stop_conn(pid) do
    if Process.alive?(pid) do
      GenServer.stop(pid, :normal)
    end

    :ok
  catch
    :exit, _ -> :ok
  end

  defp userinfo(nil), do: {"postgres", nil}

  defp userinfo(ui) do
    case String.split(ui, ":", parts: 2) do
      [u, p] -> {u, p}
      [u] -> {u, nil}
    end
  end

  @doc """
  Poll `fun` every 25 ms until it returns true; flunk after `tries` polls (≈ `tries * 25` ms).

  NOTE — `tries` is a POLL COUNT, not milliseconds. Call sites pass ms-looking values
  (`wait_until(fun, 8_000)`), so the real ceiling is 25× that (≈200 s), bounded in practice by each
  test's `@tag timeout`. This never manufactures a pass (it returns on the FIRST successful poll) —
  the generous ceiling only absorbs shared-PG16 load jitter; any wall-clock SLA is asserted separately
  (e.g. the idle-closure monotonic-time bound). Prefer a modest count for a fast-failing condition.
  """
  def wait_until(fun, tries \\ 400) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        ExUnit.Assertions.flunk("timed out waiting for a live-PG condition")

      true ->
        _ = Process.sleep(25)
        wait_until(fun, tries - 1)
    end
  end
end

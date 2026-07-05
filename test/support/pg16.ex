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

  defp userinfo(nil), do: {"postgres", nil}

  defp userinfo(ui) do
    case String.split(ui, ":", parts: 2) do
      [u, p] -> {u, p}
      [u] -> {u, nil}
    end
  end

  @doc "Poll `fun` until it returns true, or flunk after ~ `tries * 25`ms."
  def wait_until(fun, tries \\ 400) do
    cond do
      fun.() -> :ok
      tries == 0 -> ExUnit.Assertions.flunk("timed out waiting for a live-PG condition")
      true -> Process.sleep(25) && wait_until(fun, tries - 1)
    end
  end
end

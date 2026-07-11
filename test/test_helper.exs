ExUnit.start()

defmodule Replicant.TestHelper do
  @moduledoc false
  alias Replicant.Test.PG16

  # The numeric server version behind REPLICANT_TEST_URL. When the URL is UNSET, returns 0 so
  # :integration and :pg17 are honestly EXCLUDED (skip, never a vacuous pass) — the DoD requires
  # an EXECUTED PG17 run, so skip-count != coverage. When the URL is SET, the operator EXPECTS
  # substrate coverage: if the version cannot be determined (connect/query failure) we FAIL
  # CLOSED and let the error propagate, aborting the suite. Returning 0 on a set-but-unreachable
  # URL would silently drop ALL :integration + :pg17 and report green — the exact vacuous pass
  # this mechanism exists to prevent (closeout gate-integrity finding). 0 is reserved for the
  # genuinely-unset URL alone.
  def server_version_num do
    url = System.get_env("REPLICANT_TEST_URL")

    if url in [nil, ""] do
      0
    else
      # start_link is async and returns {:ok, _} even for an unreachable URL — the failure
      # surfaces at query!, which we let PROPAGATE (fail closed) rather than rescue to 0. A
      # non-{:ok} start_link likewise raises via the match (fail closed).
      {:ok, conn} = Postgrex.start_link(PG16.pg_opts())

      try do
        Postgrex.query!(conn, "SHOW server_version_num", []).rows
        |> hd()
        |> hd()
        |> String.to_integer()
      after
        GenServer.stop(conn)
      end
    end
  end
end

version = Replicant.TestHelper.server_version_num()

cond do
  version == 0 -> ExUnit.configure(exclude: [:integration, :pg17])
  version < 170_000 -> ExUnit.configure(exclude: [:pg17])
  true -> :ok
end

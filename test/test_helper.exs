ExUnit.start()

defmodule Replicant.TestHelper do
  @moduledoc false
  alias Replicant.Test.PG16

  # The numeric server version behind REPLICANT_TEST_URL, or 0 if unset/unreachable/erroring.
  # Excludes the :pg17 tag on a PG < 17 substrate so those tests are honestly SKIPPED (not
  # vacuously passed) — the DoD requires an EXECUTED PG17 run, so skip-count != coverage.
  def server_version_num do
    url = System.get_env("REPLICANT_TEST_URL")

    if url in [nil, ""] do
      0
    else
      case Postgrex.start_link(PG16.pg_opts()) do
        {:ok, conn} ->
          # Guard the QUERY too: start_link is async and returns {:ok, _} even for an
          # unreachable URL — the failure surfaces at query!. A raise must degrade to 0
          # (exclude everything), never abort the whole suite at boot.
          try do
            Postgrex.query!(conn, "SHOW server_version_num", []).rows
            |> hd()
            |> hd()
            |> String.to_integer()
          rescue
            _ -> 0
          after
            GenServer.stop(conn)
          end

        _ ->
          0
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

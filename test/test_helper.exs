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

  def primary_plugins do
    if System.get_env("REPLICANT_TEST_URL") in [nil, ""] do
      []
    else
      {:ok, conn} = Postgrex.start_link(PG16.pg_opts())

      try do
        pglogical =
          Postgrex.query!(
            conn,
            "SELECT 1 FROM pg_available_extensions WHERE name = 'pglogical'",
            []
          ).rows == [[1]]

        wal2json = wal2json_available?(conn, pglogical)

        for {plugin, true} <- [pglogical: pglogical, wal2json: wal2json], do: plugin
      after
        GenServer.stop(conn)
      end
    end
  end

  defp wal2json_available?(conn, pglogical?) do
    if System.get_env("EXPECTED_PG_PLUGINS") do
      # CI runs as the disposable instance's bootstrap user. Check both presence
      # and absence without allocating slots, including on the pre-10 primary.
      plugin_available?(Postgrex.query(conn, "LOAD 'wal2json'", []))
    else
      # A contributor's replication role need not have LOAD privileges.
      if pglogical? do
        slot = "replicant_wal2json_probe_#{System.unique_integer([:positive])}"

        plugin_available?(
          Postgrex.query(
            conn,
            "SELECT pg_create_logical_replication_slot($1, 'wal2json', true)",
            [slot]
          )
        )
      else
        false
      end
    end
  end

  defp plugin_available?({:ok, _}), do: true

  defp plugin_available?({:error, %Postgrex.Error{postgres: %{code: code}}})
       when code in [:undefined_file, :insufficient_privilege, :undefined_function],
       do: false

  defp plugin_available?({:error, error}), do: raise(error)
end

version = Replicant.TestHelper.server_version_num()
plugins = Replicant.TestHelper.primary_plugins()

if expected = System.get_env("EXPECTED_PG_PLUGINS") do
  actual = Enum.map(plugins, &Atom.to_string/1)

  if Enum.sort(actual) != Enum.sort(String.split(expected, ",", trim: true)) do
    raise "matrix row expected PostgreSQL plugins #{inspect(expected)}, found #{inspect(actual)}"
  end
end

# Pre-10 has no publications. Its cross-version plugin parity runs on the
# publication-capable primary with REPLICANT_PG96_URL pointing to the legacy server.
exclusions = [
  integration: version == 0,
  pg10: version < 100_000,
  pg14: version < 140_000,
  pg17: version < 170_000,
  pglogical: :pglogical not in plugins,
  wal2json: version < 100_000 or :wal2json not in plugins
]

ExUnit.configure(exclude: for({tag, true} <- exclusions, do: tag))

# NOTE on `--include integration` against a PG16 server: ExUnit's `--include TAG` rescues a test
# carrying that tag from EVERY exclusion, so `mix test --include integration` re-admits the
# `:pg17` tests (which carry BOTH `:integration` and `:pg17`) on a PG16 substrate and they FAIL
# (failover slots need PG17+). This is a framework constraint, not a bug: ExUnit setup callbacks
# cannot return {:skip, reason} (valid returns are :ok / keyword / map only), so there is no
# runtime-skip-from-setup to harden the gate further. The canonical invocations are correct:
# `mix test` (no flags) skips :pg17 on PG16 and runs them on PG17; run the failover file against a
# PG17+ server explicitly. Do NOT pair `--include integration` with a <17 server.

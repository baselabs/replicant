# Exercise the R01-R05 public/runtime contracts from the ARTIFACT-DERIVED, freshly compiled
# Replicant — semantically, not by text grep. Run inside the scratch consumer project (which
# depends only on the extracted package source), so `:code.which/1` must resolve Replicant under
# the scratch tree: a leak of the repo checkout or its _build would fail the provenance guard.
#
# Usage (inside the consumer project): SMOKE_SCRATCH=<scratch-root> mix run ../consumer_smoke.exs
scratch = System.get_env("SMOKE_SCRATCH") || raise "SMOKE_SCRATCH not set"

path = Replicant |> :code.which() |> to_string()

unless String.starts_with?(path, scratch <> "/") do
  raise "provenance: Replicant loaded from #{path}, not under scratch #{scratch} — the consumer is not running the extracted artifact"
end

# R04 + D2 — public Sink callbacks are present.
cbs = Replicant.Sink.behaviour_info(:callbacks)
unless {:handle_slot_origin, 2} in cbs, do: raise("R04: handle_slot_origin/2 missing from packaged Sink")
unless {:handle_session_identity, 2} in cbs, do: raise("D2: handle_session_identity/2 missing")

# D2 — the identity query and struct ship.
unless Replicant.QueryBuilder.identify_system() == "IDENTIFY_SYSTEM", do: raise("D2: IDENTIFY_SYSTEM query missing")
_ = %Replicant.SessionIdentity{system_identifier: 1, timeline_id: 1, current_lsn: 0, database: "x"}

# R02/R03 — the value-free telemetry boundary rejects a wrong-shape value without echoing it.
secret = "SECRET-ROW-VALUE-consumer"

try do
  Replicant.Telemetry.validate!(%{commit_lsn: secret})
  raise "R02: validate! accepted a string LSN"
rescue
  e in ArgumentError ->
    if String.contains?(Exception.message(e), secret), do: raise("R02/R03: telemetry leaked the value")
end

# R05 — version-tiered slot-invalidation query.
{:ok, pg15} = Replicant.QueryBuilder.slot_invalidation_status("s", 150_000)
{:ok, pg17} = Replicant.QueryBuilder.slot_invalidation_status("s", 170_000)
if String.contains?(pg15, "conflicting"), do: raise("R05: PG15 query must not select `conflicting`")
unless String.contains?(pg17, "invalidation_reason"), do: raise("R05: PG17 query must select `invalidation_reason`")

# R01 — unknown checkpoint + absent slot halts fail-closed, never creating a slot.
state = %Replicant.Connection{
  step: :invalidation_check,
  slot_name: "audit_slot",
  publication: ["audit_pub"],
  snapshot: false,
  checkpoint_lsn: 0,
  checkpoint_state: :fault,
  failover: false
}

case Replicant.Connection.handle_result([%Postgrex.Result{rows: []}], state) do
  {:disconnect, :data_gap} -> :ok
  other -> raise "R01: fault+absent slot did not halt fail-closed; got #{inspect(other)}"
end

IO.puts("consumer_smoke: OK — R01-R05 public surface exercised from #{path}")

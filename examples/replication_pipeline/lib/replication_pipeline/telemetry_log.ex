defmodule ReplicationPipeline.TelemetryLog do
  @moduledoc """
  Value-free telemetry logger on replicant's halt-signal and lifecycle events
  (the allowlisted metadata is structural — LSNs, names, counts, reason
  classes; never a row value, Critical Rule 1). Attached before the pipeline
  starts so an early halt is still visible.

  The halt-signal handling is COMPLETE ACROSS CONFIGURATIONS — every event the
  library emits on a fail-closed halt path — so a copy of this logger into a
  snapshot-enabled or spill-enabled pipeline misses nothing. Some signals
  cannot fire under THIS example's configuration (no lib checkpoint store, no
  snapshot, no spill): they are listed anyway for the copy-paste path.

  THE ONE TWO-FACED EVENT: `[:replicant, :connection, :disconnected]` is a
  plain lifecycle event on an ordinary drop, but it is ALSO the only signal of
  the sink-lag halt — which arrives with `reason: :sink_too_slow` and a signed
  `lag` measurement (bytes) and fails the pipeline closed. `handle/4` routes
  that reason to the halt (error) path; every other disconnect stays lifecycle.
  """

  require Logger

  @halt_signals [
    [:replicant, :connection, :command_error_halt],
    [:replicant, :connection, :slot_invalidated],
    [:replicant, :connection, :session_identity_rejected],
    [:replicant, :checkpoint_store, :failed],
    [:replicant, :schema_change, :halted],
    [:replicant, :stream, :spill_exhausted],
    [:replicant, :sink, :failed],
    [:replicant, :snapshot, :failed]
  ]

  # The lag halt rides the DISCONNECT event (no dedicated event name); the halt
  # itself is distinguished by its reason atom. See usage-rules.md's telemetry
  # reference for the full table.
  @lag_halt_reason :sink_too_slow

  @lifecycle [
    [:replicant, :connection, :connected],
    [:replicant, :connection, :disconnected],
    [:replicant, :checkpoint, :advanced]
  ]

  @doc "Attaches the logger handlers (idempotent by handler id)."
  @spec attach() :: :ok
  def attach do
    :telemetry.attach_many(__MODULE__, @halt_signals ++ @lifecycle, &__MODULE__.handle/4, nil)
    :ok
  end

  def handle(event, _measurements, %{reason: @lag_halt_reason} = metadata, _config) do
    Logger.error("replicant halt-signal #{inspect(event)} #{inspect(metadata)}")
  end

  def handle(event, _measurements, metadata, _config) do
    if event in @halt_signals do
      Logger.error("replicant halt-signal #{inspect(event)} #{inspect(metadata)}")
    else
      Logger.info("replicant #{inspect(event)} #{inspect(metadata)}")
    end
  end
end

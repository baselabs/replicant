defmodule ReplicationPipeline.TelemetryLog do
  @moduledoc """
  Value-free telemetry logger on replicant's ACTUAL halt-signal and lifecycle
  events (the allowlisted metadata is structural — LSNs, names, counts,
  reason classes; never a row value, Critical Rule 1). Attached before the
  pipeline starts so an early halt is still visible.
  """

  require Logger

  @halt_signals [
    [:replicant, :connection, :command_error_halt],
    [:replicant, :connection, :slot_invalidated],
    [:replicant, :connection, :session_identity_rejected],
    [:replicant, :checkpoint_store, :failed],
    [:replicant, :schema_change, :halted],
    [:replicant, :stream, :spill_exhausted],
    [:replicant, :sink, :failed]
  ]

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

  def handle(event, _measurements, metadata, _config) do
    if event in @halt_signals do
      Logger.error("replicant halt-signal #{inspect(event)} #{inspect(metadata)}")
    else
      Logger.info("replicant #{inspect(event)} #{inspect(metadata)}")
    end
  end
end

defmodule Replicant.Spill.Error do
  @moduledoc """
  A value-free spill exception. Raised by `Replicant.Spill.Reader` on any `File`/`binary_to_term`
  fault so the lazy read (which runs inside the sink call, spec §11) never surfaces a raw byte.
  Carries ONLY the fixed reason — no path, no frame, no row value (Critical Rule 1).
  """
  defexception reason: :spill_io_failed

  @type t :: %__MODULE__{reason: :spill_io_failed}

  @impl true
  def message(%__MODULE__{reason: reason}), do: "replicant spill error reason=#{reason}"
end

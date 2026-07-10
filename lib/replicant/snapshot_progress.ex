defmodule Replicant.SnapshotProgress do
  @moduledoc """
  The incremental-backfill progress token (spec §6.2): a pure struct tracking the
  table queue, the in-progress table and its last delivered PK bound, and completion.

  The wire form is version-tagged (`{:replicant_snapshot_progress, @version, map}`)
  self-identifying encoding — and decoded with `binary_to_term(bin, [:safe])`. It
  contains PK-bound ROW VALUES: it is data-plane (persisted by the sink or the
  checkpoint store) and must NEVER reach an error, log, or telemetry event
  (Critical Rule 1). A decode/shape fault is scrubbed to the bare
  `:snapshot_progress_invalid` — the raised exception is discarded, never inspected.
  """

  @version 1

  @type table_ref :: %{
          schema: String.t(),
          table: String.t(),
          qualified: String.t(),
          pk_raw: [String.t()],
          pk_quoted: [String.t()]
        }

  @type t :: %__MODULE__{
          floor_lsn: Replicant.lsn(),
          pending: [table_ref()],
          current: table_ref() | nil,
          bound: [term()] | nil,
          done: [String.t()],
          complete?: boolean()
        }

  defstruct floor_lsn: 0, pending: [], current: nil, bound: nil, done: [], complete?: false

  @doc "A fresh token over the discovered publication tables (spec §6.2)."
  @spec new([table_ref()], Replicant.lsn()) :: t()
  def new(tables, floor_lsn) when is_list(tables) and is_integer(floor_lsn) do
    %__MODULE__{floor_lsn: floor_lsn, pending: tables}
  end

  @doc "The next unit of work: the in-progress table at its bound, the next queued table, or :complete."
  @spec next(t()) :: {:table, table_ref(), [term()] | nil, t()} | :complete
  def next(%__MODULE__{complete?: true}), do: :complete
  def next(%__MODULE__{current: %{} = t, bound: bound} = sp), do: {:table, t, bound, sp}

  def next(%__MODULE__{pending: [t | rest]} = sp),
    do: {:table, t, nil, %{sp | current: t, bound: nil, pending: rest}}

  def next(%__MODULE__{pending: []}), do: :complete

  @doc """
  Record the last delivered chunk's upper PK bound for the in-progress table.

  REQUIRES an in-progress table (the caller establishes one via `next/1` first).
  Calling this with no current table is a caller bug, not a supported path — it
  raises `FunctionClauseError` rather than silently masking the ordering error.
  """
  @spec advance(t(), [term()]) :: t()
  def advance(%__MODULE__{current: %{}} = sp, bound) when is_list(bound),
    do: %{sp | bound: bound}

  @doc """
  The in-progress table is fully delivered; move on.

  REQUIRES an in-progress table (the caller establishes one via `next/1` first).
  Calling this with no current table is a caller bug, not a supported path — it
  raises `FunctionClauseError` rather than silently masking the ordering error.
  """
  @spec finish_table(t()) :: t()
  def finish_table(%__MODULE__{current: %{qualified: q}} = sp),
    do: %{sp | current: nil, bound: nil, done: [q | sp.done]}

  @doc "Reset the in-progress table's bound to re-read it from the start (PK-less redo, spec §6.4)."
  @spec redo_table(t()) :: t()
  def redo_table(%__MODULE__{current: %{}} = sp), do: %{sp | bound: nil}

  @doc "Mark the whole backfill complete (terminal; written only after the completion call, spec §6.3)."
  @spec mark_complete(t()) :: t()
  def mark_complete(%__MODULE__{} = sp), do: %{sp | complete?: true, current: nil, bound: nil}

  @doc "Whether the whole backfill is complete (terminal)."
  @spec complete?(t()) :: boolean()
  def complete?(%__MODULE__{complete?: c}), do: c

  @doc "Version-tagged wire encoding."
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{} = sp),
    do: :erlang.term_to_binary({:replicant_snapshot_progress, @version, Map.from_struct(sp)})

  @doc """
  Decode a persisted token. EVERY failure — truncation, tamper, a foreign term, an
  unknown version, an atom-forging binary — returns the bare value-free
  `{:error, :snapshot_progress_invalid}` (spec §6.2/§9; `Error.decode_failure/1`
  precedent: the exception is discarded).
  """
  @spec decode(term()) :: {:ok, t()} | {:error, :snapshot_progress_invalid}
  # Reject the COMPRESSED external-term format (version magic 131 + compression tag 80) before
  # decoding: `encode/1` uses `term_to_binary/1` without `:compressed`, so a legitimate token is
  # NEVER compressed — but `binary_to_term` INFLATES a compressed term BEFORE the `:safe` checks
  # bite, so a few-KB tampered token could expand to a multi-GB term and OOM the reader
  # (a progress-store-write-gated DoS). Value-free reject (spec §6.2/§9).
  def decode(<<131, 80, _rest::binary>>), do: {:error, :snapshot_progress_invalid}

  def decode(bin) when is_binary(bin) do
    case :erlang.binary_to_term(bin, [:safe]) do
      {:replicant_snapshot_progress, @version, %{} = map} -> from_map(map)
      _other -> {:error, :snapshot_progress_invalid}
    end
  rescue
    _ -> {:error, :snapshot_progress_invalid}
  end

  def decode(_other), do: {:error, :snapshot_progress_invalid}

  defp from_map(map) do
    sp = struct(__MODULE__, map)

    if is_integer(sp.floor_lsn) and is_list(sp.pending) and is_list(sp.done) and
         is_boolean(sp.complete?) and valid_current?(sp.current) and valid_bound?(sp.bound) and
         Enum.all?(sp.pending, &valid_table_ref?/1) and Enum.all?(sp.done, &is_binary/1) do
      {:ok, sp}
    else
      {:error, :snapshot_progress_invalid}
    end
  end

  defp valid_current?(nil), do: true
  defp valid_current?(current), do: valid_table_ref?(current)

  defp valid_bound?(nil), do: true
  defp valid_bound?(bound), do: is_list(bound)

  defp valid_table_ref?(%{schema: s, table: t, qualified: q, pk_raw: pr, pk_quoted: pq})
       when is_binary(s) and is_binary(t) and is_binary(q) and is_list(pr) and is_list(pq),
       do: true

  defp valid_table_ref?(_), do: false
end

defmodule Replicant.Spill.Reader do
  @moduledoc """
  A lazy, SINGLE-PASS `Enumerable` over a spilled transaction's changes (spec §5): it streams the
  on-disk frames (in commit order) followed by the still-resident in-memory tail, rejects any change
  whose `subxid` is in the aborted set, and stamps `commit_lsn` at replay (streamed changes have no
  LSN before commit; the `ordinal` was stamped at accumulation from the shared per-txn counter and is
  preserved). It is value-free BY CONSTRUCTION — a `File` read or
  `binary_to_term` fault raises `Replicant.Spill.Error` (fixed reason, no bytes), never a raw-byte
  exception, even though the sink forces this enumeration inside its own DB transaction (spec §11).

  Delivered as `%Transaction{changes: reader}` for a spilled transaction; the sink MUST consume it
  within the delivery call (single-pass; reading a deleted file raises `Spill.Error`, fail-loud).
  """
  alias Replicant.Spill

  @enforce_keys [:path, :tail, :aborted, :commit_lsn]
  defstruct [:path, :tail, :aborted, :commit_lsn]

  @type t :: %__MODULE__{
          path: String.t(),
          tail: [{non_neg_integer(), Replicant.Change.t()}],
          aborted: MapSet.t(non_neg_integer()),
          commit_lsn: Replicant.lsn()
        }

  @doc """
  Build a Reader over spilled file `path` + the in-memory `tail` (stored newest-first, reversed to
  commit order here), filtering `aborted` subxids, stamping `commit_lsn`.
  """
  @spec new(String.t(), [{non_neg_integer(), Replicant.Change.t()}], MapSet.t(), Replicant.lsn()) ::
          t()
  def new(path, tail, %MapSet{} = aborted, commit_lsn) do
    %__MODULE__{path: path, tail: tail, aborted: aborted, commit_lsn: commit_lsn}
  end

  @doc false
  # The lazy pipeline: disk frames ++ tail(commit-order) → reject aborted → stamp commit_lsn (the
  # accumulation-time ordinal is preserved).
  # PUBLIC (not defp) because the `Enumerable` impl below is a SEPARATE module and cannot call a
  # private function of this one.
  def raw_stream(%__MODULE__{path: path, tail: tail, aborted: aborted}) do
    disk = frame_stream(path)
    mem = Enum.reverse(tail)

    Stream.concat(disk, mem)
    |> Stream.reject(fn {subxid, _change} -> MapSet.member?(aborted, subxid) end)
    |> Stream.map(fn {_subxid, change} -> change end)
  end

  # Stream frames from the file, one resident at a time. Any File/binary_to_term fault → Spill.Error.
  defp frame_stream(path) do
    Stream.resource(
      fn -> open!(path) end,
      &next_frame/1,
      fn device -> _ = :file.close(device) end
    )
  end

  defp open!(path) do
    case :file.open(path, [:read, :binary, :raw]) do
      {:ok, device} -> device
      {:error, _} -> raise Spill.Error, reason: :spill_io_failed
    end
  end

  defp next_frame(device) do
    case :file.read(device, 4) do
      :eof ->
        {:halt, device}

      {:ok, <<len::32>>} ->
        case :file.read(device, len) do
          {:ok, bin} when byte_size(bin) == len -> {[decode_frame(bin)], device}
          _ -> raise Spill.Error, reason: :spill_io_failed
        end

      _ ->
        raise Spill.Error, reason: :spill_io_failed
    end
  end

  defp decode_frame(bin) do
    # :safe forbids atom/fun creation from disk bytes. Also validate the frame SHAPE at this at-rest
    # deserialization boundary: a valid :safe term of the WRONG shape (corruption, or a tampered 0600
    # file) must fail value-free as :spill_io_failed HERE, not slip through to a misattributed
    # MatchError in raw_stream's {subxid, _} destructure (→ :sink_failed). Any failure → Spill.Error.
    case :erlang.binary_to_term(bin, [:safe]) do
      {subxid, %Replicant.Change{}} = frame when is_integer(subxid) -> frame
      _ -> raise Spill.Error, reason: :spill_io_failed
    end
  rescue
    _ -> reraise Spill.Error, [reason: :spill_io_failed], __STACKTRACE__
  end

  defimpl Enumerable do
    alias Replicant.Spill.Reader

    def reduce(%Reader{commit_lsn: lsn} = reader, acc, fun) do
      reader
      |> Reader.raw_stream()
      # The change's `ordinal` was stamped at accumulation from the assembler's shared per-txn
      # counter (persisted in the spill frame) and is PRESERVED here — never re-indexed — so a
      # spilled change interleaves with any transactional message's ordinal (spec §5).
      |> Stream.map(fn change -> %{change | commit_lsn: lsn} end)
      |> Enumerable.reduce(acc, fun)
    end

    # Lazy / single-pass: count and membership are NOT O(1); force via reduce only if a caller insists.
    def count(_reader), do: {:error, __MODULE__}
    def member?(_reader, _element), do: {:error, __MODULE__}
    def slice(_reader), do: {:error, __MODULE__}
  end
end

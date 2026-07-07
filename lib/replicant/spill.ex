defmodule Replicant.Spill do
  @moduledoc """
  The single `File.*` boundary for consumer disk spill (spec §5/§6). Owns the per-slot spill
  directory (`0700`), the per-top-xid spill file (`0600`), append of length-prefixed
  `:erlang.term_to_binary({subxid, %Change{}})` frames, file discard, and the per-slot startup
  sweep. Every I/O or serialization fault is scrubbed to a value-free
  `{:error, %Replicant.Error{reason: :spill_io_failed}}` here (Critical Rule 1) — no path, frame,
  or row value ever escapes. Spill files are scratch: never fsync'd, deleted on
  commit/abort/reset/halt, and (for a prior abnormal exit) swept per-slot on that slot's next start.
  """
  alias Replicant.{Change, Error}

  @dir_perm 0o700
  @file_perm 0o600

  @type handle :: %{device: :file.io_device(), path: String.t()}

  @doc "Open (create+truncate) the spill file for `top_xid` under `base_dir`/`slot_name` (0700 dir, 0600 file)."
  @spec open(String.t(), String.t(), non_neg_integer()) :: {:ok, handle()} | {:error, Error.t()}
  def open(base_dir, slot_name, top_xid) do
    slot_dir = Path.join(base_dir, slot_name)
    path = Path.join(slot_dir, "#{top_xid}.spill")

    with :ok <- File.mkdir_p(slot_dir),
         :ok <- File.chmod(slot_dir, @dir_perm),
         {:ok, device} <- :file.open(path, [:write, :binary, :raw]) do
      case File.chmod(path, @file_perm) do
        :ok ->
          {:ok, %{device: device, path: path}}

        _ ->
          # close the just-opened device so a post-open chmod failure can't leak an FD
          _ = :file.close(device)
          {:error, %Error{reason: :spill_io_failed}}
      end
    else
      _ -> {:error, %Error{reason: :spill_io_failed}}
    end
  rescue
    _ -> {:error, %Error{reason: :spill_io_failed}}
  end

  @doc "Append one `{subxid, %Change{}}` frame; returns the frame's byte size."
  @spec append(handle(), non_neg_integer(), Change.t()) ::
          {:ok, pos_integer()} | {:error, Error.t()}
  def append(%{device: device}, subxid, %Change{} = change) do
    bin = :erlang.term_to_binary({subxid, change})
    # 4-byte big-endian length prefix caps one frame at 4 GiB (far above any single
    # %Change{}); the Task-3 reader relies on this framing to delimit frames.
    frame = <<byte_size(bin)::32, bin::binary>>

    case :file.write(device, frame) do
      :ok -> {:ok, byte_size(frame)}
      {:error, _} -> {:error, %Error{reason: :spill_io_failed}}
    end
  rescue
    _ -> {:error, %Error{reason: :spill_io_failed}}
  end

  @doc "Close the device WITHOUT removing the file (the reader re-opens for a single read pass)."
  @spec close(handle()) :: :ok
  def close(%{device: device}) do
    _ = :file.close(device)
    :ok
  end

  @doc "Close the device AND delete the file (commit-after-delivery / abort / reset / halt)."
  @spec discard(handle()) :: :ok
  def discard(%{device: device, path: path}) do
    _ = :file.close(device)
    _ = File.rm(path)
    :ok
  end

  @doc "Delete a spill file at `path` if it exists (used when only the path is retained). Best-effort, value-free."
  @spec rm(String.t()) :: :ok
  def rm(path) when is_binary(path) do
    _ = File.rm(path)
    :ok
  end

  @doc """
  Sweep THIS slot's spill subdir on the slot's pipeline start — removes any file a prior abnormal
  exit left (spec §5). Per-slot, NOT global: a global sweep would delete a concurrently-running
  slot's active files. Best-effort and value-free.
  """
  @spec sweep_slot(String.t(), String.t()) :: :ok
  def sweep_slot(base_dir, slot_name) do
    _ = File.rm_rf(Path.join(base_dir, slot_name))
    :ok
  end
end

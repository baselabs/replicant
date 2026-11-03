defmodule Replicant.Test.SilentProxy do
  @moduledoc false
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
  def port(pid), do: GenServer.call(pid, :port)
  def blackhole(pid), do: GenServer.call(pid, :blackhole)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_, port}} = :inet.sockname(listener)
    owner = self()
    acceptor = spawn_link(fn -> accept(listener, owner, opts) end)
    {:ok, %{listener: listener, port: port, acceptor: acceptor, workers: []}}
  end

  @impl true
  def handle_call(:port, _, state), do: {:reply, state.port, state}

  def handle_call(:blackhole, _, state) do
    Enum.each(state.workers, fn worker ->
      if Process.alive?(worker) do
        ref = make_ref()
        send(worker, {:blackhole, self(), ref})

        receive do
          {:blackholed, ^ref} -> :ok
        after
          1000 -> raise "proxy did not blackhole"
        end
      end
    end)

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:worker, pid}, state),
    do: {:noreply, %{state | workers: [pid | state.workers]}}

  def handle_info({:EXIT, pid, reason}, %{acceptor: pid} = state), do: {:stop, reason, state}

  @impl true
  def terminate(_, state) do
    :gen_tcp.close(state.listener)
    Process.exit(state.acceptor, :shutdown)
    Enum.each(state.workers, &Process.exit(&1, :shutdown))
  end

  defp accept(listener, owner, opts) do
    case :gen_tcp.accept(listener) do
      {:ok, client} ->
        worker = spawn(fn -> connect(opts) end)
        :ok = :gen_tcp.controlling_process(client, worker)
        send(worker, {:client, client})
        send(owner, {:worker, worker})
        accept(listener, owner, opts)

      {:error, :closed} ->
        :ok
    end
  end

  defp connect(opts) do
    receive do
      {:client, client} ->
        {:ok, server} =
          :gen_tcp.connect(String.to_charlist(opts[:hostname]), opts[:port], [
            :binary,
            active: true
          ])

        :ok = :inet.setopts(client, active: true)
        relay(client, server, false)
    end
  end

  defp relay(client, server, dropping) do
    receive do
      {:blackhole, caller, ref} ->
        # Model the server timing out its end of a VIP-moved connection without
        # any FIN/RST reaching the client. New TCP connections still pass through.
        :gen_tcp.close(server)
        send(caller, {:blackholed, ref})
        relay(client, server, true)

      {:tcp, socket, bytes} ->
        unless dropping, do: :gen_tcp.send(if(socket == client, do: server, else: client), bytes)
        relay(client, server, dropping)

      {:tcp_closed, ^server} when dropping ->
        relay(client, server, dropping)

      {:tcp_closed, _} ->
        :gen_tcp.close(client)
        :gen_tcp.close(server)
    end
  end
end

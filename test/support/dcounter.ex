defmodule DGen.DCounter do
  use DGen.Server

  def start_link(tenant, tuid), do: DGen.Server.start_link(__MODULE__, [tuid], tenant: tenant)

  def start_link_opts(tenant, tuid, opts),
    do: DGen.Server.start_link(__MODULE__, [tuid], [{:tenant, tenant} | opts])

  def get(pid), do: DGen.Server.call(pid, :get)
  def get_blob(pid, size), do: DGen.Server.call(pid, {:get_blob, size})
  def incr(pid, n \\ 1), do: DGen.Server.cast(pid, {:incr, n})

  @impl true
  def init([tuid]), do: {:ok, tuid, 0}

  @impl true
  def handle_call(:get, _from, state), do: {:reply, state, state}
  def handle_call({:get_blob, size}, _from, state), do: {:reply, :binary.copy(<<0>>, size), state}

  @impl true
  def handle_cast({:incr, n}, state), do: {:noreply, state + n}
end

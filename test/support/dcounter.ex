defmodule DGen.DCounter do
  use DGenServer

  def start_link(tenant, tuid), do: DGenServer.start_link(__MODULE__, [tuid], tenant: tenant)

  def start_link_opts(tenant, tuid, opts),
    do: DGenServer.start_link(__MODULE__, [tuid], [{:tenant, tenant} | opts])

  def get(pid), do: DGenServer.call(pid, :get)
  def get_blob(pid, size), do: DGenServer.call(pid, {:get_blob, size})
  def incr(pid, n \\ 1), do: DGenServer.cast(pid, {:incr, n})

  @impl true
  def init([tuid]), do: {:ok, tuid, 0}

  @impl true
  def handle_call(:get, _from, state), do: {:reply, state, state}
  def handle_call({:get_blob, size}, _from, state), do: {:reply, :binary.copy(<<0>>, size), state}

  @impl true
  def handle_cast({:incr, n}, state), do: {:noreply, state + n}
end

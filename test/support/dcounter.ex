defmodule DGen.DCounter do
  use DGenServer

  def start_link(tenant), do: DGenServer.start_link(__MODULE__, [], tenant: tenant)
  def get(pid), do: DGenServer.call(pid, :get)
  def incr(pid, n \\ 1), do: DGenServer.cast(pid, {:incr, n})

  @impl true
  def init(_), do: {:ok, 0}

  @impl true
  def handle_call(:get, _from, state), do: {:reply, state, state}

  @impl true
  def handle_cast({:incr, n}, state), do: {:noreply, state + n}
end

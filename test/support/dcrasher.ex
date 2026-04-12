defmodule DGen.DCrasher do
  @moduledoc """
  Test module whose callbacks throw on `:crash_me` messages.

  Used to exercise the dead-letter queue logic in dgen_server.
  """
  use DGen.Server

  def start_link(tenant, tuid, opts \\ []),
    do: DGen.Server.start_link(__MODULE__, [tuid], [{:tenant, tenant} | opts])

  # Priority call — bypasses queue, safe for "is the consumer alive?" checks
  def get(pid), do: DGen.Server.priority_call(pid, :get)

  # Queued call — waits for all previously enqueued casts to be processed first
  def call_get(pid), do: DGen.Server.call(pid, :get, 5_000)

  @impl true
  def init([tuid]), do: {:ok, tuid, 0}

  @impl true
  def handle_cast(:crash_me, _state), do: throw(:crash)
  def handle_cast({:incr, n}, state), do: {:noreply, state + n}

  @impl true
  def handle_call(:crash_me, _from, _state), do: throw(:crash)
  def handle_call(:get, _from, state), do: {:reply, state, state}
end

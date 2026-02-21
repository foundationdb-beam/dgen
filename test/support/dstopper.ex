defmodule DGen.DStopper do
  @moduledoc """
  Test module that exercises {stop, reason, state} returns from callbacks.

  Used to verify bug #2 (handle_info stop) and bug #3 (queued call stop).
  """
  use DGenServer

  def start_link(tenant, tuid),
    do: DGenServer.start_link(__MODULE__, [tuid], tenant: tenant)

  def start_link_opts(tenant, tuid, opts),
    do: DGenServer.start_link(__MODULE__, [tuid], [{:tenant, tenant} | opts])

  def stop_via_call(pid),
    do: DGenServer.call(pid, :stop_me, 10_000)

  def stop_via_cast(pid),
    do: DGenServer.cast(pid, :stop_me)

  def stop_with_action_via_call(pid, notify_pid),
    do: DGenServer.call(pid, {:stop_me_with_action, notify_pid}, 10_000)

  def stop_with_action_via_cast(pid, notify_pid),
    do: DGenServer.cast(pid, {:stop_me_with_action, notify_pid})

  def stop_with_action_via_info(pid, notify_pid),
    do: send(pid, {:stop_me_with_action, notify_pid})

  def get(pid), do: DGenServer.priority_call(pid, :get)

  @impl true
  def init([tuid]), do: {:ok, tuid, 0}

  @impl true
  def handle_call(:get, _from, state), do: {:reply, state, state}
  def handle_call(:stop_me, _from, state), do: {:stop, :normal, state}

  def handle_call({:stop_me_with_action, notify_pid}, _from, state),
    do: {:stop, :normal, state, [&notify_stop(notify_pid, &1)]}

  def handle_call(:throw_me, _from, _state), do: throw(:boom)

  @impl true
  def handle_cast(:stop_me, state), do: {:stop, :normal, state}
  def handle_cast({:incr, n}, state), do: {:noreply, state + n}

  def handle_cast({:stop_me_with_action, notify_pid}, state),
    do: {:stop, :normal, state, [&notify_stop(notify_pid, &1)]}

  @impl true
  def handle_info(:stop_me, state), do: {:stop, :normal, state}

  def handle_info({:stop_me_with_action, notify_pid}, state),
    do: {:stop, :normal, state, [&notify_stop(notify_pid, &1)]}

  defp notify_stop(notify_pid, state) do
    send(notify_pid, {:stop_action_executed, state})
  end
end

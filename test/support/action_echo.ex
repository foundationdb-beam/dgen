defmodule DGen.ActionEcho do
  use DGenServer

  def start_link(tenant, tuid),
    do: DGenServer.start_link(__MODULE__, [tuid], tenant: tenant)

  def get_then_take_action(pid, notify_pid),
    do: DGenServer.call(pid, {:get_then_take_action, notify_pid})

  def get_with_action(pid),
    do: DGenServer.call(pid, {:get_with_action}, reply_with_effects: true)

  def incr_then_take_action(pid, notify_pid, n \\ 1),
    do: DGenServer.cast(pid, {:incr_then_take_action, notify_pid, n})

  def priority_get_then_take_action(pid, notify_pid),
    do: DGenServer.priority_call(pid, {:get_then_take_action, notify_pid})

  def priority_incr_then_take_action(pid, notify_pid, n \\ 1),
    do: DGenServer.priority_cast(pid, {:incr_then_take_action, notify_pid, n})

  @impl true
  def init([tuid]), do: {:ok, tuid, 0}

  @impl true
  def handle_call({:get_then_take_action, notify_pid}, _from, state) do
    {:reply, state, state, [&handle_action_executed(:call, notify_pid, &1)]}
  end

  def handle_call({:get_with_action}, _from, state) do
    {:reply, state, state, [&handle_action_executed(:call, nil, &1)]}
  end

  @impl true
  def handle_cast({:incr_then_take_action, notify_pid, n}, state) do
    {:noreply, state + n, [&handle_action_executed(:cast, notify_pid, &1)]}
  end

  @impl true
  def handle_info({:info_then_take_action, notify_pid}, state) do
    {:noreply, state + 1, [&handle_action_executed(:info, notify_pid, &1)]}
  end

  defp handle_action_executed(action, nil, _state) do
    {:cont, [action]}
  end

  defp handle_action_executed(action, notify_pid, _state) do
    send(notify_pid, {:action_executed, action})
  end
end

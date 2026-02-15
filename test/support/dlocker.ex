defmodule DGen.DLocker do
  @moduledoc """
  Test module that exercises the lock mechanism.

  State is an integer counter. Certain operations acquire a lock,
  do long-running work outside the transaction, then commit the
  result via handle_locked/3.
  """
  use DGenServer

  def start_link(tenant, tuid),
    do: DGenServer.start_link(__MODULE__, [tuid], tenant: tenant)

  def start_link_opts(tenant, tuid, opts),
    do: DGenServer.start_link(__MODULE__, [tuid], [{:tenant, tenant} | opts])

  def get(pid), do: DGenServer.priority_call(pid, :get)
  def incr(pid), do: DGenServer.cast(pid, :incr)

  @doc "Cast that acquires a lock, does async work, then commits"
  def lock_incr(pid, notify_pid),
    do: DGenServer.cast(pid, {:lock_incr, notify_pid})

  @doc "Call that acquires a lock, does async work, then replies"
  def lock_get(pid, notify_pid),
    do: DGenServer.call(pid, {:lock_get, notify_pid}, 15_000)

  @doc "Priority call that acquires a lock"
  def priority_lock_get(pid, notify_pid),
    do: DGenServer.priority_call(pid, {:lock_get, notify_pid}, 15_000)

  @impl true
  def init([tuid]), do: {:ok, tuid, 0}

  @impl true
  def handle_call(:get, _from, state), do: {:reply, state, state}

  def handle_call({:lock_get, _notify_pid}, _from, state) do
    {:lock, state}
  end

  @impl true
  def handle_cast(:incr, state), do: {:noreply, state + 1}

  def handle_cast({:lock_incr, _notify_pid}, state) do
    {:lock, state}
  end

  @impl true
  def handle_locked(event_type, {tag, notify_pid}, state) when tag in [:lock_get, :lock_incr] do
    # Signal that we've entered the locked section
    send(notify_pid, {:locked_entered, self()})

    # Wait for the test to tell us to proceed
    receive do
      :continue -> :ok
    after
      10_000 -> raise "timed out waiting for :continue"
    end

    send(notify_pid, {:locked_exiting, self()})

    case event_type do
      {:call, _from} ->
        {:reply, state, state + 100}

      :cast ->
        {:noreply, state + 100}
    end
  end
end

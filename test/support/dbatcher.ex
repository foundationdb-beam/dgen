defmodule DGen.DBatcher do
  @moduledoc """
  Test module for exercising `consume_batch/9`'s exit points.

  `dgen_server:consume_batch/9` writes mod state exactly once per batch, at each
  of five exit points — batch exhausted, dead letter, `{lock, ...}`, `{stop, ...}`,
  and the `catch` clause — plus committing that batch's dequeues in the same
  transaction. That "exactly one `set_mod_state` per batch" property is maintained
  by convention across five call sites, so the tests need a module that can put any
  of those five outcomes at an arbitrary position in one `consume_k`-sized batch.

  State is `%{n: integer, seen: [term]}`: `n` proves the mod-state write landed,
  `seen` proves *which* messages contributed to it, which is what distinguishes a
  correct partial-batch commit from one that dropped or double-counted a message.
  """
  use DGen.Server

  def start_link_opts(tenant, tuid, opts),
    do: DGen.Server.start_link(__MODULE__, [tuid], [{:tenant, tenant} | opts])

  # Priority call, so reading state never queues behind the batch under test.
  def get(pid), do: DGen.Server.priority_call(pid, :get)

  def incr(pid, tag), do: DGen.Server.cast(pid, {:incr, tag})
  def incr_action(pid, tag, notify), do: DGen.Server.cast(pid, {:incr_action, tag, notify})
  def boom(pid, tag), do: DGen.Server.cast(pid, {:boom, tag})
  def stop(pid, tag), do: DGen.Server.cast(pid, {:stop, tag})
  def lock(pid, tag, notify), do: DGen.Server.cast(pid, {:lock, tag, notify})

  @impl true
  def init([tuid]), do: {:ok, tuid, %{n: 0, seen: []}}

  @impl true
  def handle_call(:get, _from, state), do: {:reply, state, state}

  @impl true
  def handle_cast({:incr, tag}, state), do: {:noreply, saw(state, tag)}

  def handle_cast({:incr_action, tag, notify}, state) do
    {:noreply, saw(state, tag), [&send_action(notify, tag, &1)]}
  end

  def handle_cast({:boom, tag}, _state), do: throw({:boom, tag})

  def handle_cast({:stop, tag}, state), do: {:stop, :normal, saw(state, tag)}

  # Returning `{:lock, state}` splits the batch: everything before this message is
  # committed and its actions run, then `handle_locked/4` runs outside the tx.
  def handle_cast({:lock, _tag, _notify}, state), do: {:lock, state}

  @impl true
  def handle_locked(_db_ctx, :cast, {:lock, tag, notify}, state) do
    send(notify, {:locked_entered, tag, self()})

    receive do
      :continue -> :ok
    after
      10_000 -> raise "timed out waiting for :continue"
    end

    {:noreply, saw(state, tag)}
  end

  # Actions receive the *end-of-batch* mod state, not the state at the message that
  # produced them (dgen_server.erl §"Actions"). Echoing both the tag and the state
  # the action was handed is what makes that aliasing observable.
  defp send_action(notify, tag, state), do: send(notify, {:action, tag, state})

  defp saw(%{n: n, seen: seen}, tag), do: %{n: n + 1, seen: seen ++ [tag]}
end

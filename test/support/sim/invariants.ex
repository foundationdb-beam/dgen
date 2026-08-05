defmodule DGen.Sim.Invariants do
  @moduledoc """
  The safety properties `formal/DgenRegistryReplication.tla` checks, evaluated
  against a live `DGen.Sim.Cluster` instead of a model.

  The formal model and this module check the same claims from opposite ends. TLC
  explores every interleaving of an abstraction of the protocol; this explores a
  sampled subset of interleavings of the *real* code. The model can prove the
  protocol right and still miss a bug in the implementation of it; this can catch
  that bug but never prove its absence. Keeping the property names aligned is
  deliberate — a violation here should be reproducible there, and vice versa.

  Each function returns `:ok` or `{:violation, details}`. None of them are allowed
  to raise on a partly-torn-down cluster, since they run continuously during a
  fault-injection run.

  ## Which properties hold *during* faults, and which only after convergence

  Some of these are true at every instant; others are only true once the network
  is healed and the cluster has quiesced. Asserting a converged-only property
  mid-fault would produce false positives, so they are separated explicitly:

  - **Always** — `leader_epoch_unique/1`.
  - **At quiescence** — `same_version_same_replica/1`; see `check_quiescent/1`.
  - **After convergence only** — `unique_binding/2`, `acked_bindings_present/2`.
  """

  alias DGen.Sim.Cluster

  # ---------------------------------------------------------------------------
  # Replica agreement (converged-only — see the moduledoc)
  # ---------------------------------------------------------------------------

  @doc """
  `PrefixConsistency`, in the form observable from outside.

  The spec states every live member's replica equals the committed history at its
  applied version, which has a directly checkable corollary the spec itself calls
  out: *two members reporting the same version necessarily hold the same map*.
  That corollary is what makes the freshest-wins handoff gather sound (§5.7) — if
  two members could hold different content at one version, picking "the freshest"
  would be picking arbitrarily.

  ## Why this is not continuous

  It is tempting to check this at every instant, and it was written that way at
  first. It does not hold continuously, and not because of a bug: members write
  their replica *optimistically*, outside the replicated stream, without advancing
  `applied_version`. `route_unregister/3` deletes the row locally the moment the
  caller asks, before the leader has committed anything, and a follower's
  `handle_register_reply/4` inserts the row when the leader's `yes` arrives. Both
  leave that member holding different content from its peers *at the same version*
  for as long as the batch takes to come back around.

  So a mid-flight difference here is ordinary speculation, not divergence. What
  must not survive is a difference that is still there once the cluster has
  **quiesced** — which is precisely the shape of the partially-delivered-batch bug
  this harness found (see the "batch atomicity" regression tests): permanent, and
  invisible to gap detection because the versions matched.

  Quiescence is the weakest condition that excludes speculation, and it is strictly
  better than waiting for convergence: healing a fault repairs the divergence before
  it can be observed. Under `eta_run` it is `check_quiescent/1`; here, where nothing
  can say when the system has stopped moving, it is asserted after
  `Cluster.converge/2` and in the targeted drop tests, which look immediately after a
  drain rather than after a heal.

  ## Only members at the *leader's* version are compared

  Quiescence alone is not enough, and a wider seed sweep proved it. Under message
  loss a follower can sit at a version behind the leader while holding a
  **speculative** write: `route_unregister/3` deletes the row on the originating
  member before anything is committed, and if the batch carrying that unregister is
  then dropped, the member keeps the deletion and its peer does not. Both are at the
  same version as each other, both differ, and no client is waiting — so quiescence
  is satisfied and the old test fired.

  It was a false positive, and the discriminator is what the two cases do with the
  *leader's* version:

      partial-batch defect   leader v, followers v      diverge AT the leader's version
      dropped batch          leader v, followers v-1    diverge BEHIND it

  The defect this property exists for makes a follower report the full version while
  holding part of the batch — it is *caught up* and wrong, which is exactly why gap
  detection cannot see it. A follower that is merely behind is owed a resync and has
  every right to differ in the meantime. Comparing only the members that have
  reached the leader keeps the first and excludes the second.

  Measured: seeds 3 (at 25 operations) and 77 (at 40) reported divergences that were
  entirely this window, and the planted `partial_batch` mutation is still caught.
  """
  def same_version_same_replica(%Cluster{} = c) do
    members =
      c
      |> Cluster.alive()
      |> Enum.map(fn name -> {name, Cluster.applied_version(name), Cluster.bindings(name)} end)
      |> Enum.reject(fn {_, v, _} -> is_nil(v) end)

    # Only members that have caught up to the leader. See below for why this is
    # not the same test as "members at the same version as each other".
    leader_version = leader_version(c)

    by_version =
      members
      |> Enum.filter(fn {_, v, _} -> not is_nil(leader_version) and v == leader_version end)
      |> Enum.group_by(fn {_, v, _} -> v end)

    divergent =
      Enum.filter(by_version, fn {_v, entries} ->
        entries |> Enum.map(fn {_, _, b} -> b end) |> Enum.uniq() |> length() > 1
      end)

    case divergent do
      [] ->
        :ok

      _ ->
        {:violation,
         %{
           property: :same_version_same_replica,
           detail: "members at the same applied_version hold different replicas",
           divergent:
             Map.new(divergent, fn {v, entries} ->
               {v, Map.new(entries, fn {name, _, b} -> {name, b} end)}
             end)
         }}
    end
  end

  # The applied_version of whichever member believes it is the leader, or `nil` if
  # none does — in which case there is nothing to compare against and the property
  # is vacuously satisfied rather than asserted against an arbitrary member.
  defp leader_version(%Cluster{} = c) do
    Enum.find_value(Cluster.alive(c), fn name ->
      case Cluster.status(name) do
        %{leader: leader, member_id: id, applied_version: v} when leader == id -> v
        _ -> nil
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Always-true properties
  # ---------------------------------------------------------------------------

  @doc """
  `LeaderEpochUnique` — two members may both believe they lead only under
  different epochs (§5.1's monotonic fencing token).

  Two members believing they lead *at the same epoch* would mean the epoch is not
  actually fencing anything, and both could commit. Note this checks each member's
  own belief (`status/1`), not the elector's committed answer: a deposed leader
  that has not yet heard about the handoff legitimately still believes it leads,
  which is exactly why the fence carries an epoch.
  """
  def leader_epoch_unique(%Cluster{} = c) do
    believers = Cluster.self_believed_leaders(c)

    duplicated_epochs =
      believers
      |> Enum.group_by(fn {_name, epoch} -> epoch end)
      |> Enum.filter(fn {_epoch, ms} -> length(ms) > 1 end)

    case duplicated_epochs do
      [] ->
        :ok

      _ ->
        {:violation,
         %{
           property: :leader_epoch_unique,
           detail: "two members believe they lead at the same epoch",
           believers: believers
         }}
    end
  end

  # ---------------------------------------------------------------------------
  # Uniqueness and durability (converged-only)
  # ---------------------------------------------------------------------------

  @doc """
  `UniqueBinding` — at most one live pid per name (Guarantee 1).

  Checked across every live member's replica after convergence. Mid-fault, two
  replicas may legitimately disagree about a name (one lagging), so this is only
  meaningful once traffic has been healed and drained.

  `dead_ok?` (default `true`) ignores names bound to already-dead pids: a pid can
  die at any moment and its unregister is asynchronous, so a dead pid still in a
  replica is lag, not a uniqueness breach.
  """
  def unique_binding(%Cluster{} = c, dead_ok? \\ true) do
    conflicts =
      c
      |> Cluster.all_bindings()
      |> Enum.flat_map(fn {_member, binds} -> Map.to_list(binds) end)
      |> Enum.group_by(fn {name, _pid} -> name end, fn {_name, pid} -> pid end)
      |> Enum.map(fn {name, pids} ->
        pids = pids |> Enum.uniq() |> maybe_only_live(dead_ok?)
        {name, pids}
      end)
      |> Enum.filter(fn {_name, pids} -> length(pids) > 1 end)

    case conflicts do
      [] ->
        :ok

      _ ->
        {:violation,
         %{
           property: :unique_binding,
           detail: "a name is bound to more than one live pid",
           conflicts: Map.new(conflicts)
         }}
    end
  end

  defp maybe_only_live(pids, true), do: Enum.filter(pids, &Process.alive?/1)
  defp maybe_only_live(pids, false), do: pids

  @doc """
  Every registration the driver was told `yes` for, whose pid is still alive, is
  still bound to that pid after convergence.

  This is the observable half of Guarantee 4 (two-holder durability): an
  acknowledged registration survives. It is only asserted when **no member was
  crashed** during the run, because the guarantee is explicitly single-fault and,
  under the default degrade-open policy, even a single crash may legitimately drop
  a registration that had one holder (Guarantee 4's stated exception, §5.5).

  `acked` is `%{name => pid}` for every `register_name` that returned `yes` and was
  not later unregistered.
  """
  def acked_bindings_present(%Cluster{} = c, acked) do
    live = Cluster.alive(c)

    missing =
      for {name, pid} <- acked,
          Process.alive?(pid),
          not Enum.any?(live, fn m -> Map.get(Cluster.bindings(m), name) == pid end),
          do: {name, pid}

    case missing do
      [] ->
        :ok

      _ ->
        {:violation,
         %{
           property: :acked_bindings_present,
           detail: "a registration acked `yes` is not held by any live member",
           missing: Map.new(missing)
         }}
    end
  end

  # ---------------------------------------------------------------------------
  # Batch runners
  # ---------------------------------------------------------------------------

  @doc "Runs every always-true property, returning the first violation or `:ok`."
  def check_always(%Cluster{} = c) do
    first_violation([fn -> leader_epoch_unique(c) end])
  end

  @doc """
  Runs the properties that hold whenever the system is **quiescent** — not only
  after a full heal.

  This is the middle ground the pre-eta harness had no way to occupy. `check_always/1`
  is what may be asserted at any instant; `check_converged/3` is what may be asserted
  after healing every fault and pumping traffic until the cluster agrees. Neither can
  catch a divergence that is permanent but invisible to gap detection: the first would
  false-positive on ordinary speculation, and the second heals it before looking —
  `Cluster.converge/2` delivers `{nodeup, _}`, which makes each member re-join, and a
  re-join has the leader re-snapshot it.

  Quiescence is the point where speculation has settled and nothing has been healed.
  `eta_sched` can say exactly when that is (nothing runnable, so only the clock can
  produce more work), which is why this set exists for the eta port and not for the
  harness it came from.
  """
  def check_quiescent(%Cluster{} = c) do
    first_violation([fn -> same_version_same_replica(c) end])
  end

  @doc "Runs the converged-only properties. Call after `Cluster.converge/2`."
  def check_converged(%Cluster{} = c, acked, opts \\ []) do
    checks = [
      fn -> same_version_same_replica(c) end,
      fn -> unique_binding(c) end
    ]

    checks =
      if Keyword.get(opts, :crashed?, false),
        do: checks,
        else: checks ++ [fn -> acked_bindings_present(c, acked) end]

    first_violation(checks)
  end

  defp first_violation(checks) do
    Enum.reduce_while(checks, :ok, fn check, _ ->
      case check.() do
        :ok -> {:cont, :ok}
        violation -> {:halt, violation}
      end
    end)
  end
end

defmodule DGen.RegistryObservabilityTest do
  @moduledoc """
  `eta_observe` against a real system: the registry member's leadership belief,
  readable while the member is suspended.

  This is the property `eta_run` needs and cannot get any other way. Under
  `eta_sched` every member is suspended between steps, so an invariant cannot call
  `status/1` — and the failure is silent rather than loud, because `status/1`
  catches its own timeout and answers `undefined`. A split-brain check built on it
  would report "no member believes it leads" and pass, having checked nothing.

  ## Why this file is shorter than it was

  The first version of this mirrored the fields by hand, publishing at each site
  that assigned them, and this file existed to prove the mirror never went stale —
  cross-checking it against `status/1` after every kind of transition.

  It did not work. Deliberately removing one of the publish calls left every test
  green, because that site was shadowed by another on the paths the tests
  exercised. The lesson is not that the tests needed to be better; it is that
  "every assignment site remembers to publish" is not a property tests can
  practically enforce. `eta_transform`'s observability pass republishes on every callback
  return, so there is no site to miss, and the cross-check below is confirmation
  rather than load-bearing enforcement.
  """
  use DGen.Case, async: false

  alias DGen.Sim.Cluster

  import DGen.ClusterHelper, only: [eventually: 1]

  setup do
    on_exit(fn -> :eta_net.stop() end)
    :ok
  end

  # The published view and the member's own state record must agree.
  defp assert_agrees!(name, context) do
    published = :eta_observe.read(name)
    actual = :dgen_registry.status(name)

    assert is_map(published), "#{context}: #{name} published nothing"
    assert is_map(actual), "#{context}: #{name} did not answer status/1"

    for field <- [:member_id, :leader, :epoch, :synced, :applied_version] do
      assert Map.fetch!(published, field) == Map.fetch!(actual, field),
             """
             #{context}: #{name} published a stale #{field}.
               published: #{inspect(Map.fetch!(published, field))}
               actual:    #{inspect(Map.fetch!(actual, field))}
             """
    end
  end

  defp assert_all_agree!(c, context) do
    for name <- Cluster.alive(c), do: assert_agrees!(name, context)
  end

  describe "the published state tracks the member" do
    test "on a settled cluster, and exactly one member leads", %{tenant: tenant} do
      c = Cluster.start(tenant, 3, seed: 1)
      on_exit(fn -> Cluster.stop(c) end)

      assert_all_agree!(c, "after startup")

      leaders =
        for name <- Cluster.alive(c),
            s = :eta_observe.read(name),
            s.leader == s.member_id,
            do: name

      assert length(leaders) == 1,
             "#{length(leaders)} members believe they lead: #{inspect(leaders)}"
    end

    test "through registration traffic", %{tenant: tenant} do
      c = Cluster.start(tenant, 3, seed: 2)
      on_exit(fn -> Cluster.stop(c) end)

      {_node, leader} = Cluster.leader(c)

      for i <- 1..20 do
        pid = spawn(fn -> Process.sleep(:infinity) end)
        on_exit(fn -> Process.exit(pid, :kill) end)
        :yes = :dgen_registry.register_name({leader, :"obs_#{i}"}, pid)
      end

      assert_all_agree!(c, "after 20 registrations")

      # applied_version moves with the writes, which is the point of republishing
      # on every callback rather than only on leadership transitions.
      assert :eta_observe.read(leader).applied_version > 0
    end

    test "across a leadership change", %{tenant: tenant} do
      c = Cluster.start(tenant, 3, seed: 3)
      on_exit(fn -> Cluster.stop(c) end)

      {_node, old_leader} = Cluster.leader(c)
      old_epoch = :eta_observe.read(old_leader).epoch

      Cluster.crash(c, old_leader)

      assert eventually(fn ->
               case Cluster.leader(c) do
                 {_n, name} -> name != old_leader
                 nil -> false
               end
             end),
             "no new leader was elected"

      assert_all_agree!(c, "after a leadership change")

      {_node, new_leader} = Cluster.leader(c)
      new = :eta_observe.read(new_leader)

      assert new.leader == new.member_id
      assert new.epoch > old_epoch, "the epoch did not advance"
    end
  end

  describe "the invariants survive suspension" do
    # The justification for all of this, tested directly rather than asserted.
    #
    # `Invariants.check_always/1` is the split-brain check, and it reaches
    # `Cluster.self_believed_leaders/1`. Built on `status/1` it would see nothing
    # at all while the members are frozen — not an error, an empty list, which
    # groups into no duplicate epochs and returns `:ok`. Green, and worthless.
    test "the split-brain check still sees the leader with every member suspended",
         %{tenant: tenant} do
      c = Cluster.start(tenant, 3, seed: 5)
      on_exit(fn -> Cluster.stop(c) end)

      running = DGen.Sim.Invariants.check_always(c)
      believers_running = Cluster.self_believed_leaders(c)
      assert length(believers_running) == 1, "no single leader before suspending"

      members = Enum.map(Cluster.alive(c), &Process.whereis/1)
      Enum.each(members, &:erlang.suspend_process/1)

      try do
        believers = Cluster.self_believed_leaders(c)

        assert believers == believers_running,
               """
               The split-brain check went blind while the members were suspended.

                 running:   #{inspect(believers_running)}
                 suspended: #{inspect(believers)}

               An empty list here is the failure mode this exists to prevent: it
               produces no duplicate epochs, so check_always/1 returns :ok having
               observed nothing.
               """

        assert DGen.Sim.Invariants.check_always(c) == running

        # And the contrast, so the test says what changed rather than only that
        # something works: asking is still useless here.
        assert Enum.all?(Cluster.alive(c), &(:dgen_registry.status(&1) == :undefined))
      after
        Enum.each(members, &:erlang.resume_process/1)
      end
    end
  end

  describe "the property the framework needs" do
    test "it reads a suspended member, which status/1 cannot", %{tenant: tenant} do
      c = Cluster.start(tenant, 1, seed: 4)
      on_exit(fn -> Cluster.stop(c) end)

      [name] = Cluster.alive(c)
      member = Process.whereis(name)
      expected = :eta_observe.read(name)
      assert expected.leader == expected.member_id

      :erlang.suspend_process(member)

      try do
        # The whole point: a live read of a process that cannot answer anything.
        assert :eta_observe.read(name) == expected

        # And the failure mode it replaces. Not an error — a plausible answer,
        # which is what makes an invariant built on it quietly vacuous.
        assert :dgen_registry.status(name) == :undefined
      after
        :erlang.resume_process(member)
      end
    end
  end
end

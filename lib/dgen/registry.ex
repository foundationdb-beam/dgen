defmodule DGen.Registry do
  @moduledoc """
  OTP-compatible process registry backed by the configured storage backend.

  Processes started with a `{:via, DGen.Registry, {name, logical_name}}` tuple
  can be addressed by name across an Erlang/Elixir cluster.

  ## Starting a registry

      {:ok, _sup} = DGen.Registry.start_link(:my_registry, tenant)

  `start_link/2,3` returns the pid of the supervisor. Keep that pid if you
  need it later, for example to pass to `Supervisor.stop/2`. You can't look the
  supervisor up by name, as "Process identity" below explains.

  ## Via-tuple usage

      via = {:via, DGen.Registry, {:my_registry, :user_service}}
      GenServer.start_link(MyServer, [], name: via)
      GenServer.call(via, :ping)

  ## Process identity

  Starting a registry creates zero atoms. The caller supplies `name`, and this
  module never generates one. Neither the supervisor nor the elector has a
  registered name. Only the member is registered, under `name` itself, and no
  other atom is derived from it. This matters for applications that start many
  registries dynamically, such as one per tenant. Atoms are never
  garbage-collected, so we avoid creating any beyond the one the caller chose.

  ## Consistency model

  `register_name/2`, `unregister_name/1`, and `whereis_name_consistent/1`
  route through the elected leader. The OTP via-tuple machinery uses
  `whereis_name/1`, which the calling process serves itself without locks. It
  reads the local member's ETS table directly, with no round-trip to the member
  and no network hop. Right after a registration, replication takes a moment to
  reach other nodes. During that window, a snapshot read on a follower node may
  still return `nil`.

  ## Metadata and queries

  A registration can carry metadata for as long as it lives. `register_name/3`
  and `set_metadata/2` attach an `index` map, which queries can search, and an
  opaque `data` payload. `get_metadata/1` reads the metadata back without locks,
  like `whereis_name/1`. `get_metadata_consistent/1` asks the leader, like
  `whereis_name_consistent/1`. `query/2` and `query_consistent/2` find every
  registration whose `index` matches all of a set of exact equalities.

  *This documentation is LLM-generated. See the AI disclosure in `README.md`.*
  """

  @spec child_spec(Keyword.t()) :: Supervisor.child_spec()
  def child_spec(arg) do
    %{
      id: {__MODULE__, make_ref()},
      start: {__MODULE__, :start_link, [arg]},
      type: :supervisor,
      restart: :permanent
    }
  end

  @spec start_link(Keyword.t()) :: Supervisor.on_start()
  def start_link(arg) do
    name = Keyword.fetch!(arg, :name)
    tenant = Keyword.fetch!(arg, :tenant)
    start_link(name, tenant)
  end

  @doc """
  Starts the registry `name` with default options.

  Returns `{:ok, sup_pid}`. The supervisor has no registered name, as
  "Process identity" above explains.
  """
  defdelegate start_link(name, tenant), to: :dgen_registry

  @doc """
  Starts the registry `name` with per-registry options `opts`.

  See `docs/design/dgen_registry_design.md` (§8 Configuration) for the
  available settings, such as `register_replicas`, `strict_replication` and
  `connectivity`. Set `connectivity: :provided_externally` to turn off this
  registry's own distribution mesh. It then relies on the connections that
  another registry maintains (§4.6).
  """
  defdelegate start_link(name, tenant, opts), to: :dgen_registry

  @doc """
  Registers `pid` under `{registry_name, logical_name}`.

  Routes through the local member, which forwards to the leader if needed.
  Returns `:yes` on success. Returns `:no` if another live process holds the
  name, or if an opt-in policy refuses it (design doc §8).
  If no leader is reachable, the call waits for one. It exits if nothing decides
  within `register_timeout` (design doc §3).
  """
  defdelegate register_name(name, pid), to: :dgen_registry

  @doc """
  Removes the registration for `{registry_name, logical_name}`. Returns `:ok`.

  Works like the register path. The local member updates its snapshot right
  away and forwards the removal to the leader. `:ok` normally means the leader
  committed it. If the leader is unreachable, the member holds on to the
  removal and sends it again when leadership changes or the member rejoins. An
  explicit unregister is never silently lost (design doc, Non-goal 5).
  """
  defdelegate unregister_name(name), to: :dgen_registry

  @doc """
  Looks up `name` in the local snapshot. The calling process reads the local
  member's ETS table directly, without locks or a round-trip to the member.

  Never blocks on the leader or the backend. On a follower node it may be
  briefly stale, between a registration on another node and the replication
  message arriving here. Returns `nil` (`:undefined`) if not found.
  """
  defdelegate whereis_name(name), to: :dgen_registry

  @doc """
  Sends `msg` to the process registered as `name`, returning the pid.

  Raises `{:badarg, {name, msg}}` if the name is not registered.
  Called internally by OTP for `{:via, …}` routing.
  """
  defdelegate send(name, msg), to: :dgen_registry

  @doc """
  Registers `pid` under `{registry_name, logical_name}` with metadata, in a
  single commit by the leader.

  `meta_spec` is `%{index: map(), data: term()}`. Both keys are optional, and
  default to `%{}` and `nil`. Answers the same way as `register_name/2`.
  """
  defdelegate register_name(name, pid, meta_spec), to: :dgen_registry

  @doc """
  Replaces the metadata of the existing registration `{registry_name, logical_name}`.

  `meta_spec` is `%{index: map(), data: term()}`. Both keys are optional. This
  replaces the metadata rather than merging, so an omitted key becomes `%{}` or
  `nil`. Returns `:ok`, or `{:error, :not_registered}` if the name is not
  currently registered. Returns `{:error, :no_leader}` if no leader is elected,
  or if leadership changed before the leader answered. In that case, retry.
  """
  defdelegate set_metadata(name, meta_spec), to: :dgen_registry

  @doc """
  Reads a registration's metadata from the local snapshot. Like
  `whereis_name/1`, the calling process reads it directly without locks.

  Returns `{:ok, %{pid: pid, index: map, data: term}}` for a registered name,
  or `nil` (`:undefined`) if not registered.
  """
  defdelegate get_metadata(name), to: :dgen_registry

  @doc """
  Reads a registration's metadata through the leader. The answer is
  authoritative and never stale.

  Returns the same shape as `get_metadata/1`.
  """
  defdelegate get_metadata_consistent(name), to: :dgen_registry

  @doc """
  Finds every registration in `registry_name` whose indexed metadata matches
  *all* of `constraints`. Each constraint is an exact equality.

  Reads the snapshot on the local member. It never sees a group commit that is
  only partly applied. Returns a list of `%{name:, pid:, index:, data:}`
  matches, or `{:error, :empty_query}` if `constraints` is empty.
  """
  defdelegate query(registry_name, constraints), to: :dgen_registry

  @doc """
  Like `query/2`, but asks the leader, so the result is authoritative and never stale.
  """
  defdelegate query_consistent(registry_name, constraints), to: :dgen_registry

  @doc """
  Creates or updates the durable presence subscription `sub_id`. It keeps the
  processes matching `notify` up to date with the set of processes matching `watch`.

  `watch` and `notify` are queries. Each is `{:all, %{attr => value}}`, or a bare map,
  which is read as `{:all, map}`. `sub_id` is any application term. Subscribing again
  with the same `sub_id` replaces the earlier subscription. Whenever a committed batch
  changes which processes match `watch`, every process currently matching `notify`
  receives one message `{:dgen_presence, sub_id, events}`. `events` is a non-empty list
  of `{:joined, name, pid}` and `{:left, name, pid}` tuples. When a process enters the
  notify set, it first gets the current `watch` set as `:joined` events, and then the
  changes after that. This happens both at subscribe time and when a process registers
  into `notify` later. So you can create the subscription before any viewer exists.

  The subscription is **durable**. The elector stores it in its backend state, so it
  outlives the cluster. If you tie its lifetime to a database entity, presence comes
  back intact after the cluster scales to zero and back. The request is sent as a
  durable cast and applied asynchronously, so `:ok` only means it was accepted.
  `subscriptions/1` shows it shortly after. Returns `:ok`, or `{:error, :empty_query}`
  if either query is empty.
  """
  defdelegate subscribe(registry_name, sub_id, watch, notify), to: :dgen_registry

  @doc """
  Removes the durable presence subscription `sub_id` created by `subscribe/4`. Returns
  `:ok`. It is safe to call more than once, and it is applied asynchronously.
  """
  defdelegate unsubscribe(registry_name, sub_id), to: :dgen_registry

  @doc """
  Removes all durable presence subscriptions from the registry. Returns `:ok`. The
  removal is applied asynchronously.
  """
  defdelegate unsubscribe_all(registry_name), to: :dgen_registry

  @doc """
  Returns the registry's durable presence subscriptions as
  `%{sub_id => {watch, notify}}`. Every node sees the same set.
  """
  defdelegate subscriptions(registry_name), to: :dgen_registry

  @doc """
  Removes all of the registry's durable backend state: membership, the leader and
  version keys, and subscriptions. Returns `:ok`.

  Stop the registry supervisor first, then call this with the tenant you started it
  with. A running elector would write the keys again from its in-memory state. This
  clears only this registry's keys, not those of other registries in the same tenant.
  """
  defdelegate delete(registry_name, tenant), to: :dgen_registry

  @doc """
  Looks up `name` through the leader.

  Returns the authoritative pid for `{registry_name, logical_name}`, or
  `nil` (`:undefined`) if not registered. It costs more than `whereis_name/1`,
  but it is never stale.
  """
  defdelegate whereis_name_consistent(name), to: :dgen_registry

  @doc "Returns the current leader member id, or `:undefined` if none is elected."
  defdelegate get_leader(registry_name), to: :dgen_registry

  @doc "Returns the current leader epoch. Increments each time a new leader is elected."
  defdelegate get_epoch(registry_name), to: :dgen_registry

  @doc "Returns the list of all current member ids `[{node, member_atom}]` in the registry."
  defdelegate get_members(registry_name), to: :dgen_registry

  @doc """
  Returns whether the registry on this node can serve registrations right now.
  That means a leader is known and this member has synced. To wait for it, use
  `await_ready/2`.
  """
  defdelegate ready(registry_name), to: :dgen_registry

  @doc "Blocks until `ready/1` holds, or 5s elapses. Returns `:ok` or `{:error, :timeout}`."
  defdelegate await_ready(registry_name), to: :dgen_registry

  @doc """
  Blocks until the registry on this node is ready to serve registrations, or
  `timeout` ms elapses. Returns `:ok` or `{:error, :timeout}`. Use after starting
  or joining a cluster to wait out leader election and the join handoff.
  """
  defdelegate await_ready(registry_name, timeout), to: :dgen_registry

  @doc """
  Returns how the member on this node sees the registry, as a map with `:member_id`,
  `:leader`, `:is_leader`, `:epoch`, `:synced`, and `:applied_version`.

  This is what the member *believes*, which can differ from the committed answer
  that `get_leader/1` gets from the elector. For example, a deposed leader that has
  not yet heard about the handoff still reports `is_leader: true` here. It has no
  side effects. Returns `:undefined` if the member is not running.
  """
  defdelegate status(registry_name), to: :dgen_registry

  @doc """
  Returns the registered name of the member process for the given registry name.

  Today this returns its argument unchanged, because the member is registered
  under `name` itself.
  """
  defdelegate member_name(registry_name), to: :dgen_registry

  @doc """
  Returns the ETS table name holding the registry's local `name => pid` replica.

  Today this returns its argument unchanged. It is exported for introspection.
  Ordinary callers should use `whereis_name/1` or `get_metadata/1` rather than
  reading the table directly.
  """
  defdelegate names_table(registry_name), to: :dgen_registry

  @doc """
  Returns the current elector pid for `registry_name`, or `nil` (`:undefined`)
  if the member, and so the elector, cannot be found right now.

  The elector has no registered name. We find it through the supervisor it
  shares with the member, which is a local lookup with no network hop.
  `get_leader/1`, `get_epoch/1`, and `get_members/1` use it internally, and it is
  exported for introspection.
  """
  defdelegate elector_pid(registry_name), to: :dgen_registry
end

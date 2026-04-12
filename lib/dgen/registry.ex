defmodule DGen.Registry do
  @moduledoc """
  OTP-compatible process registry backed by the configured storage backend.

  Processes started with a `{:via, DGen.Registry, {name, logical_name}}` tuple
  can be addressed by name across an Erlang/Elixir cluster.

  ## Starting a registry

      {:ok, _} = DGen.Registry.start_link(:my_registry, tenant)

  To embed the registry supervisor in your own supervision tree under a
  different registered name:

      DGen.Registry.start_link(:my_sup, :my_registry, tenant)

  ## Via-tuple usage

      via = {:via, DGen.Registry, {:my_registry, :user_service}}
      GenServer.start_link(MyServer, [], name: via)
      GenServer.call(via, :ping)

  ## Consistency model

  `register_name/2`, `unregister_name/1`, and `whereis_name_consistent/1`
  route through the elected leader. `whereis_name/1` (used by the OTP
  via-tuple machinery) is served from the local member's in-memory map —
  no network hop or backend round-trip. There is a short replication window
  after registration where a follower node's snapshot read may still return
  `nil`.
  """

  @doc "Starts the registry supervisor registered as `name`."
  defdelegate start_link(name, tenant), to: :dgen_registry

  @doc """
  Starts the registry supervisor registered as `sup_name`, using `name` as
  the registry name. Use this form to embed the registry under an existing
  supervision tree with a distinct supervisor name.
  """
  defdelegate start_link(sup_name, name, tenant), to: :dgen_registry

  @doc """
  Registers `pid` under `{registry_name, logical_name}`.

  Routes through the local member, which forwards to the leader if needed.
  Returns `:yes` on success, `:no` if the name is already taken or no leader
  is currently elected.
  """
  defdelegate register_name(name, pid), to: :dgen_registry

  @doc """
  Removes the registration for `{registry_name, logical_name}`.

  Fire-and-forget: routes through the local member, which forwards to the
  leader. The local member's snapshot is updated immediately.
  """
  defdelegate unregister_name(name), to: :dgen_registry

  @doc """
  Snapshot read from the local member's in-memory map.

  Never blocks on the leader or the backend. May be slightly stale on
  follower nodes in the brief window between a remote registration and the
  replication cast arriving. Returns `nil` (`:undefined`) if not found.
  """
  defdelegate whereis_name(name), to: :dgen_registry

  @doc """
  Sends `msg` to the process registered as `name`, returning the pid.

  Raises `{:badarg, {name, msg}}` if the name is not registered.
  Called internally by OTP for `{:via, …}` routing.
  """
  defdelegate send(name, msg), to: :dgen_registry

  @doc """
  Consistent read routed through the leader.

  Returns the authoritative pid for `{registry_name, logical_name}`, or
  `nil` (`:undefined`) if not registered. More expensive than `whereis_name/1`
  but never stale.
  """
  defdelegate whereis_name_consistent(name), to: :dgen_registry

  @doc "Returns the current leader member id, or `:undefined` if none is elected."
  defdelegate get_leader(registry_name), to: :dgen_registry

  @doc "Returns the list of all current member ids `[{node, member_atom}]` in the registry."
  defdelegate get_members(registry_name), to: :dgen_registry

  @doc "Returns the registered name of the elector process for the given registry name."
  defdelegate elector_name(registry_name), to: :dgen_registry

  @doc "Returns the registered name of the member process for the given registry name."
  defdelegate member_name(registry_name), to: :dgen_registry
end

defmodule DGenServer do
  @callback init(args :: term) ::
              {:ok, state :: term()} | {:error, reason :: term()}

  @callback key(state :: term) ::
              key :: tuple

  @callback handle_cast(msg :: term, state :: term) ::
              {:noreply, new_state :: term}

  @callback handle_call(request :: term, from :: term, state :: term) ::
              {:reply, reply :: term, new_state :: term} | {:noreply, new_state :: term}

  @callback handle_info(info :: term, state :: term) ::
              {:noreply, new_state :: term}

  @optional_callbacks key: 1, handle_cast: 2, handle_call: 3, handle_info: 2

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @behaviour DGenServer

      if not Module.has_attribute?(__MODULE__, :doc) do
        @doc """
        Returns a specification to start this module under a supervisor.

        See `Supervisor`.
        """
      end

      def child_spec(init_arg) do
        default = %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [init_arg]}
        }

        Supervisor.child_spec(default, unquote(Macro.escape(opts)))
      end
    end
  end

  def start_link(module, init_arg, options \\ []) do
    do_start(:link, module, init_arg, options)
  end

  def start(module, init_arg, options \\ []) do
    do_start(:nolink, module, init_arg, options)
  end

  defp do_start(link, module, init_arg, options) when is_atom(module) and is_list(options) do
    case {link, Keyword.pop(options, :name)} do
      {:link, {nil, opts}} ->
        :dgen_server.start_link(module, init_arg, opts)

      {:nolink, {nil, opts}} ->
        :dgen_server.start(module, init_arg, opts)

      {:link, {atom, opts}} when is_atom(atom) ->
        :dgen_server.start_link({:local, atom}, module, init_arg, opts)

      {:nolink, {atom, opts}} when is_atom(atom) ->
        :dgen_server.start({:local, atom}, module, init_arg, opts)

      {:link, {{:global, _term} = tuple, opts}} ->
        :dgen_server.start_link(tuple, module, init_arg, opts)

      {:nolink, {{:global, _term} = tuple, opts}} ->
        :dgen_server.start(tuple, module, init_arg, opts)

      {:link, {{:via, via_module, _term} = tuple, opts}} when is_atom(via_module) ->
        :dgen_server.start_link(tuple, module, init_arg, opts)

      {:nolink, {{:via, via_module, _term} = tuple, opts}} when is_atom(via_module) ->
        :dgen_server.start_link(tuple, module, init_arg, opts)

      {_, {other, _}} ->
        raise ArgumentError, """
        expected :name option to be one of the following:

          * nil
          * atom
          * {:global, term}
          * {:via, module, term}

        Got: #{inspect(other)}
        """
    end
  end

  defdelegate cast(server, msg), to: :dgen_server
  defdelegate cast_k(server, msg), to: :dgen_server
  defdelegate priority_cast(server, msg), to: :dgen_server
  defdelegate priority_call(server, msg), to: :dgen_server
  defdelegate priority_call(server, msg, timeout), to: :dgen_server
  defdelegate call(server, msg), to: :dgen_server
  defdelegate call(server, msg, timeout), to: :dgen_server
  defdelegate kill(server, reason), to: :dgen_server
end

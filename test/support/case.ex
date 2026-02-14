defmodule DGen.Case do
  @moduledoc false
  use ExUnit.CaseTemplate

  @test_sandbox_name "DGen.Case"
  @test_case_dir_name "DGen.Case"

  def init() do
    :dgen_config.init()
    b = :dgen_config.backend()

    db = b.sandbox_open(@test_sandbox_name)
    :persistent_term.put({__MODULE__, :db}, db)

    root = b.dir_root(node_prefix: <<0xFE>>, content_prefix: <<>>)
    :persistent_term.put({__MODULE__, :root}, root)

    dir = b.dir_create_or_open(db, root, @test_case_dir_name)
    :persistent_term.put({__MODULE__, :dir}, dir)
  end

  setup do
    b = :dgen_config.backend()
    db = :persistent_term.get({__MODULE__, :db})
    dir = :persistent_term.get({__MODULE__, :dir})

    id = Base.encode16(:crypto.strong_rand_bytes(16))
    case_dir = b.dir_create(db, dir, id)

    on_exit(fn ->
      b.dir_remove(db, dir, id)
    end)

    {:ok, [tenant: {db, case_dir}]}
  end
end

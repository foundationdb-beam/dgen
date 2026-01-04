defmodule DGen.Case do
  @moduledoc false
  use ExUnit.CaseTemplate

  @test_sandbox_name "DGen.Case"
  @test_case_dir_name "DGen.Case"

  def init() do
    db = :erlfdb_sandbox.open(@test_sandbox_name)
    :persistent_term.put({__MODULE__, :db}, db)

    root = :erlfdb_directory.root(node_prefix: <<0xFE>>, content_prefix: <<>>)
    :persistent_term.put({__MODULE__, :root}, root)

    dir = :erlfdb_directory.create_or_open(db, root, @test_case_dir_name)
    :persistent_term.put({__MODULE__, :dir}, dir)
  end

  setup do
    db = :persistent_term.get({__MODULE__, :db})
    dir = :persistent_term.get({__MODULE__, :dir})

    id = Base.encode16(:crypto.strong_rand_bytes(16))
    case_dir = :erlfdb_directory.create(db, dir, id)

    on_exit(fn ->
      :erlfdb_directory.remove(db, dir, id)
    end)

    {:ok, [tenant: {db, case_dir}]}
  end
end

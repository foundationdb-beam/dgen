defmodule DGenModStateCodec.Test do
  use DGen.Case, async: true

  defp base_key, do: {<<"test">>, <<"codec">>, <<"s">>}

  # -- term encoding (fallback) --

  describe "term encoding" do
    test "round-trips an integer", %{tenant: tenant} do
      key = base_key()
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, 42)
      assert {:ok, 42} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "round-trips a binary", %{tenant: tenant} do
      key = base_key()
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, <<"hello world">>)
      assert {:ok, <<"hello world">>} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "round-trips a tuple", %{tenant: tenant} do
      key = base_key()
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, {:ok, [1, 2, 3]})
      assert {:ok, {:ok, [1, 2, 3]}} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "round-trips a map with non-atom keys as term", %{tenant: tenant} do
      key = base_key()
      state = %{<<"binary_key">> => 1, 42 => :val}
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, state)
      assert {:ok, ^state} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "round-trips a list without id keys as term", %{tenant: tenant} do
      key = base_key()
      state = [%{name: "a"}, %{name: "b"}]
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, state)
      assert {:ok, ^state} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "round-trips a large binary that requires chunking", %{tenant: tenant} do
      key = base_key()
      # 250KB binary, needs 3 chunks at 100KB each
      large = :crypto.strong_rand_bytes(250_000)
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, large)
      assert {:ok, ^large} = :dgen_mod_state_codec.get(tenant, key)
    end
  end

  # -- assigns map encoding --

  describe "assigns map encoding" do
    test "round-trips an empty map", %{tenant: tenant} do
      key = base_key()
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, %{})
      assert {:ok, %{}} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "round-trips a simple assigns map", %{tenant: tenant} do
      key = base_key()
      state = %{count: 5, name: <<"alice">>, active: true}
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, state)
      assert {:ok, ^state} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "round-trips nested assigns maps", %{tenant: tenant} do
      key = base_key()
      state = %{user: %{name: <<"bob">>, age: 30}, settings: %{theme: :dark}}
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, state)
      assert {:ok, ^state} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "assigns map with a term value that needs chunking", %{tenant: tenant} do
      key = base_key()
      large = :crypto.strong_rand_bytes(250_000)
      state = %{data: large, label: <<"test">>}
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, state)
      assert {:ok, ^state} = :dgen_mod_state_codec.get(tenant, key)
    end
  end

  # -- component list encoding --

  describe "component list encoding" do
    test "round-trips an empty list", %{tenant: tenant} do
      key = base_key()
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, [])
      assert {:ok, []} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "round-trips a single-item list", %{tenant: tenant} do
      key = base_key()
      state = [%{id: <<"a">>, value: 1}]
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, state)
      assert {:ok, ^state} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "preserves list order", %{tenant: tenant} do
      key = base_key()

      state = [
        %{id: <<"z">>, value: 3},
        %{id: <<"a">>, value: 1},
        %{id: <<"m">>, value: 2}
      ]

      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, state)
      assert {:ok, ^state} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "round-trips items with many fields", %{tenant: tenant} do
      key = base_key()

      state = [
        %{id: <<"header">>, type: :text, content: <<"Hello">>, visible: true},
        %{id: <<"footer">>, type: :text, content: <<"Bye">>, visible: false}
      ]

      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, state)
      assert {:ok, ^state} = :dgen_mod_state_codec.get(tenant, key)
    end
  end

  # -- nested encoding --

  describe "nested encoding" do
    test "assigns map containing a component list", %{tenant: tenant} do
      key = base_key()

      state = %{
        title: <<"My Page">>,
        components: [
          %{id: <<"header">>, text: <<"Hello">>},
          %{id: <<"footer">>, text: <<"Bye">>}
        ]
      }

      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, state)
      assert {:ok, ^state} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "assigns map containing multiple component lists", %{tenant: tenant} do
      key = base_key()

      state = %{
        widgets: [
          %{id: <<"w1">>, label: <<"Widget 1">>},
          %{id: <<"w2">>, label: <<"Widget 2">>}
        ],
        panels: [
          %{id: <<"p1">>, title: <<"Panel 1">>}
        ],
        count: 3
      }

      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, state)
      assert {:ok, ^state} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "three levels deep: map -> list -> map with nested map", %{tenant: tenant} do
      key = base_key()

      state = %{
        components: [
          %{id: <<"c1">>, config: %{color: :red, size: 10}},
          %{id: <<"c2">>, config: %{color: :blue, size: 20}}
        ]
      }

      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, state)
      assert {:ok, ^state} = :dgen_mod_state_codec.get(tenant, key)
    end
  end

  # -- clear --

  describe "clear/2" do
    test "clears all state", %{tenant: tenant} do
      key = base_key()
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, %{a: 1, b: 2})
      assert {:ok, _} = :dgen_mod_state_codec.get(tenant, key)
      :ok = :dgen_mod_state_codec.clear(tenant, key)
      assert {:error, :not_found} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "get returns not_found on empty key", %{tenant: tenant} do
      key = base_key()
      assert {:error, :not_found} = :dgen_mod_state_codec.get(tenant, key)
    end
  end

  # -- set/3 overwrites --

  describe "set/3 overwrite" do
    test "overwriting with a different type clears old data", %{tenant: tenant} do
      key = base_key()
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, %{a: 1, b: 2})
      assert {:ok, %{a: 1, b: 2}} = :dgen_mod_state_codec.get(tenant, key)

      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, 42)
      assert {:ok, 42} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "overwriting map with list", %{tenant: tenant} do
      key = base_key()
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, %{x: 1})
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, [%{id: <<"a">>, v: 1}])
      assert {:ok, [%{id: <<"a">>, v: 1}]} = :dgen_mod_state_codec.get(tenant, key)
    end
  end

  # -- set/4 diff-based updates --

  describe "set/4 diff map" do
    test "no-op when state is identical", %{tenant: tenant} do
      key = base_key()
      state = %{a: 1, b: 2}
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, state)
      :ok = :dgen_mod_state_codec.set(tenant, key, state, state)
      assert {:ok, ^state} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "updates a single changed key", %{tenant: tenant} do
      key = base_key()
      old = %{a: 1, b: 2, c: 3}
      new = %{a: 1, b: 99, c: 3}
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, old)
      :ok = :dgen_mod_state_codec.set(tenant, key, old, new)
      assert {:ok, ^new} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "adds a new key", %{tenant: tenant} do
      key = base_key()
      old = %{a: 1}
      new = %{a: 1, b: 2}
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, old)
      :ok = :dgen_mod_state_codec.set(tenant, key, old, new)
      assert {:ok, ^new} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "removes a key", %{tenant: tenant} do
      key = base_key()
      old = %{a: 1, b: 2}
      new = %{a: 1}
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, old)
      :ok = :dgen_mod_state_codec.set(tenant, key, old, new)
      assert {:ok, ^new} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "adds, removes, and updates keys simultaneously", %{tenant: tenant} do
      key = base_key()
      old = %{a: 1, b: 2, c: 3}
      new = %{a: 10, c: 3, d: 4}
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, old)
      :ok = :dgen_mod_state_codec.set(tenant, key, old, new)
      assert {:ok, ^new} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "diff nested map change", %{tenant: tenant} do
      key = base_key()
      old = %{user: %{name: <<"alice">>, age: 30}, status: :active}
      new = %{user: %{name: <<"alice">>, age: 31}, status: :active}
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, old)
      :ok = :dgen_mod_state_codec.set(tenant, key, old, new)
      assert {:ok, ^new} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "type change via diff triggers full rewrite", %{tenant: tenant} do
      key = base_key()
      old = %{a: 1}
      new = 42
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, old)
      :ok = :dgen_mod_state_codec.set(tenant, key, old, new)
      assert {:ok, 42} = :dgen_mod_state_codec.get(tenant, key)
    end
  end

  describe "set/4 diff list" do
    test "no-op when list is identical", %{tenant: tenant} do
      key = base_key()
      state = [%{id: <<"a">>, v: 1}, %{id: <<"b">>, v: 2}]
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, state)
      :ok = :dgen_mod_state_codec.set(tenant, key, state, state)
      assert {:ok, ^state} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "updates an item's field", %{tenant: tenant} do
      key = base_key()
      old = [%{id: <<"a">>, v: 1}, %{id: <<"b">>, v: 2}]
      new = [%{id: <<"a">>, v: 99}, %{id: <<"b">>, v: 2}]
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, old)
      :ok = :dgen_mod_state_codec.set(tenant, key, old, new)
      assert {:ok, ^new} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "removes an item", %{tenant: tenant} do
      key = base_key()
      old = [%{id: <<"a">>, v: 1}, %{id: <<"b">>, v: 2}, %{id: <<"c">>, v: 3}]
      new = [%{id: <<"a">>, v: 1}, %{id: <<"c">>, v: 3}]
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, old)
      :ok = :dgen_mod_state_codec.set(tenant, key, old, new)
      assert {:ok, ^new} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "adds an item at the end", %{tenant: tenant} do
      key = base_key()
      old = [%{id: <<"a">>, v: 1}]
      new = [%{id: <<"a">>, v: 1}, %{id: <<"b">>, v: 2}]
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, old)
      :ok = :dgen_mod_state_codec.set(tenant, key, old, new)
      assert {:ok, ^new} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "adds an item at the beginning", %{tenant: tenant} do
      key = base_key()
      old = [%{id: <<"b">>, v: 2}]
      new = [%{id: <<"a">>, v: 1}, %{id: <<"b">>, v: 2}]
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, old)
      :ok = :dgen_mod_state_codec.set(tenant, key, old, new)
      assert {:ok, ^new} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "adds an item in the middle", %{tenant: tenant} do
      key = base_key()
      old = [%{id: <<"a">>, v: 1}, %{id: <<"c">>, v: 3}]
      new = [%{id: <<"a">>, v: 1}, %{id: <<"b">>, v: 2}, %{id: <<"c">>, v: 3}]
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, old)
      :ok = :dgen_mod_state_codec.set(tenant, key, old, new)
      assert {:ok, ^new} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "preserves order after multiple diffs", %{tenant: tenant} do
      key = base_key()
      v1 = [%{id: <<"a">>, v: 1}, %{id: <<"b">>, v: 2}, %{id: <<"c">>, v: 3}]
      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, v1)

      # Remove b, add d at end
      v2 = [%{id: <<"a">>, v: 1}, %{id: <<"c">>, v: 3}, %{id: <<"d">>, v: 4}]
      :ok = :dgen_mod_state_codec.set(tenant, key, v1, v2)
      assert {:ok, ^v2} = :dgen_mod_state_codec.get(tenant, key)

      # Insert e between a and c
      v3 = [
        %{id: <<"a">>, v: 1},
        %{id: <<"e">>, v: 5},
        %{id: <<"c">>, v: 3},
        %{id: <<"d">>, v: 4}
      ]

      :ok = :dgen_mod_state_codec.set(tenant, key, v2, v3)
      assert {:ok, ^v3} = :dgen_mod_state_codec.get(tenant, key)
    end
  end

  # -- nested diff --

  describe "set/4 diff nested" do
    test "diff nested list inside map", %{tenant: tenant} do
      key = base_key()

      old = %{
        title: <<"Page">>,
        items: [
          %{id: <<"x">>, val: 1},
          %{id: <<"y">>, val: 2}
        ]
      }

      new = %{
        title: <<"Page">>,
        items: [
          %{id: <<"x">>, val: 1},
          %{id: <<"y">>, val: 99},
          %{id: <<"z">>, val: 3}
        ]
      }

      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, old)
      :ok = :dgen_mod_state_codec.set(tenant, key, old, new)
      assert {:ok, ^new} = :dgen_mod_state_codec.get(tenant, key)
    end

    test "diff map nested inside list items", %{tenant: tenant} do
      key = base_key()

      old = [
        %{id: <<"c1">>, config: %{color: :red, size: 10}},
        %{id: <<"c2">>, config: %{color: :blue, size: 20}}
      ]

      new = [
        %{id: <<"c1">>, config: %{color: :green, size: 10}},
        %{id: <<"c2">>, config: %{color: :blue, size: 20}}
      ]

      :ok = :dgen_mod_state_codec.set(tenant, key, :undefined, old)
      :ok = :dgen_mod_state_codec.set(tenant, key, old, new)
      assert {:ok, ^new} = :dgen_mod_state_codec.get(tenant, key)
    end
  end

  # -- transactional usage --

  describe "transactional" do
    test "set and get within a single transaction", %{tenant: {db, dir}} do
      key = base_key()

      b = :dgen_config.backend()

      result =
        b.transactional(db, fn tx ->
          td = {tx, dir}
          :ok = :dgen_mod_state_codec.set(td, key, :undefined, %{a: 1})
          :dgen_mod_state_codec.get(td, key)
        end)

      assert {:ok, %{a: 1}} = result
    end
  end
end

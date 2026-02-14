defmodule DGenFracIndex.Test do
  use ExUnit.Case, async: true

  describe "first/0" do
    test "returns a single-character binary" do
      result = :dgen_frac_index.first()
      assert is_binary(result)
      assert byte_size(result) == 1
    end
  end

  describe "n_first/1" do
    test "returns N sorted binaries" do
      keys = :dgen_frac_index.n_first(5)
      assert length(keys) == 5
      assert keys == Enum.sort(keys)
    end

    test "single key" do
      [key] = :dgen_frac_index.n_first(1)
      assert key == :dgen_frac_index.first()
    end

    test "all keys are unique" do
      keys = :dgen_frac_index.n_first(100)
      assert length(keys) == length(Enum.uniq(keys))
    end

    test "keys are strictly ascending" do
      keys = :dgen_frac_index.n_first(20)

      keys
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.each(fn [a, b] -> assert a < b end)
    end
  end

  describe "between/2" do
    test "returns a key strictly between the two inputs" do
      keys = :dgen_frac_index.n_first(2)
      [a, b] = keys
      mid = :dgen_frac_index.between(a, b)
      assert mid > a
      assert mid < b
    end

    test "repeated bisection stays sorted" do
      [a, b] = :dgen_frac_index.n_first(2)

      # Bisect 50 times between a and the last midpoint
      mids =
        Enum.reduce(1..50, [a, b], fn _, [lo, hi] ->
          mid = :dgen_frac_index.between(lo, hi)
          assert mid > lo
          assert mid < hi
          [lo, mid]
        end)

      assert hd(mids) < List.last(mids)
    end

    test "works with adjacent keys from n_first" do
      keys = :dgen_frac_index.n_first(10)

      keys
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.each(fn [a, b] ->
        mid = :dgen_frac_index.between(a, b)
        assert mid > a
        assert mid < b
      end)
    end
  end

  describe "before/1" do
    test "returns a key less than the input" do
      key = :dgen_frac_index.first()
      result = :dgen_frac_index.before(key)
      assert result < key
    end

    test "repeated before stays sorted" do
      key = :dgen_frac_index.first()

      Enum.reduce(1..20, key, fn _, current ->
        prev = :dgen_frac_index.before(current)
        assert prev < current
        prev
      end)
    end
  end

  describe "after_/1" do
    test "returns a key greater than the input" do
      key = :dgen_frac_index.first()
      result = :dgen_frac_index.after_(key)
      assert result > key
    end

    test "repeated after stays sorted" do
      key = :dgen_frac_index.first()

      Enum.reduce(1..20, key, fn _, current ->
        next = :dgen_frac_index.after_(current)
        assert next > current
        next
      end)
    end
  end

  describe "n_between/3" do
    test "generates N sorted keys between two bounds" do
      [a, b] = :dgen_frac_index.n_first(2)
      mids = :dgen_frac_index.n_between(a, b, 5)
      assert length(mids) == 5
      assert mids == Enum.sort(mids)
      assert hd(mids) > a
      assert List.last(mids) < b
    end

    test "single key between bounds" do
      [a, b] = :dgen_frac_index.n_first(2)
      [mid] = :dgen_frac_index.n_between(a, b, 1)
      assert mid > a
      assert mid < b
    end

    test "all generated keys are unique" do
      [a, b] = :dgen_frac_index.n_first(2)
      mids = :dgen_frac_index.n_between(a, b, 50)
      assert length(mids) == length(Enum.uniq(mids))
    end
  end
end

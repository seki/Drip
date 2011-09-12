require 'test/unit'
require '../lib/drip.rb'
require 'fileutils'

class TestDrip < Test::Unit::TestCase
  def setup
    @drip = Drip.new(nil)
  end

  def test_time_travel
    @drip.write('age' => 1)
    @drip.write('age' => 2)
    @drip.write('age' => 3)
    oid, value = @drip.older(nil)
    assert_equal(value, 'age' => 3)
    oid, value = @drip.older(oid)
    assert_equal(value, 'age' => 2)
    oid, value = @drip.older(oid)
    assert_equal(value, 'age' => 1)
    oid, value = @drip.older(oid)
    assert_equal(oid, nil)
    assert_equal(value, nil)
    
    oid, value = @drip.newer(0)
    assert_equal(value, 'age' => 1)
    oid, value = @drip.newer(oid)
    assert_equal(value, 'age' => 2)
    oid, value = @drip.newer(oid)
    assert_equal(value, 'age' => 3)
    oid, value = @drip.newer(oid)
    assert_equal(oid, nil)
    assert_equal(value, nil)
  end

  def test_read
    11.times do |n|
      @drip.write("n=#{n}" => 'x' * n, n => n, "n" => n, :symbol => n)
    end
    ary = @drip.read(0, 3)
    assert_equal(ary.size, 3)
    assert_equal(ary[0][1]['n'], 0)
    assert_equal(ary[1][1]['n'], 1)
    assert_equal(ary[2][1]['n'], 2)
    ary = @drip.read(ary[2][0], 3)
    assert_equal(ary.size, 3)
    ary = @drip.read(ary[2][0], 3)
    assert_equal(ary.size, 3)
    ary = @drip.read(ary[2][0], 3)
    assert_equal(ary.size, 2)

    oid = @drip.write('latest', 'tag1', 'tag2')
    oid, value, *tags = @drip.newer(oid - 1)
    assert_equal(value, 'latest')
    assert_equal(tags, ['tag1', 'tag2'])
  end

  def test_symbol_is_not_tag
    @drip.write({:symbol => :symbol, 'string' => :string}, :symbol, 'string')
    assert_raise(ArgumentError) {@drip.read_tag(0, :symbol, 1, 0)}
    oid, value = @drip.older(@drip.time_to_key(Time.now))
    assert_equal(value, {:symbol => :symbol, 'string' => :string})
  end

  def test_number_is_not_tag
    @drip.write({5 => :five, 'string' => :string}, 5, 'string')
    assert_equal(@drip.read_tag(0, 'string', 1, 0).size, 1)
    assert_raise(ArgumentError) {@drip.read_tag(0, 5, 1, 0)}
  end
  
  def test_older_now_is_newest
    @drip.write('age' => 1)
    @drip.write('age' => 2)
    @drip.write('age' => 3)
    oid, value = @drip.older(nil)
    assert_equal(value, 'age' => 3)
    oid, value = @drip.older(@drip.time_to_key(Time.now))
    assert_equal(value, 'age' => 3)

    # newer(past)
    assert_equal(@drip.newer(0)[1], 'age' => 1)
    assert_equal(@drip.newer(0)[1], 'age' => 1)
  end

  def test_read_tag
    3.times do |n|
      @drip.write({'n' => n}, 'n')
      @drip.write({'n' => n, '2' => n * 2}, 'n', '2')
      @drip.write({'n' => n, '2' => n * 2, '3' => n * 3}, 'n', '2', '3')
    end
    
    ary = @drip.read_tag(0, 'n', 10)
    assert_equal(ary.size, 9)
    assert_equal(ary[0][1]['n'], 0)
    assert_equal(ary[0][2], 'n')
    assert_equal(ary[2][4], '3')
  end

  def test_head
    10.times do |n|
      @drip.write(n)
    end
    
    ary = @drip.head(3)
    assert_equal(ary.size, 3)
    assert_equal(ary[0][1], 7)
    assert_equal(ary[2][1], 9)
  end
  
  def test_after
    assert_equal(@drip.write_after(Time.at(1), 1), 1000000)
    assert_equal(@drip.write_after(Time.at(2), 2), 2000000)
    assert_equal(@drip.write_after(Time.at(3), 3), 3000000)
    assert_equal(@drip.write_after(Time.at(4), 4), 4000000)
    assert_equal(@drip.write_after(Time.at(4), 5), 4000001)
    assert_equal(@drip.write_after(Time.at(2), 6), 4000002)
    assert_equal(@drip.write_after(Time.at(5), 6), 5000000)
    assert_equal(@drip.write_after(Time.at(5), 7), 5000001)
    assert_equal(@drip.write_at(Time.at(6), 8), 6000000)
    assert_raise(RuntimeError) {@drip.write_at(Time.at(6), 8)}
    assert_equal(@drip.write_after(Time.at(5), 8), 6000001)
  end
  
  def test_duplicate_tags
    oid = @drip.write('dup', 'hello', 'hello', 'hello')
    assert_equal(@drip[oid], ['dup', 'hello'])
  end
end

class TestDripUsingStorage < TestDrip
  def remove_drip(dir='test_db')
    FileUtils.rm_r(dir, :force => true)
  end

  def setup
    remove_drip
    @drip = Drip.new('test_db')
  end

  def teardown
    remove_drip
  end
  
  def test_twice
    11.times do |n|
      @drip.write("n=#{n}" => 'x' * n, n => n, "n" => n, :symbol => n)
    end

    drip = Drip.new('test_db')
    ary = drip.read(0, 3)
    assert_equal(ary.size, 3)
    assert_equal(ary[0][1]['n'], 0)
    assert_equal(ary[1][1]['n'], 1)
    assert_equal(ary[2][1]['n'], 2)
    ary = drip.read(ary[2][0], 3)
    assert_equal(ary.size, 3)
    ary = drip.read(ary[2][0], 3)
    assert_equal(ary.size, 3)
    ary = drip.read(ary[2][0], 3)
    assert_equal(ary.size, 2)
  end

  def ignore_test_huge
    str = File.read(__FILE__)

    10.times do 
      1000.times do |n|
        @drip.write(str, "n=#{n}")
      end
      @drip = Drip.new('test_db')
    end

    assert_equal(10000, @drip.read(0, 12000, 10000).size)
  end
end

class TestImmutableDrip < Test::Unit::TestCase
  def test_bsearch
    im = Drip::ImmutableDrip.new

    assert_equal(0, im.lower_boundary([], 'c'))
    assert_equal(0, im.upper_boundary([], 'c'))

    ary = %w(a b c c c d e f).collect {|x| [x]}

    assert_equal(0, im.lower_boundary(ary, ''))
    assert_equal(0, im.lower_boundary(ary, 'a'))
    assert_equal(1, im.lower_boundary(ary, 'b'))
    assert_equal(2, im.lower_boundary(ary, 'c'))
    assert_equal(5, im.lower_boundary(ary, 'd'))
    assert_equal(6, im.lower_boundary(ary, 'e'))
    assert_equal(7, im.lower_boundary(ary, 'f'))
    assert_equal(8, im.lower_boundary(ary, 'g'))

    assert_equal(0, im.upper_boundary(ary, ''))
    assert_equal(1, im.upper_boundary(ary, 'a'))
    assert_equal(2, im.upper_boundary(ary, 'b'))
    assert_equal(5, im.upper_boundary(ary, 'c'))
    assert_equal(6, im.upper_boundary(ary, 'd'))
    assert_equal(7, im.upper_boundary(ary, 'e'))
    assert_equal(8, im.upper_boundary(ary, 'f'))
    assert_equal(8, im.upper_boundary(ary, 'g'))
  end

  def test_fetch
    ary = []
    ary << [21, ['a']]
    ary << [39, ['b']]
    ary << [60, ['c']]
    ary << [99, ['d']]
    
    im = Drip::ImmutableDrip.new(ary, [])
    assert_equal(nil, im.fetch(20))
    assert_equal(['a'], im.fetch(21))
    assert_equal(nil, im.fetch(23))
    assert_equal(['b'], im.fetch(39))
    assert_equal(['d'], im.fetch(99))
    assert_equal(nil,  im.fetch(990))
  end
end

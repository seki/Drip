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

  def test_latest?
    key = @drip.write(:start)
    10.times do |n|
      @drip.write(n)
    end
    assert_equal(@drip.latest?(key), false)
    key = @drip.write(:stop)
    assert_equal(@drip.latest?(key), true)

    key = @drip.write(:tag_start, 'tag')
    @drip.write(:tag, 'ignore tag')
    assert_equal(@drip.latest?(key, 'tag'), true)
    @drip.write(:tag, 'tag')
    assert_equal(@drip.latest?(key, 'tag'), false)
  end

  def test_write_if_latest
    t1 = @drip.write('t1', 't1')
    t2 = @drip.write('t2', 't2')
    t3 = @drip.write('t3', 't3')
    assert_equal(@drip.latest?(t1, 't1'), true)
    assert(@drip.write_if_latest([['t1', t1],
                                  ['t2', t2],
                                  ['t3', t3]], 'hello', 't1'))
    assert_equal(@drip.latest?(t1, 't1'), false)
    assert_equal(@drip.write_if_latest([['t1', t1],
                                        ['t2', t2],
                                        ['t3', t3]], 'hello', 't1'), nil)
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

  def test_twice_latest?
    assert_equal(@drip.latest?(1), false)
    tag1 = @drip.write('tag1', 'tag1')
    assert_equal(@drip.latest?(tag1), true)
    @drip.write('nop', 'tag1')
    @drip.write('nop', 'tag1')
    tag2 = @drip.write('tag2', 'tag1')
    assert_equal(@drip.latest?(1), false)
    drip = Drip.new('test_db')
    assert_equal(drip.latest?(1), false)
    assert_equal(drip.latest?(tag1, 'tag1'), false)
    assert_equal(drip.latest?(tag2, 'tag1'), true)
    assert_equal(drip.latest?(tag2, 'tag0'), false)
  end
  
  def test_twice
    11.times do |n|
      @drip.write("n=#{n}" => 'x' * n, n => n, "n" => n, :symbol => n)
    end
    
    drip = Drip.new('test_db')
    ary = drip.head(3)
    assert_equal(ary.size, 3)
    assert_equal(ary[0][1]['n'], 8)
    assert_equal(ary[1][1]['n'], 9)
    assert_equal(ary[2][1]['n'], 10)
    ary = drip.head(1)
    assert_equal(ary.size, 1)
    assert_equal(ary[0][1]['n'], 10)
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
    ab = Drip::ArrayBsearch

    assert_equal(0, ab.lower_boundary([], 'c'))
    assert_equal(0, ab.upper_boundary([], 'c'))

    ary = %w(a b c c c d e f).collect {|x| [x]}

    assert_equal(0, ab.lower_boundary(ary, ''))
    assert_equal(0, ab.lower_boundary(ary, 'a'))
    assert_equal(1, ab.lower_boundary(ary, 'b'))
    assert_equal(2, ab.lower_boundary(ary, 'c'))
    assert_equal(5, ab.lower_boundary(ary, 'd'))
    assert_equal(6, ab.lower_boundary(ary, 'e'))
    assert_equal(7, ab.lower_boundary(ary, 'f'))
    assert_equal(8, ab.lower_boundary(ary, 'g'))

    assert_equal(0, ab.upper_boundary(ary, ''))
    assert_equal(1, ab.upper_boundary(ary, 'a'))
    assert_equal(2, ab.upper_boundary(ary, 'b'))
    assert_equal(5, ab.upper_boundary(ary, 'c'))
    assert_equal(6, ab.upper_boundary(ary, 'd'))
    assert_equal(7, ab.upper_boundary(ary, 'e'))
    assert_equal(8, ab.upper_boundary(ary, 'f'))
    assert_equal(8, ab.upper_boundary(ary, 'g'))
  end

  def add_to_gen(gen, key, value, *tag)
    gen.add(key, [value, *tag], *tag)
  end

  def test_fetch_and_read_wo_tag
    gen = Drip::ImmutableDrip::Generator.new
    add_to_gen(gen, 21, 'a')
    add_to_gen(gen, 99, 'd', 'tag')
    add_to_gen(gen, 39, 'b')
    add_to_gen(gen, 60, 'c', 'tag')

    im = gen.generate

    assert_equal(nil, im.fetch(20))
    assert_equal(['a'], im.fetch(21))
    assert_equal(nil, im.fetch(23))
    assert_equal(['b'], im.fetch(39))
    assert_equal(['d', 'tag'], im.fetch(99))
    assert_equal(nil,  im.fetch(990))
    
    assert_equal([[21, 'a']], im.read(0))
    assert_equal([[39, 'b']], im.read(21))
    assert_equal([[60, 'c', 'tag']], im.read(39))
    assert_equal([[99, 'd', 'tag']], im.read(60))
    assert_equal([], im.read(99))
    
    assert_equal([[21, 'a'], [39, 'b']], im.read(0, 2))
    assert_equal([[60, 'c', 'tag'], [99, 'd', 'tag']], im.read(39, 10))
    
    assert_equal([[99, 'd', 'tag']], im.head)
    assert_equal([[60, 'c', 'tag'], [99, 'd', 'tag']], im.head(2))
    assert_equal([[21, 'a'], [39, 'b'], [60, 'c', 'tag'], [99, 'd', 'tag']],
                 im.head(10))
    
    assert_equal([99, 'd', 'tag'], im.older(nil))
    assert_equal([60, 'c', 'tag'], im.older(99))
    assert_equal([39, 'b'], im.older(60))
    assert_equal([21, 'a'], im.older(39))
    assert_equal(nil, im.older(21))
    
    assert_equal([21, 'a'], im.newer(0))
    assert_equal([39, 'b'], im.newer(21))
    assert_equal([60, 'c', 'tag'], im.newer(39))
    assert_equal([99, 'd', 'tag'], im.newer(60))
    assert_equal(nil, im.newer(99))
  end

  def test_read_w_tag
    gen = Drip::ImmutableDrip::Generator.new
    add_to_gen(gen, 21, 'a')
    add_to_gen(gen, 39, 'b', 'b', 'tag')
    add_to_gen(gen, 60, 'c', 'c', 'tag')
    add_to_gen(gen, 99, 'd', 'tag', 'd')
    add_to_gen(gen, 159, 'e', 'tag2', 'e')
    im = gen.generate
    
    assert_equal([[99, 'd', 'tag', 'd']], im.head(1, 'tag'))
    assert_equal([[99, 'd', 'tag', 'd']], im.head(1, 'd'))
    assert_equal([[60, 'c', 'c', 'tag'], [99, 'd', 'tag', 'd']],
                 im.head(2, 'tag'))
    
    assert_equal([[159, 'e', 'tag2', 'e']], im.read_tag(1, 'tag2'))
    assert_equal([[99, 'd', 'tag', 'd']], im.read_tag(60, 'tag', 3))
    assert_equal([[60, 'c', 'c', 'tag'], [99, 'd', 'tag', 'd']],
                 im.read_tag(39, 'tag', 3))
    assert_equal([[39, 'b', 'b', 'tag'],
                  [60, 'c', 'c', 'tag'],
                  [99, 'd', 'tag', 'd']],
                 im.read_tag(21, 'tag', 5))
    assert_equal([[39, 'b', 'b', 'tag'],
                  [60, 'c', 'c', 'tag']],
                  im.read_tag(21, 'tag', 2))
    assert_equal([[39, 'b', 'b', 'tag'],
                  [60, 'c', 'c', 'tag']],
                  im.read_tag(38, 'tag', 2))
    
    assert_equal([99, 'd', 'tag', 'd'], im.older(nil, 'tag'))
    assert_equal([60, 'c', 'c', 'tag'], im.older(99, 'tag'))
    assert_equal(nil, im.older(21), 'tag')

    assert_equal([60, 'c', 'c', 'tag'], im.newer(39, 'tag'))
    assert_equal(nil, im.newer(99, 'tag'))
  end
end

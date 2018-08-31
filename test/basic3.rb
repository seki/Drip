require 'test/unit'
require '../lib/drip3.rb'
require 'fileutils'

class TestDrip < Test::Unit::TestCase
  def setup
    @drip = Drip3.new(nil)
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

    oid = @drip.write('latest', 'tag1')
    oid, value, *tags = @drip.newer(oid - 1)
    assert_equal(value, 'latest')
    assert_equal(tags, ['tag1'])
  end

  def test_symbol_is_not_tag
    assert_raise(ArgumentError){
      @drip.write({:symbol => :symbol, 'string' => :string}, :symbol)
    }
  end

  def test_number_is_not_tag
    assert_raise(ArgumentError){
      @drip.write({5 => :five, 'string' => :string}, 5)
    }
  end
  
  def test_older_now_is_newest
    @drip.write('age' => 1)
    @drip.write('age' => 2)
    @drip.write('age' => 3)
    oid, value, = @drip.older(nil)
    assert_equal(value, 'age' => 3)
    oid, value, = @drip.older(@drip.time_to_key(Time.now))
    assert_equal(value, 'age' => 3)

    # newer(past)
    assert_equal(@drip.newer(0)[1], 'age' => 1)
    assert_equal(@drip.newer(0)[1], 'age' => 1)
  end

  def test_read_tag
    3.times do |n|
      @drip.write({'n' => n}, 'n')
      @drip.write({'n' => n, '2' => n * 2}, 'n')
      @drip.write({'n' => n, '2' => n * 2, '3' => n * 3}, 'n')
    end
    
    ary = @drip.read_tag(0, 'n', 10)
    assert_equal(ary.size, 9)
    assert_equal(ary[0][1]['n'], 0)
    assert_equal(ary[0][2], 'n')
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
  
  def test_use_string_tag
    oid = @drip.write(:symbol, 'dup')
    assert_equal(@drip[oid], :symbol)
  end

  def test_latest?
    key = @drip.write(:start)

    assert_equal(@drip.latest?(key), true)

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
                                  ['t3', t3],
                                  ['t4', 0]], 'hello', 't1'))
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
    @drip = Drip3.new('test_db')
  end

  def teardown
    remove_drip
  end

  def test_tag_browse
    @drip.write(1, 't1')
    @drip.write(2, 't2')
    @drip.write(3, 't3')
    @drip.write(4, 't4')

    assert_equal(@drip.tag_next(''), 't1')
    assert_equal(@drip.tag_next('t1'), 't2')
    assert_equal(@drip.tag_next('t3'), 't4')
    assert_equal(@drip.tag_next('t5'), nil)

    assert_equal(@drip.tag_prev('u'), 't4')
    assert_equal(@drip.tag_prev('t2'), 't1')
    assert_equal(@drip.tag_prev('t1'), nil)
  end

  def test_twice_latest?
    assert_equal(@drip.latest?(1), false)
    tag1 = @drip.write('tag1', 'tag1')
    assert_equal(@drip.latest?(tag1), true)
    @drip.write('nop', 'tag1')
    @drip.write('nop', 'tag1')
    tag2 = @drip.write('tag2', 'tag1')
    assert_equal(@drip.latest?(1), false)
    drip = Drip3.new('test_db')
    assert_equal(drip.latest?(1), false)
    assert_equal(drip.latest?(tag1, 'tag1'), false)
    assert_equal(drip.latest?(tag2, 'tag1'), true)
    assert_equal(drip.latest?(tag2, 'tag0'), false)
  end
  
  def test_twice
    11.times do |n|
      @drip.write("n=#{n}" => 'x' * n, n => n, "n" => n, :symbol => n)
    end
    
    drip = Drip3.new('test_db')
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

  def test_huge
    str = File.read(__FILE__)

    10.times do 
      Thread.new do
        1000.times do |n|
          @drip.write(str, "n=#{n}")
        end
      end
    end

    assert_equal(@drip.read_tag(0, 'n=999', 10, 10).size, 10)

    assert_equal(10000, @drip.read(0, 12000, 10000).size)
  end
end


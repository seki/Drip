require 'test/unit'
require '../lib/drop.rb'
require 'fileutils'

class BasicTest < Test::Unit::TestCase
  def remove_drop(dir='test_db')
    FileUtils.rm_r(dir, :force => true)
  end

  def setup
    remove_drop
    @drop = Drop.new('test_db')
  end

  def teardown
    remove_drop
  end
  
  def test_tag
    10.times do |n|
      @drop.write("n=#{n}" => 'x' * n)
    end
    assert_equal(@drop.next_tag(''), 'n=0')
    assert_equal(@drop.next_tag('n=0'), 'n=1')
  end
end

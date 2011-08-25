require 'nkf'
require 'rbtree'
require 'my_drip'
require 'monitor'
require 'pp'


class Indexer
  def initialize(cursor=0)
    @drip = MyDrip
    @dict = Dict.new
    k, = @drip.head(1, 'rbcrowl-begin')[0]
    @fence = k || 0
    @cursor = [cursor, @fence].max
  end
  attr_reader :dict

  def update_dict
    each_document do |cur, prev|
      @dict.delete(*prev) if prev
      @dict.push(*cur)
    end
  end

  def each_document
    while true
      ary = @drip.read_tag(@cursor, 'rbcrowl', 10, 1)
      ary.each do |k, v|
        prev = prev_version(k, v[0])
        yield(v, prev)
        @cursor = k
      end
    end
  end

  def prev_version(cursor, fname)
    k, v = @drip.older(cursor, 'rbcrowl-fname=' + fname)
    (v && k > @fence) ? v : nil
  end
end

class Dict
  include MonitorMixin
  def initialize
    super()
    @tree = RBTree.new
  end

  def query(word)
    synchronize do
      @tree.bound([word, 0, ''], [word + "\0", 0, '']).collect {|k, v| k[2]}
    end
  end

  def delete(fname, mtime, src)
    synchronize do
      each_tree_key(fname, mtime, src) do |key|
        @tree.delete(key)
      end
    end
  end

  def push(fname, mtime, src)
    synchronize do
      each_tree_key(fname, mtime, src) do |key|
        @tree[key] = true
      end
    end
  end

  def intern(word)
    k, v = @tree.lower_bound([word, 0, ''])
    return k[0] if k && k[0] == word
    word
  end

  def each_tree_key(fname, mtime, src)
    NKF.nkf('-w', src).scan(/\w+/m).uniq.each do |word|
      yield([intern(word), mtime.to_i, fname])
    end
  end
end

indexer ||= Indexer.new(0)
Thread.new do
  indexer.update_dict
end

while line = gets
  ary = indexer.dict.query(line.chomp)
  pp ary
  pp ary.size
end



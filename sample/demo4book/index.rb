require 'pp'
require 'nkf'
require 'rbtree'
require 'my_drip'

class Indexer
  def initialize(cursor=0)
    @drip = MyDrip
    @cursor = cursor
  end

  def prev_version(cursor, fname)
    _, v = @drip.older(cursor, 'rbcrowl-fname=' + fname)
    v
  end

  def each_document
    n = 0
    while true
      ary = @drip.read_tag(@cursor, 'rbcrowl', 10, 0)
      break if ary.empty?
      ary.each do |k, v|
        prev = prev_version(k, v[0])
        yield(v, prev)
        @cursor = k
        n += 1
        p n if n % 100 == 0
      end
    end
  end
  
  def make_dict
    dict = Dict.new
    indexer = Indexer.new
    indexer.each_document do |cur, prev|
      dict.delete(*prev) if prev
      dict.push(*cur)
    end
    dict
  end
end

class Dict
  def initialize
    @tree = RBTree.new
  end

  def query(word)
    @tree.bound([word, 0, ''], [word + "\0", 0, '']).collect {|k, v| k[2]}
  end

  def delete(fname, mtime, src)
    each_tree_key(fname, mtime, src) do |key|
      @tree.delete(key)
    end
  end

  def push(fname, mtime, src)
    each_tree_key(fname, mtime, src) do |key|
      @tree[key] = true
    end
  end

  def intern(word)
    k, v = @tree.lower_bound([word, 0, ''])
    return k[0] if k && k[0] == word
    word
  end

  def each_tree_key(fname, mtime, src)
    NKF.nkf('-w', src).scan(/\w+/m) do |word|
      yield([intern(word), mtime.to_i, fname])
    end
  end
end

indexer = Indexer.new
dict = indexer.make_dict

File.open('index.dump', 'wb') do |fp|
  Marshal.dump(dict, fp)
end

pp dict

p :indexed
while line = gets
  pp dict.query(line.chomp)
end



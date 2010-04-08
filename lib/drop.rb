require 'rbtree'
require 'drb/drb'
require 'rinda/tuplespace'
require 'enumerator'

class Drop
  include DRbUndumped

  class SimpleStore
    def self.reader(name)
      self.to_enum(:each, name)
    end

    def self.each(name)
      file = File.open(name, 'rb')
      while true
        key, value = Marshal.load(file)
        yield(key, value)
      end
    rescue EOFError
    ensure
      file.close if file
    end

    def initialize(name)
      @file = File.open(name, 'a+b')
    end
    
    def write(key, value)
      Marshal.dump([key, value], @file)
      @file.flush
    end
  end

  def initialize(dir)
    @dir = dir
    @pool = RBTree.new
    @prop = RBTree.new
    @event = Rinda::TupleSpace.new
    @event.write([:last, 0])
    make_key {|nop|}
    prepare_store(dir)
    @store = SimpleStore.new(File.join(dir, "#{(last_key + 1).to_s(36)}.log"))
  end

  def first
    @pool.first
  end

  def last_key
    @event.read([:last, nil])[1]
  end
  
  def write(value)
    make_key do |key|
      do_write(key, value)
      @store.write(key, value)
    end
  end

  def read(key)
    @pool[key]
  end
  
  def read_prop(key, prop)
    @prop[prop, key]
  end
  
  def read_after(key, n, at_least=1)
    ary = []
    n.times do
      wait(key) if at_least > ary.size
      it = @pool.lower_bound(succ(key))
      return ary unless it
      ary << it
      key = it[0]
    end
    ary
  end

  def read_prop_after(key, prop, n, at_least=1)
    ary = []
    n.times do
      wait_prop(key, prop) if at_least > ary.size
      it = @prop.lower_bound([prop, succ(key)])
      return ary unless it && it[0][0] == prop
      ary << it
      key = it[0][1]
    end
    ary
  end
  
  private
  def prepare_store(dir)
    Dir.mkdir(dir) rescue nil
    Dir.glob(File.join(dir, '*.log')) do |fn|
      begin
        store = SimpleStore.reader(fn)
        restore(store)
      rescue
      end
    end
  end

  def do_write(key, value)
    value.each do |k, v|
      next unless String === k
      @prop[[k, key]] = v
    end
    @pool[key] = value
  end

  def restore(store)
    _, last = @event.take([:last, nil])
    store.each do |k, v|
      do_write(k, v)
    end
    last ,= @pool.last
  ensure
    @event.write([:last, last || 0])
  end

  def make_key
    _, last = @event.take([:last, nil])
    begin
      now = Time.now
      key = now.tv_sec * 1000000 + now.tv_usec
    end while last == key
    yield(key)
    key
  ensure
    @event.write([:last, key])
  end
  
  class LessThan
    def initialize(key)
      @key = key
    end
    
    def ===(other)
      (@key <=> other) < 0
    end
  end

  def wait(key)
    @event.read([:last, LessThan.new(key)])[1]
  end

  def wait_prop(key, prop)
    wait(key)
    okey = succ(key)
    begin
      found = @prop.lower_bound([prop, okey])
      return found if found && found[0][0] == prop
    end while key = wait(key)
  end
  
  def succ(key)
    key + 1
  end
end

if __FILE__ == $0
  drop = Drop.new('my_log')
  DRb.start_service('druby://localhost:54545', drop)
  DRb.thread.join
end

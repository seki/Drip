require 'rbtree'
require 'drb/drb'
require 'rinda/tuplespace'
require 'enumerator'

class Drop
  include DRbUndumped

  def inspect; to_s; end

  def initialize(dir)
    @pool = RBTree.new
    @prop = RBTree.new
    @event = Rinda::TupleSpace.new
    @event.write([:last, 0])
    make_key {|nop|}
    prepare_store(dir)
  end

  def write(value)
    make_key do |key|
      do_write(key, value)
      @pool[key] = @store.write(key, value)
    end
  end

  def fetch(key)
    @pool[key].to_hash
  end
  alias [] fetch
  
  def prop(key, prop)
    @pool[key][prop]
  end
  
  def read_after(key, n=1, at_least=1)
    ary = []
    n.times do
      wait(key) if at_least > ary.size
      it = @pool.lower_bound(key + 1)
      return ary unless it
      ary << [it[0], it[1].to_hash]
      key = it[0]
    end
    ary
  end

  def read_prop_after(key, prop, n=1, at_least=1)
    ary = []
    n.times do
      wait_prop(key, prop) if at_least > ary.size
      it ,= @prop.lower_bound([prop, key + 1])
      return ary unless it && it[0] == prop
      ary << it
      key = it[1]
    end
    ary
  end

  def read_before(key, prop=nil)
    key = time_to_key(Time.now) unless key
    if prop
      it ,= @prop.upper_bound([prop, key - 1])
      return nil unless it && it[0] == prop
      [it[1], fetch(it[1])]
    else
      k, v = @pool.upper_bound(key)
      k ? [k, v.to_hash] : nil
    end
  end

  def time_to_key(time)
    time.tv_sec * 1000000 + time.tv_usec
  end

  def key_to_time(key)
    Time.at(*key.divmod(1000000))
  end
  
  def _forget(key=nil)
    key = time_to_key(Time.now) unless key    
    @pool.each do |k, v|
      return if k > key
      v.forget
    end
    nil
  end

  private
  class Attic
    def initialize(fname, fpos, key, value)
      @fname = fname
      @fpos = fpos
      @key = key
      @value = value
    end
    
    def to_hash
      retrieve unless @value
      @value
    end
    
    def [](prop)
      to_hash[prop]
    end
    
    def forget
      @value = nil
    end
    
    def retrieve
      File.open(@fname) do |fp|
        fp.seek(@fpos)
        @key, @value = Marshal.load(fp)
      end
    end
  end

  class SimpleStore
    def self.reader(name)
      self.to_enum(:each, name)
    end

    def self.each(name)
      file = File.open(name, 'rb')
      while true
        pos = file.pos
        key, value = Marshal.load(file)
        yield(name, pos, key, value)
      end
    rescue EOFError
    ensure
      file.close if file
    end

    def initialize(name)
      if name
        @file = File.open(name, 'a+b')
      else
        @file = nil
      end
    end
    
    def write(key, value)
      return unless @file
      name = @file.path
      pos = @file.pos
      Marshal.dump([key, value], @file)
      @file.flush
      Attic.new(name, pos, key, value)
    end
  end

  def prepare_store(dir)
    if dir.nil?
      @store = SimpleStore.new(nil)
      return
    end

    Dir.mkdir(dir) rescue nil
    Dir.glob(File.join(dir, '*.log')) do |fn|
      begin
        store = SimpleStore.reader(fn)
        restore(store)
      rescue
      end
    end
    name = time_to_key(Time.now).to_s(36) + '.log'
    @store = SimpleStore.new(File.join(dir, name))
  end

  def do_write(key, value)
    value.each do |k, v|
      next unless String === k
      @prop[[k, key]] = key
    end
    @pool[key] = value
  end

  def restore(store)
    _, last = @event.take([:last, nil])
    store.each do |name, pos, k, v|
      do_write(k, v)
      @pool[k] = Attic.new(name, pos, k, v)
      @pool[k].forget
    end
    last ,= @pool.last
  ensure
    @event.write([:last, last || 0])
  end

  def make_key
    _, last = @event.take([:last, nil])
    begin
      key = time_to_key(Time.now)
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
    okey = key + 1
    begin
      it ,= @prop.lower_bound([prop, okey])
      return if it && it[0] == prop
    end while key = wait(key)
  end
end

if __FILE__ == $0
  drop = Drop.new('my_log')
  DRb.start_service('druby://localhost:54545', drop)
  DRb.thread.join
end

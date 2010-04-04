require 'rbtree'
require 'drb/drb'
require 'rinda/tuplespace'

class Drop
  class Unknown
    def initialize(err, buf)
      case err.to_s
      when /uninitialized constant (\S+)/
        @name = $1
      when /undefined class\/module (\S+)/
        @name = $1
      else
        @name = nil
      end
      @buf = buf
    end
    
    def [](key)
      [self.class.to_s, @name, @buf]
    end
  end

  class SimpleStore
    def initialize(name)
      @file = File.open(name, 'a+b')
    end
    
    MAX_LONG = 2 ** 32
    def write(key, value)
      buf = Marshal.dump(value)
      header = (key.divmod(MAX_LONG) + [buf.size]).pack('NNN')
      @file.write(header)
      @file.write(buf)
      @file.flush
    end

    def each
      @file.rewind
      while true
        buf = @file.read(12) || break
        header = buf.unpack('NNN')
        key = header[0] * MAX_LONG + header[1]
        buf = @file.read(header[2])
        begin
          value = Marshal.load(buf)
        rescue NameError, ArgumentError
          value = Unknown.new($!, s)
        end
        yield(key, value)
      end
    rescue EOFError
    end
  end

  def initialize(store)
    @store = String === store ? SimpleStore.new(store) : store
    @pool = RBTree.new
    @prop = RBTree.new
    @event = Rinda::TupleSpace.new
    @event.write([:last, 0])
    make_key {|nop|}
    restore
  end

  def first
    @pool.first
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
  
  def read_after(key, n, blocking=true)
    wait(key) if blocking
    ary = []
    n.times do
      it = @pool.lower_bound(succ(key))
      return ary unless it
      ary << it
      key = it[0]
    end
    ary
  end

  def read_prop_after(key, prop, n, blocking=true)
    wait_prop(key, prop) if blocking
    ary = []
    n.times do
      it = @prop.lower_bound([prop, succ(key)])
      return ary unless it && it[0][0] == prop
      ary << it
      key = it[0][1]
    end
    ary
  end
  
  private
  def do_write(key, value)
    value.each do |k, v|
      @prop[[k, key]] = v
    end
    @pool[key] = value
  end

  def restore
    _, last = @event.take([:last, nil])
    @store.each do |k, v|
      do_write(k, v)
      last = k if last < k
    end
  ensure
    @event.write([:last, last])
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
  drop = Drop.new('my_log.db')
#   DRb.start_service('druby://localhost:54545', drop)
#   DRb.thread.join
  key = 0
  while it = drop.read_after(key, 1, false)
    break unless it[0]
    p it[0]
    key = it[0][0]
  end

  while line = gets
    p drop.write('line' => line)
  end
end

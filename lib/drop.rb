require 'rbtree'
require 'drb/drb'
require 'rinda/tuplespace'
require 'enumerator'

class DropCore
  def inspect; to_s; end

  def initialize(dir)
    @pool = RBTree.new
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
  
  def read(key, n=1, at_least=1)
    key = time_to_key(Time.now) unless key
    ary = []
    n.times do
      wait(key) if at_least > ary.size
      key, value = @pool.lower_bound(key + 1)
      return ary unless key
      ary << [key, value.to_hash]
    end
    ary
  end

  def older(key)
    key = time_to_key(Time.now) unless key
    k, v = @pool.upper_bound(key - 1)
    k ? [k, v.to_hash] : nil
  end

  def newer(key)
    read(key, 1, 0)[0]
  end

  def time_to_key(time)
    time.tv_sec * 1000000 + time.tv_usec
  end

  def key_to_time(key)
    Time.at(*key.divmod(1000000))
  end
  
  def _forget(key=nil)
    return unless @store.forgettable?
    key = time_to_key(Time.now) unless key    
    @pool.each do |k, v|
      return if k > key
      v.forget
    end
    nil
  end

  private
  class SimpleStore
    Attic = Struct.new(:fname, :fpos, :value)
    class Attic
      def to_hash
        retrieve unless value
        value
      end
      
      def forget
        self.value = nil
      end
      
      def retrieve
        File.open(fname) do |fp|
          fp.seek(fpos)
          kv = Marshal.load(fp)
          self.value = kv[1]
        end
      end
    end

    def self.reader(name)
      self.to_enum(:each, name)
    end

    def self.each(name)
      file = File.open(name, 'rb')
      while true
        pos = file.pos
        key, value = Marshal.load(file)
        yield(key, value, Attic.new(name, pos, value))
      end
    rescue EOFError
    ensure
      file.close if file
    end

    def initialize(name)
      @name = name
      @file = nil
    end

    def forgettable?
      @name ? true : false
    end
    
    def write(key, value)
      return value unless @name
      @file = File.open(@name, 'a+b') unless @file
      pos = @file.pos
      Marshal.dump([key, value], @file)
      @file.flush
      Attic.new(@name, pos, value)
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
    @pool[key] = value
  end

  def restore(store)
    _, last = @event.take([:last, nil])
    store.each do |k, v, attic|
      do_write(k, v)
      @pool[k] = attic
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
end

class Drop < DropCore
  include DRbUndumped

  def initialize(dir)
    @tag = RBTree.new
    super(dir)
  end

  def read_tag(key, tag, n=1, at_least=1)
    key = time_to_key(Time.now) unless key
    ary = []
    n.times do
      wait_tag(key, tag) if at_least > ary.size
      it ,= @tag.lower_bound([tag, key + 1])
      return ary unless it && it[0] == tag
      key = it[1]
      ary << [key, fetch(key)]
    end
    ary
  end

  def older(key, tag=nil)
    key = time_to_key(Time.now) unless key
    return super(key) unless tag

    it ,= @tag.upper_bound([tag, key - 1])
    return nil unless it && it[0] == tag
    [it[1], fetch(it[1])]
  end

  def newer(key, tag=nil)
    return super(key) unless tag
    read_tag(key, tag, 1, 0)[0]
  end
  def next_tag(cur=nil, n=1)
    return _next_tag(cur) if n == 1
    ary = []
    while cur = _next_tag(cur)
      ary << cur
      n -= 1
      break if n <= 0
    end
    ary
  end

  def tags(prefix='')
    ary = []
    cur = next_tag(prefix)
    while cur && cur.index(prefix) == 0
      str = cur.dup
      str[prefix] = ''
      ary << str
      cur = next_tag(cur + "\0")
    end
    ary
  end

  private
  def do_write(key, value)
    value.each do |k, v|
      next unless String === k
      @tag[[k, key]] = key
    end
    super(key, value)
  end

  def wait_tag(key, tag)
    wait(key)
    okey = key + 1
    begin
      it ,= @tag.lower_bound([tag, okey])
      return if it && it[0] == tag
    end while key = wait(key)
  end

  def _next_tag(cur)
    fwd = cur ? cur + "\0" : ''
    it ,= @tag.lower_bound([fwd, 0])
    return nil unless it
    it[0]
  end
end

if __FILE__ == $0
  drop = Drop.new('my_log')
  DRb.start_service('druby://localhost:54545', drop)
  DRb.thread.join
end

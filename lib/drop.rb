require 'rbtree'
require 'drb/drb'
require 'rinda/tuplespace'
require 'enumerator'

class Drop
  include DRbUndumped

  def inspect; to_s; end

  def initialize(dir)
    @pool = RBTree.new
    @tag = RBTree.new
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
      it = @pool.lower_bound(key + 1)
      return ary unless it
      ary << [it[0], it[1].to_hash]
      key = it[0]
    end
    ary
  end

  def read_tag(key, tag, n=1, at_least=1)
    key = time_to_key(Time.now) unless key
    ary = []
    n.times do
      wait_tag(key, tag) if at_least > ary.size
      it ,= @tag.lower_bound([tag, key + 1])
      return ary unless it && it[0] == tag
      ary << it
      key = it[1]
    end
    ary
  end

  def older(key, tag=nil)
    key = time_to_key(Time.now) unless key
    if tag
      it ,= @tag.upper_bound([tag, key - 1])
      return nil unless it && it[0] == tag
      [it[1], fetch(it[1])]
    else
      k, v = @pool.upper_bound(key - 1)
      k ? [k, v.to_hash] : nil
    end
  end

  def newer(key, tag=nil)
    if tag
      read_tag(key, tag, 1, 0)[0]
    else
      read(key, 1, 0)[0]
    end
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
    
    def [](tag)
      to_hash[tag]
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
      @name = name
      @file = nil
    end
    
    def write(key, value)
      return unless @name
      @file = File.open(@name, 'a+b') unless @file
      pos = @file.pos
      Marshal.dump([key, value], @file)
      @file.flush
      Attic.new(@name, pos, key, value)
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
      @tag[[k, key]] = key
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

require 'rbtree'
require 'drb/drb'
require 'rinda/tuplespace'
require 'enumerator'

class Drip
  include DRbUndumped
  def inspect; to_s; end

  def initialize(dir, option={})
    @pool = RBTree.new
    @tag = RBTree.new
    @event = Rinda::TupleSpace.new(5)
    @event.write([:last, 0])
    prepare_store(dir, option)
  end

  def write(obj, *tags)
    write_after(Time.now, obj, *tags)
  end

  def write_after(at, *value)
    make_key(at) do |key|
      value = do_write(key, value)
      @pool[key] = @store.write(key, value)
    end
  end
  
  def write_at(at, *value)
    make_key_at(at) do |key|
      value = do_write(key, value)
      @pool[key] = @store.write(key, value)
    end
  end

  def fetch(key)
    @pool[key].to_a
  end
  alias [] fetch

  def make_renewer(timeout)
    case timeout
    when 0
      return 0
    when Numeric
      return Renewer.new(timeout)
    else
      nil
    end
  end
  
  def read(key, n=1, at_least=1, timeout=nil)
    renewer = make_renewer(timeout)
    key = time_to_key(Time.now) unless key
    ary = []
    n.times do
      begin
        wait(key, renewer) if at_least > ary.size
      rescue Rinda::RequestExpiredError
        return ary
      end
      key, value = @pool.lower_bound(key + 1)
      return ary unless key
      ary << [key] + value.to_a
    end
    ary
  end

  def read_tag(key, tag, n=1, at_least=1, timeout=nil)
    renewer = make_renewer(timeout)
    key = time_to_key(Time.now) unless key
    ary = []
    n.times do
      begin
        wait_tag(key, tag, renewer) if at_least > ary.size
      rescue Rinda::RequestExpiredError
        return ary
      end
      it ,= @tag.lower_bound([tag, key + 1])
      return ary unless it && it[0] == tag
      key = it[1]
      ary << [key] + fetch(key)
    end
    ary
  end

  def head(n=1, tag=nil)
    ary = []
    key = nil
    while it = older(key, tag)
      break if n <= 0
      ary.unshift(it)
      key = it[0]
      n -= 1
    end
    ary
  end

  def older(key, tag=nil)
    key = time_to_key(Time.now) unless key
    unless tag
      k, v = @pool.upper_bound(key - 1)
      return k ? [k] + v.to_a : nil
    end

    it ,= @tag.upper_bound([tag, key - 1])
    return nil unless it && it[0] == tag
    [it[1]] + fetch(it[1])
  end

  def newer(key, tag=nil)
    return read(key, 1, 0)[0] unless tag
    read_tag(key, tag, 1, 0)[0]
  end

  def time_to_key(time)
    time.tv_sec * 1000000 + time.tv_usec
  end

  def key_to_time(key)
    Time.at(*key.divmod(1000000))
  end
  
  private
  class SimpleStore
    Attic = Struct.new(:fname, :fpos, :value)
    class Attic
      def to_a
        value || retrieve
      end
      
      def forget
        self.value = nil
      end
      
      def retrieve
        File.open(fname) do |fp|
          fp.seek(fpos)
          kv = Marshal.load(fp)
          kv[1]
        end
      end
    end

    class AtticCache
      def initialize(n)
        @size = n
        @tail = 0
        @ary = Array.new(n)
      end
      
      def push(attic)
        @ary[@tail].forget if @ary[@tail]
        @ary[@tail] = attic
        @tail = (@tail + 1) % @size
        attic
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

    def initialize(name, option={})
      @name = name
      @file = nil
      cache_size = option.fetch(:cache_size, 8)
      @cache = AtticCache.new(cache_size) if @name
    end

    def write(key, value)
      return value unless @name
      @file = File.open(@name, 'a+b') unless @file
      pos = @file.pos
      Marshal.dump([key, value], @file)
      @file.flush
      @cache.push(Attic.new(@name, pos, value))
    end
  end

  def prepare_store(dir, option={})
    if dir.nil?
      @store = SimpleStore.new(nil, option)
      return
    end

    Dir.mkdir(dir) rescue nil
    dump = Dir.glob(File.join(dir, '*.dump')).max_by do |fn| 
      File.basename(fn).to_i(36)
    end
    if dump
      @pool, @tag, last = File.open(dump, 'rb') {|fp| Marshal.load(fp)}
      @event.take([:last, nil])
      @event.write([:last, last])
      File.unlink(dump)
    end
    loaded = dump ? File.basename(dump).to_i(36) : 0
    Dir.glob(File.join(dir, '*.log')) do |fn|
      next if loaded > File.basename(fn).to_i(36)
      begin
        store = SimpleStore.reader(fn)
        restore(store)
      rescue
      end
    end
    name = time_to_key(Time.now).to_s(36)
    _, last = @event.read([:last, nil])
    File.open(File.join(dir, name + '.dump'), 'wb') {|fp|
      Marshal.dump([@pool, @tag, last], fp)
    }
    @store = SimpleStore.new(File.join(dir, name + '.log'))
  end

  def shared_text(str)
    key, value = @tag.lower_bound([str, 0])
    if key && key[0] == str
      key[0]
    else
      str
    end
  end

  def do_write(key, value)
    obj, *tags = value
    tags.uniq!
    tags.each do |k|
      next unless String === k
      tag = shared_text(k)
      @tag[[tag, key]] = key
    end
    @pool[key] = [obj] + tags
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
  
  def make_key(at=Time.now)
    synchronize do |last|
      key = [time_to_key(at), last + 1].max
      yield(key)
      key
    end
  end
  
  def make_key_at(at)
    synchronize do |last|
      key = time_to_key(at)
      raise 'InvalidTimeError' if key <= last
      yield(key)
      key
    end
  end
  
  def synchronize
    _, last = @event.take([:last, nil])
    last = yield(last)
  ensure
    @event.write([:last, last])
  end
  
  INF = 1.0/0.0
  def wait(key, renewer)
    @event.read([:last, key+1 .. INF], renewer)[1]
  end

  def wait_tag(key, tag, renewer)
    wait(key, renewer)
    okey = key + 1
    begin
      it ,= @tag.lower_bound([tag, okey])
      return if it && it[0] == tag
    end while key = wait(key, renewer)
  end

  class Renewer
    def initialize(timeout)
      @at = Time.now + timeout
    end
    
    def renew
      @at - Time.now
    end
  end
end

if __FILE__ == $0
  require 'my_drip'
  MyDrip.invoke
end

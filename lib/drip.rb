require 'rbtree'
require 'drb/drb'
require 'rinda/tuplespace'
require 'enumerator'

class Drip
  include DRbUndumped
  INF = 1.0/0.0

  def inspect; to_s; end
  def initialize(dir, option={})
    @past = prepare_store(dir, option)
    @fence = (@past.head[0][0] rescue 0) || 0
    @pool = Drip::SortedArray.new([])
    @tag = RBTree.new
    @event = Rinda::TupleSpace.new(5)
    @event.write([:last, @fence])
  end

  def write(obj, *tags)
    write_after(Time.now, obj, *tags)
  end

  def write_after(at, *value)
    make_key(at) do |key|
      value = do_write(key, value)
      @pool.push([key, @store.write(key, value)])
    end
  end
  
  def write_at(at, *value)
    make_key_at(at) do |key|
      value = do_write(key, value)
      @pool.push([key, @store.write(key, value)])
    end
  end

  def write_if_latest(cond, *value)
    make_key(Time.now) do |key|
      cond.each {|it|
        return nil unless latest?(it[1], it[0])
      }
      value = do_write(key, value)
      @pool.push([key, @store.write(key, value)])
    end
  end

  def fetch(key)
    return @past.fetch(key) if @fence >= key 
    @pool.fetch(key)
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
    return curr_read(key, n, at_least, timeout) if key > @fence
    ary = @past.read(key, n)
    return ary if ary.size >= n
    ary + curr_read(key, n - ary.size, at_least - ary.size, timeout)
  end

  def read_tag(key, tag, n=1, at_least=1, timeout=nil)
    return curr_read_tag(key, tag, n, at_least, timeout) if key > @fence
    ary = @past.read_tag(key, tag, n)
    return ary if ary.size >= n
    ary + curr_read_tag(key, tag, n - ary.size, at_least - ary.size, timeout)
  end

  def head(n=1, tag=nil)
    unless tag
      ary = @pool.head(n)
      return @past.head(n - ary.size) + ary
    end
    ary = curr_head(n, tag)
    return ary if ary.size == n
    @past.head(n - ary.size, tag) + ary
  end

  def latest?(key, tag=nil)
    now = time_to_key(Time.now)
    if tag
      it ,= @tag.upper_bound([tag, now])
      if it && it[0] == tag
        return true if it[1] == key
        return false if it[1] > key
      end
    else
      return true if @pool.latest?(key)
    end
    @past.latest?(key, tag)
  end

  def older(key, tag=nil)
    curr_older(key, tag) || @past.older(key, tag)
  end
  
  def newer(key, tag=nil)
    @past.newer(key, tag) || curr_newer(key, tag)
  end

  def curr_read(key, n=1, at_least=1, timeout=nil)
    renewer = make_renewer(timeout)
    key = time_to_key(Time.now) unless key
    ary = []
    n.times do
      begin
        wait(key, renewer) if at_least > ary.size
      rescue Rinda::RequestExpiredError
        return ary
      end
      key, value, *tags = @pool.read(key)[0]
      return ary unless key
      ary << [key] + [value, *tags]
    end
    ary
  end

  def curr_read_tag(key, tag, n=1, at_least=1, timeout=nil)
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

  def curr_head(n=1, tag=nil)
    ary = []
    key = nil
    while it = curr_older(key, tag)
      break if n <= 0
      ary.unshift(it)
      key = it[0]
      n -= 1
    end
    ary
  end

  def curr_older(key, tag=nil)
    key = time_to_key(Time.now) unless key

    return @pool.older(key) unless tag

    it ,= @tag.upper_bound([tag, key - 1])
    return nil unless it && it[0] == tag
    [it[1]] + fetch(it[1])
  end

  def curr_newer(key, tag=nil)
    return read(key, 1, 0)[0] unless tag
    read_tag(key, tag, 1, 0)[0]
  end

  def self.time_to_key(time)
    time.tv_sec * 1000000 + time.tv_usec
  end

  def time_to_key(time)
    self.class.time_to_key(time)
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
      return ImmutableDrip.new
    end

    Dir.mkdir(dir) rescue nil
    dump = Dir.glob(File.join(dir, '*.dump')).max_by do |fn| 
      File.basename(fn).to_i(36)
    end
    if dump
      pool, tag, _ = File.open(dump, 'rb') {|fp| Marshal.load(fp)}
      File.unlink(dump)
    else
      pool = []
      tag = []
    end
    gen = ImmutableDrip::Generator.new(pool, tag)
    loaded = dump ? File.basename(dump).to_i(36) : 0
    Dir.glob(File.join(dir, '*.log')) do |fn|
      next if loaded > File.basename(fn).to_i(36)
      begin
        SimpleStore.reader(fn).each do |k, v, attic|
          obj, *tags = v
          attic.forget
          gen.add(k, attic, *tags)
        end
      rescue
      end
    end
    name = Drip.time_to_key(Time.now).to_s(36)
    File.open(File.join(dir, name + '.dump'), 'wb') {|fp|
      Marshal.dump([gen.pool, gen.tag], fp)
    }
    @store = SimpleStore.new(File.join(dir, name + '.log'))
    return gen.generate
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
    # @pool[key] = [obj] + tags
    [obj] + tags
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

class Drip
  module ArrayBsearch
    module_function
    def lower_boundary(ary, key)
      lower = -1
      upper = ary.size
      while lower + 1 != upper
        mid = (lower + upper).div(2)
        if (ary[mid][0] <=> key) < 0
          lower = mid
        else
          upper = mid
        end
      end
      return upper
    end
    
    def upper_boundary(ary, key)
      lower = -1
      upper = ary.size
      while lower + 1 != upper
        mid = (lower + upper).div(2)
        if (ary[mid][0] <=> key) <= 0
          lower = mid
        else
          upper = mid
        end
      end
      return lower + 1
    end
  end
end

class Drip
  class FakeRBTree
    include Drip::ArrayBsearch

    def initialize
      @tree = Hash.new {|h, k| h[k] = Array.new}
    end
    
    def upper_bound(pair)
      tag, now = pair
      return nil unless @tree.include?(tag)
      ary = @tree[tag]
      idx = upper_boundary(ary, now)
      [tag, ary[tag][idx]]
    end
    
    def lower_bound(pair)
    end
  end
end



class Drip
  class SortedArray
    include Drip::ArrayBsearch

    def initialize(ary)
      @ary = ary
      @last_key = ary.empty? ? 0 : ary[-1][0]
    end

    def push(obj)
      raise 'InvalidTimeError' if obj[0] <= @last_key
      @last_key = obj[0]
      @ary << obj
    end

    def fetch(key)
      idx = lower_boundary(@ary, key)
      k, v = @ary[idx]
      k == key ? v.to_a : nil
    end

    def read(key, n=1)
      idx = lower_boundary(@ary, key + 1)
      return [] unless idx
      @ary[idx, n].collect {|kv|
        [kv[0], *kv[1].to_a]
      }
    end

    def empty?
      @ary.empty?
    end

    def latest?(key)
      @last_key == key
    end

    def head(n)
      n = @ary.size < n ? @ary.size : n
      @ary[-n, n].collect {|kv|
        [kv[0], *kv[1].to_a]
      }
    end

    def older(key)
      return nil if @ary.empty?
      key = @ary[-1][0] + 1 unless key
      idx = upper_boundary(@ary, key - 1)
      k, v = @ary[idx - 1]
      k && k < key ? [k, *v.to_a] : nil
    end

    def last
      @ary[-1]
    end
    
    def last_key
      @ary.empty? ? 0 : last[0]
    end
  end
end

class Drip
  class ImmutableDrip
    include Drip::ArrayBsearch

    def initialize(pool=[], tag=[])
      @pool = Drip::SortedArray.new(pool)
      @tag = tag
    end

    def fetch(key)
      @pool.fetch(key)
    end

    def read(key, n=1)
      @pool.read(key, n)
    end

    def read_tag(key, tag, n=1)
      idx = lower_boundary(@tag, [tag, key + 1])
      return [] unless idx
      @tag[idx, n].find_all {|kv| kv[0][0] == tag}.collect {|kv| 
        [kv[0][1], *fetch(kv[0][1])]
      }
    end

    def latest?(key, tag)
      return (key == 0) if @pool.empty?
      return @pool.latest?(key) unless tag

      lower = lower_boundary(@tag, [tag, key])
      upper = upper_boundary(@tag, [tag, INF])
      
      if key == 0
        return lower == upper
      else
        return lower == upper - 1
      end
    end

    def head_tag(n, tag)
      lower = lower_boundary(@tag, [tag, 0])
      upper = upper_boundary(@tag, [tag, INF])
      lower = [lower, upper - n].max
      @tag[lower ... upper].collect {|kv|
        [kv[0][1], *fetch(kv[0][1])]
      }
    end

    def head(n=1, tag=nil)
      tag ? head_tag(n, tag) : @pool.head(n)
    end

    def older_tag(key, tag)
      idx = upper_boundary(@tag, [tag, key-1])
      k, v = @tag[idx - 1]
      k && k[0] == tag ? [k[1], *fetch(k[1])] : nil
    end

    def older(key, tag=nil)
      return nil if @pool.empty?
      key = @pool.last_key + 1 unless key
      return older_tag(key, tag) if tag
      @pool.older(key)
    end

    def newer(key, tag=nil)
      return read(key, 1)[0] unless tag
      read_tag(key, tag, 1)[0]
    end
  end
end

class Drip
  class ImmutableDrip
    class Generator
      def initialize(pool=[], tag=[])
        @pool = pool
        @tag = tag
        @shared = Hash.new {|h, k| h[k] = k; k}
        @tag.each {|pair| @shared[pair[0]]}
      end
      attr_reader :pool, :tag

      def add(key, value, *tag)
        @pool << [key, value]
        idx = @pool.size - 1
        tag.uniq.each do |t|
          @tag << [[@shared[t], key]]
        end
      end
      
      def generate
        tag = @tag.sort
        tag.inject(nil) do |last, kv|
          k = kv[0]
          k[0] = last if k[0] == last
          k[0]
        end
        ImmutableDrip.new(@pool.sort, tag)
      end
    end
  end
end

if __FILE__ == $0
  require 'my_drip'
  MyDrip.invoke
end

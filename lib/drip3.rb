require 'drb/drb'
require 'rinda/tuplespace'
require 'enumerator'
require 'sqlite3'

class SQLite3::Database
  def statement(s)
    if @cache.nil?
      @cache = Hash.new { |h, k| h[k] = self.prepare(k) }
    end
    @cache[s]
  end
end

class SQLite3::Statement
  def get_first_row(*bind_vars)
    execute(*bind_vars).first
  end

  def get_first_value(*bind_vars)
    execute(*bind_vars).each { |row| return row[0] }
    nil
  end
end

class Drip3
  include DRbUndumped
  INF = 1.0/0.0

  def inspect; to_s; end
  
  def initialize(dir)
    @event = Rinda::TupleSpace.new(5)
    @event.write([:last, 1])
    setup_db(dir)
  end

  def self.time_to_key(time)
    time.tv_sec * 1000000 + time.tv_usec
  end

  def time_to_key(time)
    self.class.time_to_key(time)
  end

  def self.key_to_time(key)
    Time.at(*key.divmod(1000000))
  end

  def key_to_time(key)
    self.class.key_to_time(key)
  end

  def write(obj, tag=nil)
    write_after(Time.now, obj, tag)
  end

  def write_after(at, obj, tag=nil)
    make_key(at) do |key|
      do_write(key, obj, tag)
    end
  end

  def write_at(at, obj, tag=nil)
    make_key_at(at) do |key|
      do_write(key, obj, tag)
    end
  end

  def write_if_latest(cond, obj, tag=nil)
    make_key(Time.now) do |key|
      do_write(key, obj, tag, cond)
    end
  end

  def fetch(key)
    do_fetch(key)
  end
  alias [] fetch

  def head(n=1, tag=nil)
    if tag
      do_head_tag(n, tag)
    else
      do_head(n)
    end
  end

  def read(key, n=1, at_least=1, timeout=nil)
    key = time_to_key(Time.now) unless key
    ary = do_read(key, n)
    while ary.size < at_least
      key = ary[-1][0] unless ary.empty?
      begin
        renewer = make_renewer(timeout)
        wait(key, renewer)
      rescue Rinda::RequestExpiredError
        return ary
      end
      ary += do_read(key, n - ary.size)
    end
    ary
  end

  def read_tag(key, tag, n=1, at_least=1, timeout=nil)
    at_least = n if n < at_least
    key = time_to_key(Time.now) unless key
    ary = do_read_tag(key, tag, n)
    while ary.size < at_least
      key = ary[-1][0] unless ary.empty?
      begin
        renewer = make_renewer(timeout)
        wait_tag(key, tag, renewer)
      rescue Rinda::RequestExpiredError
        return ary
      end
      ary += do_read_tag(key, tag, n - ary.size)
    end
    ary
  end

  def latest?(key, tag=nil)
    do_latest(key, tag)
  end

  def older(key, tag=nil)
    do_older(key, tag)
  end

  def newer(key, tag=nil)
    return read(key, 1, 0)[0] unless tag
    read_tag(key, tag, 1, 0)[0]
  end

  def tag_next(tag)
    do_tag_next(tag)
  end

  def tag_prev(tag)
    do_tag_prev(tag)
  end

  private
  def transaction_rw(key, &blk)
    _, db = @event.take([key, nil])
    db.transaction do
      return yield(db)
    end
  ensure
    @event.write([key, db])
  end

  def transaction(&blk)
    transaction_rw(:db, &blk)
  end

  def transaction_w(&blk)
    transaction_rw(@w_key, &blk)
  end

  def from_row(ary)
    return nil unless ary
    return ary[0], Marshal.load(ary[1]), ary[2]
  end

  def do_fetch(key)
    transaction do |db|
      db.statement('select value from Drip where key=?').execute(key).each do |row|
        return Marshal.load(row[0])
      end
    end
    nil
  end

  def do_tag_next(tag)
    return nil if tag.nil?
    transaction do |db|
      sql = <<SQL
select tag from Drip where tag>? order by key asc limit 1
SQL
      db.statement(sql).get_first_value(tag) rescue nil
    end
  end
  
  def do_tag_prev(tag)
    return nil if tag.nil?
    transaction do |db|
      sql = <<SQL
select tag from Drip where tag<? order by key desc limit 1
SQL
      db.statement(sql).get_first_value(tag) rescue nil
    end
  end
  
  def do_head_tag(n, tag)
    transaction do |db|
      sql = <<SQL
select key, value, tag from Drip where tag=? order by key desc limit ?;
SQL
      db.statement(sql).execute(tag, n).collect {|row| from_row(row)}.reverse
    end
  end

  def do_read_tag(key, tag, n=1)
    transaction do |db|
      sql = <<SQL
select key, value, tag from Drip
  where key > ? and tag=?
    order by key asc limit ?;
SQL
      db.statement(sql).execute(key, tag, n).collect {|row| from_row(row)}
    end
  end

  def wait(key, renewer)
    @event.read([:last, key+1 .. INF], renewer)[1]
  end

  def wait_tag(key, tag, renewer)
    @event.read([tag, key+1 .. INF], renewer)[1]
  end

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

  def do_head(n)
    transaction do |db|
      sql = <<SQL
select key, value, tag from Drip order by key desc limit ?
SQL
      db.statement(sql).execute(n).collect {|row| from_row(row)}.reverse
    end
  end
  
  def do_read(key, n)
    transaction do |db|
      sql = <<SQL
select key, value, tag from Drip where key > ? order by key asc limit ?
SQL
      db.statement(sql).execute(key, n).collect {|row| from_row(row)}
    end
  end

  def do_older(key, tag)
    if key.nil?
      if tag.nil?
        return do_head(1)[0]
      else
        return do_head_tag(1, tag)[0]
      end
    end
    
    transaction do |db|
      if tag
        sql = <<SQL
select key, value, tag from Drip where key < ? and tag=?
  order by key desc limit 1
SQL
        from_row(db.statement(sql).get_first_row(key, tag))
      else
        sql = <<SQL
select key, value, tag from Drip where key < ?
  order by key desc limit 1
SQL
        from_row(db.statement(sql).get_first_row(key))
      end
    end
  end

  def make_key(at=Time.now)
    synchronize do |last|
      key = [time_to_key(at), last + 1].max
      yield(key)
      # key
    end
  end
  
  def make_key_at(at)
    synchronize do |last|
      key = time_to_key(at)
      raise 'InvalidTimeError' if key <= last
      yield(key)
      # key
    end
  end
  
  def synchronize
    _, last = @event.take([:last, nil])
    last = yield(last)
  ensure
    @event.write([:last, last])
  end

  def do_latest(key, tag)
    transaction do |db|
      do_latest_inner(db, key, tag)
    end
  end
  
  def do_latest_inner(db, key, tag)
    if tag
      sql = <<SQL
select max(key) from Drip where tag=?
SQL
      v = db.statement(sql).get_first_value(tag) || 0
    else
      sql = <<SQL
select max(key) from Drip
SQL
      v = db.statement(sql).get_first_value() || 0
    end
    return v == key
  end
  
  def do_write(key, obj, tag, cond=nil)
    unless tag.nil?
      raise(ArgumentError) unless String === tag
      tag = tag.to_s
    end
    transaction_w do |db|
      if cond
        cond.each {|it|
          return nil unless do_latest_inner(db, it[1], it[0])
        }
      end
      
      if tag
        @event.take([tag, nil], 0) rescue nil
        @event.write([tag, key])
      end
      sql = 'insert into Drip values (?, ?, ?)'
      db.statement(sql).execute(key, Marshal.dump(obj), tag)
    end
    key
  end

  private
  def create_db(dir)
    if dir
      Dir.mkdir(dir) rescue nil
      # fname = 'file:' + File.join(dir, 'drip.db') + '?cache=shared'
      fname = File.join(dir, 'drip.db')
      @w_key = :db
      @event.write([:db, SQLite3::Database.open(fname)])
    else
      fname = ':memory:'
      @w_key = :db
      @event.write([:db, SQLite3::Database.open(fname)])
    end
  end

  def setup_db(dir)
    create_db(dir)
    create_table
  end
  
  def create_table
    transaction_w do |db|
      begin
        db.execute('create table Drip (key bigint, value blob, tag text);')
        db.execute('create index DripKey on Drip(key);')
        db.execute('create index Tags on Drip(key, tag);')
      rescue SQLite3::SQLException
        unless $!.message == 'table Drip already exists'
          raise $!
        end
      end
    end
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
  # d3 = Drip3.new('test_db')
  d3 = Drip3.new(nil)

  d3.write("こんにちはせかい\0\0こんにちはアゲイン")
  p d3.head[0][1]

  key = d3.write('start')
  50.times do |n|
    Thread.new(n) do |x|
      100.times do
        d3.write(x, 'count')
      end
      d3.write(x, 'wakeup')
    end
  end
  50.times do
    key, value, = d3.read_tag(key, 'wakeup', 1)[0]
    p value
  end
end
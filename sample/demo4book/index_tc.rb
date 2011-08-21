require 'pp'
require 'nkf'
require 'my_drip'
require 'tokyocabinet'
require 'monitor'

class Indexer
  def initialize(cursor=0)
    @drip = MyDrip
    @cursor = cursor
    @dict = Dict.new
  end
  attr_reader :dict

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
        p [n, @cursor] if n % 100 == 0
        # return if n == 200
      end
    end
  end
  
  def update_dict
    each_document do |cur, prev|
      @dict.delete(*prev) if prev
      @dict.push(*cur)
    end
  end
end

class Dict
  class BDBError < RuntimeError
    def initialize(bdb)
      super(bdb.errmsg(bdb.ecode))
    end
  end

  class BDB < TokyoCabinet::BDB
    def exception
      BDBError.new(self)
    end
    
    def cursor
      TokyoCabinet::BDBCUR.new(self)
    end
    
    def self.call_or_die(*ary)
      file, lineno = __FILE__, __LINE__
      if /^(.+?):(\d+)(?::in `(.*)')?/ =~ caller(1)[0]
        file = $1
        lineno = $2.to_i
      end
      ary.each do |sym|
        module_eval("def #{sym}(*arg); super || raise(self); end",
                    file, lineno)
      end
    end
    
    call_or_die :open, :close
    call_or_die :tranbegin, :tranabort, :trancommit
    call_or_die :vanish
  end

  include MonitorMixin
  def initialize
    super()
    @bdb = BDB.new
    @name = 'index.tc'
    writer{}
  end

  def transaction(mode)
    synchronize do
      begin
        @bdb.open(@name, mode)
        return yield
      ensure
        @bdb.close
      end
    end
  end

  def reader(&block)
    transaction(BDB::OREADER, &block)
  end
  
  def writer(&block)
    transaction(BDB::OWRITER | BDB::OCREAT, &block)
  end

  def query(word)
    ary = []
    reader do
      cursor = @bdb.cursor
      cursor.jump(word + "\0")
      while cursor.key
        w, mtime, fname = cursor.key.split("\0")
        break unless w == word
        ary << fname
        cursor.next
      end
    end
    ary
  end

  def delete(fname, mtime, src)
    writer do
      each_tree_key(fname, mtime, src) do |key|
        @bdb.out(key)
      end
    end
  end

  def push(fname, mtime, src)
    writer do
      each_tree_key(fname, mtime, src) do |key|
        @bdb[key] = '0'
      end
    end
  end

  def each_tree_key(fname, mtime, src)
    NKF.nkf('-w', src).scan(/\w+/m) do |word|
      yield([word, mtime.to_i.to_s, fname].join("\0"))
    end
  end
end

indexer ||= Indexer.new(0)
indexer.update_dict

p :indexed
while line = gets
  ary = indexer.dict.query(line.chomp)
  pp ary
  pp ary.size
end

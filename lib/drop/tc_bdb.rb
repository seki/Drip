require 'tokyocabinet'

class TC_BDB < TokyoCabinet::BDB
  class BDBError < RuntimeError
    def initialize(bdb)
      super(bdb.errmsg(bdb.ecode))
    end
  end
  
  def exception
    BDBError.new(self)
  end
  
  def cursor
    TokyoCabinet::BDBCUR.new(self)
  end
  
  def self.call_or_die(*ary)
    file, lineno = __FILE__, __LINE__
    if /^(.+?):(¥d+)(?::in `(.*)')?/ =~ caller(1)[0]
      file = $1
      lineno = $2.to_i
    end
    ary.each do |sym|
      module_eval("def #{sym}(*arg); super || raise(self); end", file, lineno)
    end
  end
  
  call_or_die :open, :close
  call_or_die :tranbegin, :tranabort, :trancommit
  call_or_die :vanish
end

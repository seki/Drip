require 'drb/drb'

MyDrip = DRbObject.new_with_uri('drbunix:' + File.expand_path('~/.drip/port'))

class Dripper
  def initialize(drip, bufsiz=10, at_least=10)
    @drip = drip
    @cur = nil
    @bufsiz = bufsiz
    @at_least = at_least
  end
  attr_accessor :cur

  def now
    @cur ? @drip.key_to_time(@cur) : nil
  end

  def now=(time)
    @cur = @drip.time_to_key(time)
  end

  def past_each(tag=nil)
    while kv = @drip.older(@cur, tag)
      @cur, value = kv
      yield(value)
    end
  end
end


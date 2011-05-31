require 'drb/drb'

MyDrip = DRbObject.new_with_uri('drbunix:' + File.expand_path('~/.drip/port'))

def MyDrip.invoke
  fork do
    Process.daemon
    
    require 'drip'
    require 'fileutils'
    
    dir = File.expand_path('~/.drip')
    uri = 'drbunix:' + File.join(dir, 'port')
    ro = DRbObject.new_with_uri(uri)
    begin
      ro.older(nil) #ping
      exit
    rescue
    end
    
    FileUtils.mkdir_p(dir)
    FileUtils.cd(dir)
    
    drip = Drip.new('drip')
    def drip.quit
      Thread.new do
      synchronize do |key|
          exit(0)
        end
      end
    end
    
    DRb.start_service(uri, drip)
    File.open('pid', 'w') {|fp| fp.puts($$)}
    
    DRb.thread.join
  end
end

class DripCursor
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

  def seek_at(time)
    @cur = @drip.time_to_key(time)
  end

  def past_each(tag=nil)
    while kv = @drip.older(@cur, tag)
      @cur, value = kv
      yield(value)
    end
  end
end


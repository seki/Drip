require 'pp'
require 'my_drip'
require 'monitor'

class Crowler
  include MonitorMixin

  def initialize
    super()
    @root = File.expand_path('~/develop/git-repo/')
    @drip = MyDrip
    k, = @drip.head(1, 'rbcrowl-begin')[0]
    @fence = k || 0
  end

  def last_mtime(fname)
    k, v, = @drip.head(1, 'rbcrowl-fname=' + fname)[0]
    (v && k > @fence) ? v[1] : Time.at(1)
  end

  def do_crowl
    synchronize do
      ary = []
      Dir.chdir(@root)
      Dir.glob('**/*.rb').each do |fname|
        mtime = File.mtime(fname)
        next if last_mtime(fname) >= mtime
        @drip.write([fname, mtime, File.read(fname)],
                    'rbcrowl', 'rbcrowl-fname=' + fname)
        ary << fname
      end
      @drip.write(ary, 'rbcrowl-footprint')
      ary
    end
  end
  
  def quit
    synchronize do
      exit(0)
    end
  end
end

crowler = Crowler.new
Thread.new do
  while true
    pp crowler.do_crowl
    sleep 60
  end
end

gets
crowler.quit

require 'pp'
require 'my_drip'
require 'monitor'

class Crawler
  include MonitorMixin

  def initialize
    super()
    @root = File.expand_path('~/develop/git-repo/')
    @drip = MyDrip
    k, = @drip.head(1, 'rbcrawl-begin')[0]
    @fence = k || 0
  end

  def last_mtime(fname)
    k, v, = @drip.head(1, 'rbcrawl-fname=' + fname)[0]
    (v && k > @fence) ? v[1] : Time.at(1)
  end

  def do_crawl
    synchronize do
      ary = []
      Dir.chdir(@root)
      Dir.glob('**/*.rb').each do |fname|
        mtime = File.mtime(fname)
        next if last_mtime(fname) >= mtime
        @drip.write([fname, mtime, File.read(fname)],
                    'rbcrawl', 'rbcrawl-fname=' + fname)
        ary << fname
      end
      @drip.write(ary, 'rbcrawl-footprint')
      ary
    end
  end
  
  def quit
    synchronize do
      exit(0)
    end
  end
end

if __FILE__ == $0
  crawler = Crawler.new
  Thread.new do
    while true
      pp crawler.do_crawl
      sleep 60
    end
  end
  
  gets
  crawler.quit
end


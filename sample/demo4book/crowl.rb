require 'pp'
require 'my_drip'

class Crowler
  def initialize
    @root = File.expand_path('~')
    @drip = MyDrip
  end

  def last_mtime(fname)
    k, v, = @drip.head(1, 'rbcrowl-fname=' + fname)[0]
    v ? v[1] : Time.at(1)
  end

  def do_crowl
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
    pp ary
  end
end

crowler = Crowler.new
crowler.do_crowl


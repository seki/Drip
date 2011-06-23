class GoogleChartApi
  NtoS = ('A'..'Z').to_a + ('a'..'z').to_a + ('0'..'9').to_a
  NtoE = NtoS + %w(- .)
  URL = 'http://chart.apis.google.com/chart'

  def initialize
    @query = []
  end

  def as_s(n)
    NtoS[n]
  end
  
  def as_e(n)
    h, v = n.divmod(64)
    NtoE[h] + NtoE[v]
  end
  
  def chd_s(*ary_ary)
    @query << "chd=s:" + ary_ary.collect do |ary|
      ary.collect {|n| as_s(n)}.join('')
    end.join("|")
  end

  def chd_e(*ary_ary)
    @query << "chd=e:" + ary_ary.collect do |ary|
      ary.collect {|n| as_e(n)}.join('')
    end.join("|")
  end
  
  def chd_t(*ary_ary)
    @query << "chd=t:" + ary_ary.collect do |ary|
      ary.join(',')
    end.join("|")
  end
  
  def chs(w, h)
    @query << "chs=#{w}x#{h}"
  end

  def chtt(title)
    @query << "chtt=#{title.gsub(/ /, '+') .gsub(/\n/m, '|')}"
  end

  def method_missing(name, *args, &blk)
    if /^ch/ =~ name
      @query << "#{name}=#{args[0]}"
    else
      super
    end
  end

  def to_s
    [URL, @query.join("&")].join("?")
  end
end

gca = GoogleChartApi.new

gca.chs(460, 200)
gca.chd_t([62,12,5,2,19])
gca.cht('lc')
gca.chxt('r')
gca.chxr('0,60,130')
gca.chxl('0:80|100|120')
gca.chxp('0,80,100,120')

puts gca
system("open '#{gca}'")


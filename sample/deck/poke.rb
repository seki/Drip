# -*- coding: utf-8 -*-
require 'hpricot'
require 'open-uri'
require 'erb'
require 'nkf'

class PokemonCardCom
  def initialize(cache)
    @cache = cache
  end
  
  def host
    'http://www.pokemon-card.com'
  end

  def search_by_name_1(name, page=nil)
    url = host + '/card/index.php' + 
      '?mode=imagelist&sm_and_keyword=true&keyword=' + ERB::Util.u(name)
    url += "&page=#{page}" if page
    open(url)
  end

  def search_by_name(name)
    page = nil
    while true
      doc = Hpricot(search_by_name_1(name, page))
      doc.search('ul[@class="clearFix cardList"]/li').each do |e|
        yield(e.at('a')['href'], e.at('img')['src'])
      end
      if doc.at('ul[@class="pagination"]/li[@class="next"]/a')
        page ||= 1
        page += 1
        p [:page, page]
      else
        break
      end
    end
    nil
  end

  def get(path)
    if @cache
      @cache[host + path]
    else
      open(host + path)
    end
  end
  
  def card_summary(path)
    doc = Hpricot(get(path))
    div = doc.at('div[@class="specData"]')
    return nil unless div
    spec = {}
    spec[:name] = div.at('h2').inner_text
    spec[:type] = div.at('h3').next.next.at('dt').inner_text
    basic_data = div.at('dl[@class="basicData"]/dt')
    if basic_data && basic_data.inner_text == 'LV.'
      spec[:lv] = div.at('dl[@class="basicData"]/dd').inner_text
    else
      spec[:lv] = 'n/a'
    end
    spec[:series] = div.at('dl[@class="clearFix collectNo"]/dt/img')['alt']
    spec[:series_name] = doc.search('ul[@class="linkList01"]/li').collect {|e|
      e.inner_text
    }
    spec[:id] = div.at('dl[@class="clearFix collectNo"]/dt/img').next.to_html.gsub(/\&nbsp\;/, ' ').gsub(/\s/, '')
    spec[:path] = path
    spec[:illust] = doc.at('div[@class="illustData"]/p/img')['src']
    spec
  end
end

if __FILE__ == $0
  name = ARGV.shift
  there = PokemonCardCom.new({})
  there.search_by_name(name) do |url, img|
    p [url, img]
  end
end




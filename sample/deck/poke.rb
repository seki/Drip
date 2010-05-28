# -*- coding: utf-8 -*-
require 'hpricot'
require 'open-uri'
require 'erb'
require 'nkf'
require 'monitor'

class PageFolder
  include MonitorMixin

  def initialize(drop)
    super()
    @drop = drop
  end

  def get(prop)
    k, v = @drop.read_before(nil, prop)
    return v if k
    yield(@drop)
  end

  def get_page(uri)
    get("uri=#{uri}") { |drop|
      synchronize do
        begin
          p [:open, uri]
          body = open(uri).read
          return '' if body.nil? || body.empty?
          drop.write({"uri=#{uri}" => uri, "body" => body})
          return body
        rescue
          return ''
        end
      end
    }['body']
  end

  def [](uri)
    k, v = @drop.read_before(nil, "uri=#{uri}")
    return v['body'] if k
    synchronize do
      begin
        p [:open, uri]
        body = open(uri).read
        return '' if body.nil? || body.empty?
        @drop.write({"uri=#{uri}" => uri, "body" => body})
        body
      rescue
        ''
      end
    end
  end
end

class PokemonCardCom
  def initialize(drop)
    @cache = PageFolder.new(drop)
    @drop = drop
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
      it = @cache[host + path]
      return it if it
    end
    open(host + path)
  end
  
  def card_summary_1(path)
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
  
  def card_summary(path)
    @cache.get("pcc_summary=#{path}") { |drop|
      p [:summary, path]
      spec = card_summary_1(path)
      drop.write({"pcc_summary=#{path}" => path, 'value' => spec})
      return spec
    }['value']
  end
end

if __FILE__ == $0
  require 'drop'

  name = ARGV.shift
  there = PokemonCardCom.new(Drop.new('drop_db'))
  p there.card_summary(name)
end

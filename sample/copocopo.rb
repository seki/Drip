# -*- coding: utf-8 -*-
require 'drip_tw'
require 'my_drip'
require 'date'
require 'pp'

def dig(root, *keys)
  keys.inject(root) do |node, key|
    return nil if node.nil?
    node[key] rescue nil
  end
end

class CopoCopo
  def initialize(drip=MyDrip)
    @app = DripDemo.new('CopoCopo OAuth')
    @drip = drip
    _, @last = @drip.older(nil, 'CopoCopo Footprint')
    @last = 0 if @last.nil?
  end
  attr_reader :app

  def extract(str)
    ary = []
    str.scan(/(([ぁ-ん]{2,})\2)|(([ァ-ヴ]{2,})\4)/) do |x|
      ary << (x[1] || x[3])
    end
    ary
  end

  def created_at(event)
    DateTime.parse(event['created_at']).to_time
  rescue
    Time.at(1)
  end

  def make_status(ary, name)
    "@#{name} " + ary.collect { |s|
      "#{s}#{s}、#{s}"
    }.join(", ") + "　(by copocopo)"
  end
  
  def main_loop
    while true
      @last, event = @drip.read_tag(@last, 'DripDemo Event', 1)[0]
      ary = extract(event['text'] || '')
      if ary.size > 0
        next unless Time.now < created_at(event) + 6000
        name = dig(event, 'user', 'screen_name')
        tweet_id = event['id']
        if ['m_seki', 'miwa719', 'hsbt', 'vestige', 'mame'].include?(name)
          @app.update(make_status(ary, name), tweet_id)
        end
        @drip.write(@last, 'CopoCopo Footprint')
      end
    end
  end
end

copo = CopoCopo.new
app = copo.app

unless app.has_token?
  url = app.pin_url
  puts url
  system('open ' + url) # for OSX
  app.set_pin(gets.scan(/\w+/)[0])
  app.write_setting
end

copo.main_loop


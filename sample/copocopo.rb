# -*- coding: utf-8 -*-
require 'drip_tw'
require 'my_drip'
require 'pp'

def dig(root, *keys)
  keys.inject(root) do |node, key|
    return nil if node.nil?
    node[key] rescue nil
  end
end

class CopoCopo
  def initialize(drip=MyDrip)
    @app = DripDemo.new
    @drip = drip
    _, @last = @drip.older(nil, 'CopoCopo Footprint')
    @last = 0 if @last.nil?
  end
  attr_reader :last

  def extract(str)
    ary = []
    str.scan(/(([ぁ-ん]{2,})\2)|(([ァ-ヴ]{2,})\4)/) do |x|
      ary << (x[1] || x[3])
    end
    ary
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
        name = dig(event, 'user', 'screen_name')
        tweet_id = event['id']
        if ['m_seki', 'miwa719', 'vestige', 'mame'].include?(name)
          @app.update(make_status(ary, name), tweet_id)
        end
        @drip.write(@last, 'CopoCopo Footprint')
      end
    end
  end
end

copo = CopoCopo.new
copo.main_loop

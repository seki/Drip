# -*- coding: utf-8 -*-
require 'drip_tw'

def last_tweet_id
  key = nil
  while kv = MyDrip.older(key, 'DripDemo MyStatus')
    key, value = kv
    return value['id'] if value['id']
  end
  nil
end

def get_user_timeline(app, since_id, max_id)
  count = 3
  begin
    ary = app.user_timeline(since_id, max_id)
  rescue
    p $!
    count -= 1
    return nil, [] if count <= 0
    sleep 5
    retry
  end
  max_id = nil
  ary.reverse_each do |event|
    next unless event['id']
    max_id = event['id'] - 1
    break
  end
  return max_id, ary
end

app = DripDemo.new

since_id = last_tweet_id

max_id = nil
timeline = []
while true
  max_id, ary = get_user_timeline(app, since_id, max_id)
  break if ary.empty?
  pp [ary[0]['text'], ary[-1]['text']] rescue nil
  sleep 5
  timeline += ary
end

p [timeline.size, 'continue?']
gets

timeline.reverse_each do |event|
  MyDrip.write(event, 'DripDemo MyStatus')
end

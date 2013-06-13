require 'simple-oauth'
require 'drb'
require 'pp'
require 'json'
require 'my_drip'

class DripFiber
  def initialize(app)
    @app = app
    @fiber = Fiber.new do |event|
      story(event)
    end
  end

  def story(event)
    pending = []
    while event['id_str'].nil?
      pending << event
      event = Fiber.yield
    end
    
    @app.fill_timeline(event['id_str'])

    while event = pending.shift
      @app.write(event)
    end
    
    while true
      event = Fiber.yield
      @app.write(event)
    end
  end
  
  def push(event)
    @fiber.resume(event)
  end
end

class JSONStream
  def initialize(drip)
    @buf = ''
    @drip = drip
  end

  def push(str)
    @buf << str
    while (line = @buf[/.+?(\r\n)+/m]) != nil
      begin
        @buf.sub!(line,"")
        line.strip!
        event = JSON.parse(line)
      rescue
        break
      end
      pp event if $DEBUG
      @drip.push(event)
    end
  end
end

class SimpleOAuthS < SimpleOAuth
  def http_class
    return Net::HTTP unless ENV['http_proxy']
    proxy_url = URI.parse(ENV['http_proxy'])
    Net::HTTP.Proxy(proxy_url.host, proxy_url.port)
  end

  def request(method, url, body =nil, headers = {}, &block)
    method = method.to_s
    url = URI.parse(url)
    request = create_http_request(method, url.request_uri, body, headers)
    request['Authorization'] = auth_header(method, url, request.body)
    http = http_class.new(url.host, url.port)
    if url.scheme == 'https'
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_NONE
      store = OpenSSL::X509::Store.new
      store.set_default_paths
      http.cert_store = store
    end
    http.request(request, &block)
  end
end

class DripDemo
  def initialize(oauth_tag = 'DripDemo OAuth')
    @oauth_tag = oauth_tag
    @oa = last_setting || {}
  end

  def has_token?
    @oa.include?(:oauth_token)
  end

  def last_setting
    MyDrip.older(nil, @oauth_tag)[1]
  end

  def write_setting
    MyDrip.write(@oa, @oauth_tag)
  end

  def update_setting(body, keys=nil)
    found = []
    body.split('&').each do |pair|
      k, v = pair.split('=')
      if keys.nil? || keys.include?(k)
        @oa[k.intern] = v
        found << k
      end
    end
    found
  end

  def oauth
    SimpleOAuthS.new(@oa[:consumer_key],
                     @oa[:consumer_secret],
                     @oa[:oauth_token],
                     @oa[:oauth_token_secret])
  end
  
  def pin_url
    @oa[:oauth_token] = ''
    @oa[:oauth_token_secret] = ''
    response = oauth.get('https://api.twitter.com/oauth/request_token')
    raise response.message unless response.code == '200'
    update_setting(response.body, ['oauth_token', 'oauth_token_secret'])
    
    'http://twitter.com/oauth/authorize?oauth_token=' + @oa[:oauth_token]    
  end

  def set_pin(pin)
    response = oauth.get('https://api.twitter.com/oauth/access_token',
                         'oauth_token' => @oa[:oauth_token],
                         'oauth_velifier' => pin)
    raise response.message unless response.code == '200'
    update_setting(response.body)
  end

  def drip_stream
    json = JSONStream.new(DripFiber.new(self))
    oauth.request(:GET, 'https://userstream.twitter.com/2/user.json') do |r|
      r.read_body do |chunk|
        json.push(chunk)
      end
    end
  end

  def home_timeline(since_id, max_id)
    url = "http://api.twitter.com/1.1/statuses/home_timeline.json?count=200&include_entities=true"
    url += "&since_id=#{since_id}" if since_id
    url += "&max_id=#{max_id}" if max_id
    r = oauth.request(:GET, url)
    JSON.parse(r.body)
  end

  def user_timeline(since_id, max_id)
    url = "http://api.twitter.com/1.1/statuses/user_timeline.json?count=200&include_entities=true&trim_user=t"
    url += "&since_id=#{since_id}" if since_id
    url += "&max_id=#{max_id}" if max_id
    r = oauth.request(:GET, url)
    JSON.parse(r.body)
  end

  def last_tweet_id
    key = nil
    while kv = MyDrip.older(key, 'DripDemo Event')
      key, value = kv
      return value['id_str'] if value.include?('text')
    end
    nil
  end

  def fill_timeline(max_id)
    since_id = last_tweet_id
    return unless since_id
    timeline = []
    4.times do
      return if since_id == max_id || max_id.nil?
      ary = home_timeline(since_id, max_id)
      pp [:fill_timeline, ary.size] if $DEBUG
      max_id = nil
      ary.reverse_each do |event|
        next unless event['id']
        max_id = event['id'] - 1
        break
      end
      break if max_id.nil?
      timeline += ary
    end
    timeline.reverse_each do |event|
      write(event)
    end
  end

  def compact_event(event)
    return event unless Hash === event
    result = {}
    event.each do |k, v|
      case v
      when Hash
        v = compact_event(v)
        next if v.nil?
      when [], '', 0, nil
        next
      when Array
        v = v.collect {|vv| compact_event(vv)}
      end
      if k == 'user'
        tmp = {}
        %w(name screen_name id id_str).each {|attr| tmp[attr] = v[attr]}
        v = tmp
      end
      result[k] = v
    end
    result.size == 0 ? nil : result
  end
  
  def write(event, tag='DripDemo Event')
    event = compact_event(event)
    key = MyDrip.write(event, tag)
    pp [key, event['id_str'], event['text']] if $DEBUG
  end

  def update(str, in_reply_to=nil)
    hash = { :status => str }
    hash[:in_reply_to_status_id] = in_reply_to if in_reply_to
    r = oauth.post('http://api.twitter.com/1.1/statuses/update.xml',
                   hash)
    pp r.body if $DEBUG
  end

  def test
    r = oauth.post('http://api.twitter.com/1.1/statuses/update.xml',
                   {:status => 'test'})
    pp r.body if $DEBUG
  end
end

if __FILE__ == $0
  app = DripDemo.new
  
  unless app.has_token?
    url = app.pin_url
    puts url
    system('open ' + url) # for OSX
    app.set_pin(gets.scan(/\w+/)[0])
    app.write_setting
  end

  unless $DEBUG
    Process.daemon
  end
  app.drip_stream
end

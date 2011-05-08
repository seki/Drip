require 'simple-oauth'
require 'drb'
require 'pp'
require 'json'

MyDrip = DRbObject.new_with_uri('drbunix:' + File.expand_path('~/.drip/port'))

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
      @drip.write(event, 'DripDemo Event')
    end
  end
end

class SimpleOAuthS < SimpleOAuth
  def request(method, url, body =nil, headers = {}, &block)
    method = method.to_s
    url = URI.parse(url)
    request = create_http_request(method, url.request_uri, body, headers)
    request['Authorization'] = auth_header(method, url, request.body)
    http = Net::HTTP.new(url.host, url.port)
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
  def initialize
    @oa = last_setting || {}
  end

  def has_token?
    @oa.include?(:oauth_token)
  end

  def last_setting
    MyDrip.older(nil, 'DripDemo OAuth')[1]
  end

  def write_setting
    MyDrip.write(@oa, 'DripDemo OAuth')
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
    json = JSONStream.new(MyDrip)
    oauth.request(:GET,
                  'https://userstream.twitter.com/2/user.json') do |r|
      r.read_body do |chunk|
        json.push(chunk)
      end
    end
  end
end

app = DripDemo.new

unless app.has_token?
  url = app.pin_url
  puts url
  system('open ' + url) # for OSX
  app.set_pin(gets.scan(/\w+/)[0])
  app.write_setting
end

app.drip_stream



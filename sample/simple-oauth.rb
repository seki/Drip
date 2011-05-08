#
#     import from https://github.com/shibason/rb-simple-oauth
#     see: http://d.hatena.ne.jp/shibason/20090809/1249764381
require 'uri'
require 'net/http'
require 'openssl'

class SimpleOAuth
  def initialize(consumer_key, consumer_secret, token, token_secret)
    @consumer_key = consumer_key
    @consumer_secret = consumer_secret
    @token = token
    @token_secret = token_secret
    # This class supports only 'HMAC-SHA1' as signature method at present.
    @signature_method = 'HMAC-SHA1'
  end

  def get(url, headers = {})
    request(:GET, url, nil, headers)
  end

  def head(url, headers = {})
    request(:HEAD, url, nil, headers)
  end

  def post(url, body = nil, headers = {})
    request(:POST, url, body, headers)
  end

  def put(url, body = nil, headers = {})
    request(:PUT, url, body, headers)
  end

  def delete(url, headers = {})
    request(:DELETE, url, nil, headers)
  end

private
  def request(method, url, body = nil, headers = {})
    method = method.to_s
    url = URI.parse(url)
    request = create_http_request(method, url.request_uri, body, headers)
    request['Authorization'] = auth_header(method, url, request.body)
    Net::HTTP.new(url.host, url.port).request(request)
  end

  RESERVED_CHARACTERS = /[^a-zA-Z0-9\-\.\_\~]/

  def escape(value)
    URI.escape(value.to_s, RESERVED_CHARACTERS)
  end

  def encode_parameters(params, delimiter = '&', quote = nil)
    if params.is_a?(Hash)
      params = params.map do |key, value|
        "#{escape(key)}=#{quote}#{escape(value)}#{quote}"
      end
    else
      params = params.map { |value| escape(value) }
    end
    delimiter ? params.join(delimiter) : params
  end

  VERSION = '0.1'
  USER_AGENT = "SimpleOAuth/#{VERSION}"

  def create_http_request(method, path, body, headers)
    method = method.capitalize.to_sym
    request = Net::HTTP.const_get(method).new(path, headers)
    request['User-Agent'] = USER_AGENT
    if method == :Post || method == :Put
      request.body = body.is_a?(Hash) ? encode_parameters(body) : body.to_s
      request.content_type = 'application/x-www-form-urlencoded'
      request.content_length = (request.body || '').length
    end
    request
  end

  def auth_header(method, url, body)
    parameters = oauth_parameters
    parameters[:oauth_signature] = signature(method, url, body, parameters)
    'OAuth ' + encode_parameters(parameters, ', ', '"')
  end

  OAUTH_VERSION = '1.0'

  def oauth_parameters
    {
      :oauth_consumer_key => @consumer_key,
      :oauth_token => @token,
      :oauth_signature_method => @signature_method,
      :oauth_timestamp => timestamp,
      :oauth_nonce => nonce,
      :oauth_version => OAUTH_VERSION
    }
  end

  def timestamp
    Time.now.to_i.to_s
  end

  def nonce
    OpenSSL::Digest::Digest.hexdigest('MD5', "#{Time.now.to_f}#{rand}")
  end

  def signature(*args)
    base64(digest_hmac_sha1(signature_base_string(*args)))
  end

  def base64(value)
    [ value ].pack('m').gsub(/\n/, '')
  end

  def digest_hmac_sha1(value)
    OpenSSL::HMAC.digest(OpenSSL::Digest::SHA1.new, secret, value)
  end

  def secret
    escape(@consumer_secret) + '&' + escape(@token_secret)
  end

  def signature_base_string(method, url, body, parameters)
    method = method.upcase
    base_url = signature_base_url(url)
    parameters = normalize_parameters(parameters, body, url.query)
    encode_parameters([ method, base_url, parameters ])
  end

  def signature_base_url(url)
    URI::HTTP.new(url.scheme, url.userinfo, url.host, nil, nil, url.path,
                  nil, nil, nil)
  end

  def normalize_parameters(parameters, body, query)
    parameters = encode_parameters(parameters, nil)
    parameters += body.split('&') if body
    parameters += query.split('&') if query
    parameters.sort.join('&')
  end
end

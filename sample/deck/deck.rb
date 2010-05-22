require 'open-uri'
require 'drop'
require 'tofu'
require 'poke'
require 'monitor'

class PageFolder
  include MonitorMixin
  def initialize(drop)
    super()
    @drop = drop
  end
  
  def [](uri)
    k, v = @drop.read_before(nil, "uri=#{uri}")
    return v['body'] if k
    synchronize do
      begin
        p [:open, uri]
        body = open(uri).read
        @drop.write({"uri=#{uri}" => uri, "body" => body})
        body
      rescue
        nil
      end
    end
  end
end

class BaseDiv < Tofu::Div
  set_erb('base.erb')
  
  def initialize(session)
    super(session)
    @enter = EnterDiv.new(session)
    @list = ListDiv.new(session)
  end
end

class EnterDiv < Tofu::Div
  set_erb('enter.erb')
  
  def div_id
    'enter'
  end

  def do_search(context, params)
    p params
    text ,= params['str']
    session.do_search(text)
  end
end

class ListDiv < Tofu::Div
  set_erb('list.erb')
end

class DeckSession < Tofu::Session
  def initialize(bartender, hint=nil)
    super
    @base = BaseDiv.new(self)
    @drop = $drop
    @folder = $page_folder
    @pcc = PokemonCardCom.new(@folder)
    @result = []
    @text = ''
  end
  attr_reader :folder, :result, :text

  def add(url)
    @folder[url]
  end

  def do_GET(context)
    update_div(context)

    case context.req_path_info
    when /card_image/
      get_image(context)
    else
      get_result(context)
    end
  end

  def do_search(text)
    @text = text
    ary = []
    @pcc.search_by_name(text) do |detail, thumb|
      it = @pcc.card_summary(detail)
      it[:thumb] = thumb
      ary << it
    end
    @text = text
    @result = ary.sort_by {|x| [x[:name], x[:path]]}
  end

  def get_image(context)
    basename = File.extname(context.req_path_info)
    case basename
    when '.gif'
      context.res_header('content-type', 'image/gif')
    else
      context.res_header('content-type', 'image/jpeg') #FIXME
    end
    context.res_body(@pcc.get(context.req_path_info))
  end
  
  def get_result(context)
    context.res_header('content-type', 'text/html; charset=utf-8')
    context.res_body(@base.to_html(context))
  end
end

$drop = Drop.new('drop_db')
$page_folder = PageFolder.new($drop)

uri = ARGV.shift || 'druby://localhost:54322'
tofu = Tofu::Bartender.new(DeckSession)
DRb.start_service(uri, Tofu::CGITofulet.new(tofu))
gets




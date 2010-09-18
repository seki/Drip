require 'open-uri'
require 'drop'
require 'tofu'
require 'poke'
require 'monitor'
require 'drawer'

class BaseDiv < Tofu::Div
  set_erb('base.erb')
  
  def initialize(session)
    super(session)
    @enter = EnterDiv.new(session)
    @list = ListDiv.new(session)
    @drawers = []
    @drawers << DrawerDiv.new(session, 'pokemon')
    @drawers << DrawerDiv.new(session, 'trainer')
    @drawers << DrawerDiv.new(session, 'energy')
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
    @drop = $drop
    @pcc = PokemonCardCom.new(@drop)
    @result = []
    @text = ''
    @base = BaseDiv.new(self)
  end
  attr_reader :result, :text, :pcc

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
      next unless it
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
    context.res_header('cache-contol','max-age=2592000')
    context.res_body(@pcc.get(context.req_path_info))
  end
  
  def get_result(context)
    context.res_header('content-type', 'text/html; charset=utf-8')
    context.res_body(@base.to_html(context))
  end
end

$drop = Drop.new('drop_db')

uri = ARGV.shift || 'druby://localhost:54322'
tofu = Tofu::Bartender.new(DeckSession)
DRb.start_service(uri, Tofu::CGITofulet.new(tofu))
gets




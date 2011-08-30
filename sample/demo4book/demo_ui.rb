require 'index'
require 'crawl'
require 'webrick/cgi'
require 'erb'

class DemoListView
  include ERB::Util
  extend ERB::DefMethod
  def_erb_method('to_html(word, list)', ERB.new(<<EOS))
<html><head><title>Demo UI</title></head><body>
<form method="post"><input type="text" name="w" value="<%=h word %>" /></form>
<% if word %>
<p>search: <%=h word %></p>
<ul>
<%   list.each do |fname| %>
<li><%=h fname%></li>
<%   end %>
</ul>
<% end %>
</body></html>
EOS
end

class DemoUICGI < WEBrick::CGI
  def initialize(crawler, indexer, *args)
    super(*args)
    @crawler = crawler
    @indexer = indexer
    @list_view = DemoListView.new
  end

  def req_query(req, key)
    value ,= req.query[key]
    return nil unless value
    value.force_encoding('utf-8')
    value
  end

  def do_GET(req, res)
    if req.path_info == '/quit'
      Thread.new do
        @crawler.quit
      end
    end
    word = req_query(req, 'w') || ''
    list = word.empty? ? [] : @indexer.dict.query(word)
    res['content-type'] = 'text/html; charset=utf-8'
    res.body = @list_view.to_html(word, list)
  end
  
  alias do_POST do_GET
end

if __FILE__ == $0
  crawler = Crawler.new
  Thread.new do
    while true
      pp crawler.do_crawl
      sleep 60
    end
  end

  indexer = Indexer.new
  Thread.new do
    indexer.update_dict
  end
  
  cgi = DemoUICGI.new(crawler, indexer)
  DRb.start_service('druby://localhost:50830', cgi)
  DRb.thread.join
end

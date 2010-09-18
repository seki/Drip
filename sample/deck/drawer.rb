class Drawer
  def initialize(name, pcc)
    @name = name
    @card = []
    @pcc = pcc
  end
  attr_reader :name, :card
  
  def each(&blk)
    @card.each(&blk)
  end
  
  def add(path)
    path = URI.parse(path).path
    @card << @pcc.card_summary(path)
  end
end

class DrawerDiv < Tofu::Div
  set_erb('drawer.erb')

  def initialize(session, name='untitled')
    super(session)
    @drawer = Drawer.new(name, session.pcc)
  end
  attr_reader :drawer

  def do_add(context, param)
    path ,= param['path']
    p [:path, path]
    return if path.nil? || path.empty?
    @drawer.add(path)
  end
end

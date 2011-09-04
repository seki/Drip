class Foo
  # initialize foo
  def initialize(name)
    @foo = name
  end

  def foo; end
  def baz; end
end

class Bar < Foo
  # initialize bar and foo
  def initialize(name)
    super("bar #{name}")
  end
  def bar; end
end


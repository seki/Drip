require 'rbtree'
require 'nkf'

class Query2
  def initialize
    @tree = RBTree.new
  end
  
  def push(word, fname, lineno)
    @tree[[word, fname, lineno]] = true
  end

  def fwd(w1, fname, lineno)
    k, v = @tree.lower_bound([w1, fname, lineno])
    return nil unless k
    return nil unless k[0] == w1
    k[1..2]
  end

  def query2(w1, w2)
    f1 = fwd(w1, '', 0)
    f2 = fwd(w2, '', 0)
    while f1 && f2
      cmp = f1 <=> f2
      if cmp > 0
        f2 = fwd(w2, *f1)
      elsif cmp < 0
        f1 = fwd(w1, *f2)
      else
        yield(f1)
        f1 = fwd(w1, f1[0], f1[1] + 1)
        f2 = fwd(w2, f2[0], f2[1] + 1)
      end
    end
  end
end

if __FILE__ == $0
  q2 = Query2.new
  while line = ARGF.gets
    NKF.nkf('-w', line).scan(/\w+/) do |word|
      q2.push(word, ARGF.filename, ARGF.lineno)
    end
  end
  q2.query2('def', 'initialize') {|x| p x}
end


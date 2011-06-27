# -*- coding: utf-8 -*-
require 'my_drip'
require 'MeCab'
require 'pp'

class Bag
  def initialize
    @bag = Hash.new(0)
    @size = 0
    @cache = nil
  end
  attr_reader :size
  
  def push(obj)
    @cache = nil
    @bag[obj] += 1
    @size += 1
    obj
  end

  def sample
    @cache = @bag.sort_by {|k, v| v} unless @cache
    prob = rand(@size)
    @cache.each do |k, v|
      prob -= v
      return k if prob < 0
    end
  end
end

class BagInBag < Bag
  def initialize(&blk)
    super
    @bag_in_bag = Hash.new {|h, k| h[k] = blk.call}
  end
  
  def push(obj, *rest)
    super(obj)
    @bag_in_bag[obj].push(*rest)
  end

  def [](key, *rest)
    if rest.empty?
      @bag_in_bag[key]
    else
      @bag_in_bag[key][*rest]
    end
  end
end

class EntityEnum
  def initialize(str, entity)
    @str = str || ''
    @entity = []
    (entity || []).each do |k, v|
      @entity += v
    end
  end

  def each
    unless @entity
      yield(@str, nil)
      return
    end
    ary = @entity.sort_by {|e| e['indices'][0]}
    cursor = 0
    while head = ary.shift
      left, right = head['indices']
      if left > cursor
        yield(@str[cursor ... left], nil)
      end
      yield(@str[left .. right], head)
      cursor = right + 1
    end
    if @str.length > cursor
      yield(@str[cursor ... @str.length], nil)
    end
  end
end

class Markov
  def initialize
    @bag = BagInBag.new { BagInBag.new { Bag.new } }
    @start = Bag.new
    @intern = Hash.new
  end

  def intern(str)
    @intern[str] ||= str
    @intern[str]
  end

  def add(ary)
    return if ary.size <= 3
    ary = ary.collect {|s| intern(s)}
    @start.push(ary[0])
    ary.each_cons(3) do |p0, p1, suffix|
      @bag.push(p0, p1, suffix)
    end
  end

  def generate(max_len=30)
    s = @start.sample
    ary = [s, @bag[s].sample]
    max_len.times do
      it = @bag[*ary[-2, 2]].sample
      break if it == :eos
      ary << it
    end
    ary
  end
end

$screen_name = {}

def mention_user
  " @#{$screen_name.keys.sample} "
end

def as_str(ary)
  ary = ary.map {|x| x == :name ? mention_user : x}
  ary.inject{|s,c|s+=(s+"\00"+c=~/\w\00\w/?" ":"")+c}
end

markov = Markov.new
m = MeCab::Tagger.new('-Owakati')

MyDrip.head(4000, 'DripDemo MyStatus').each do |k, v|
  next if v['retweeted_status']
  entity = v['entities']
  str = v['text']
  ary = []
  EntityEnum.new(str, entity).each do |s, kind|
    next if s == 'RT'
    unless kind
      ary += m.parse(s).split(' ')
    else
      ary <<:name if kind['screen_name']
      if kind['screen_name']
        $screen_name[kind['screen_name']] = true
      end
    end
  end
  markov.add(ary + [:eos])
end

while gets
  10.times do
    ary = markov.generate
    next if ary.size < 10
    puts as_str(ary)
    puts
  end
end

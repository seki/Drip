# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "drip/version"

Gem::Specification.new do |s|
  s.name        = "drip"
  s.version     = Drip::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Masatoshi Seki"]
  s.homepage    = "https://github.com/seki/Drip"
  s.summary     = %q{Simple RD-Stream for Rinda::TupleSpace lovers.}
  s.description = ""

  s.rubyforge_project = "drip"
  s.add_dependency "rbtree"
  s.add_development_dependency "test-unit"
  s.add_development_dependency "sqlite3"
  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end

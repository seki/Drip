require 'rbconfig'
require 'fileutils'

dest = RbConfig::CONFIG['sitelibdir']
src = 'lib/drip.rb'
FileUtils.install(src, dest, {:verbose => true, :mode => 0644})
                  

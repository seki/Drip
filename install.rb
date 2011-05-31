require 'rbconfig'
require 'fileutils'

dest = RbConfig::CONFIG['sitelibdir']
src = ['lib/drip.rb', 'lib/my_drip.rb']

src.each do |s|
  FileUtils.install(s, dest, {:verbose => true, :mode => 0644})
end
                  

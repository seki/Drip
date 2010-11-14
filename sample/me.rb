require 'drop'
require 'drb'
require 'fileutils'

unless $DEBUG
  exit!(0) if fork
  Process.setsid
  exit!(0) if fork
end

dir = File.expand_path('~/.drop')
FileUtils.mkdir_p(dir)
FileUtils.cd(dir)

drop = Drop.new('drop')
DRb.start_service('drbunix:' + File.join(dir, 'port'), drop)
File.open('pid', 'w') {|fp| fp.puts($$)}

unless $DEBUG
  STDIN.reopen('/dev/null')
  STDOUT.reopen('/dev/null', 'w')
  STDERR.reopen('/dev/null', 'w')
end
DRb.thread.join


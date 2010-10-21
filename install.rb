require 'rbconfig'
require 'find'
require 'fileutils'

include RbConfig

$srcdir = CONFIG["srcdir"]
if CONFIG['ruby_version'] > '1.9'
  $version = CONFIG["MAJOR"]+"."+CONFIG["MINOR"]+"."+CONFIG["TEENY"]
else
  $version = CONFIG["MAJOR"]+"."+CONFIG["MINOR"]
end
$libdir = File.join(CONFIG["libdir"], "ruby", $version)
$archdir = File.join($libdir, CONFIG["arch"])
$site_libdir = $:.find {|x| x =~ /site_ruby$/}
$datadir = CONFIG["datadir"]
if !$site_libdir
  $site_libdir = File.join($libdir, "site_ruby")
elsif Regexp.new(Regexp.quote($version)) !~ $site_libdir
  $site_libdir = File.join($site_libdir, $version)
end

def install(basedir, srcdir, destdir)
  if srcdir
    target_dir = File.join(srcdir, basedir)
  else
    target_dir = basedir
  end
  
  path = []
  dir = []
  Find.find(target_dir) do |f|
    next unless FileTest.file?(f)
    next if (f = f[target_dir.length+1..-1]) == nil
    next if (/CVS$/ =~ File.dirname(f))
    next if (/\.svn/ =~ File.dirname(f))
    path.push f
    dir |= [File.dirname(f)]
  end
  for f in dir
    next if f == "."
    next if f == "CVS"
    next if f == ".svn"
    FileUtils.mkdir_p(File.join(destdir, f))
  end
  for f in path
    next if (/\~$/ =~ f)
    next if (/^\./ =~ File.basename(f))
    FileUtils.install(File.join(target_dir, f),
                      File.join(destdir, f),
                      :mode => 0644,
                      :verbose => true)
  end
end

def install_rb(srcdir=nil, destdir=$site_libdir)
  install("lib", srcdir, destdir)
end

def install_data(srcdir=nil, datadir=$datadir)
  install("data", srcdir, datadir)
end
    
def ARGV.switch
  return nil if self.empty?
  arg = self.shift
  return nil if arg == '--'
  if arg =~ /^-(.)(.*)/
    return arg if $1 == '-'
    raise 'unknown switch "-"' if $2.index('-')
    self.unshift "-#{$2}" if $2.size > 0
    "-#{$1}"
  else
    self.unshift arg
    nil
  end
end

def ARGV.req_arg
  self.shift || raise('missing argument')
end

destdir = $site_libdir
datadir = $datadir

begin
  while switch = ARGV.switch
    case switch
    when '-d', '--destdir'
      destdir = ARGV.req_arg
    when '--datadir'
      datadir = ARGV.req_arg
    else
      raise "unknown switch #{switch.dump}"
    end
  end
rescue
  STDERR.puts $!.to_s
  STDERR.puts File.basename($0) + 
    " [-d <destdir>]" +
    " [--datadir <datadir>]"
  exit 1
end    

install_rb(nil, destdir)
install_data(nil, datadir) rescue nil

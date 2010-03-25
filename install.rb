require 'rbconfig'
require 'find'
require 'ftools'

include Config

$srcdir = CONFIG["srcdir"]
$version = CONFIG["MAJOR"]+"."+CONFIG["MINOR"]
$libdir = File.join(CONFIG["libdir"], "ruby", $version)
$archdir = File.join($libdir, CONFIG["arch"])
$site_libdir = $:.find {|x| x =~ /site_ruby$/}
if !$site_libdir
  $site_libdir = File.join($libdir, "site_ruby")
elsif Regexp.new(Regexp.quote($version)) !~ $site_libdir
  $site_libdir = File.join($site_libdir, $version)
end

def install_rb(srcdir = nil, destdir = $site_libdir)
  libdir = "lib"
  libdir = File.join(srcdir, libdir) if srcdir
  path = []
  dir = []
  Find.find(libdir) do |f|
    next unless FileTest.file?(f)
    next if (f = f[libdir.length+1..-1]) == nil
    next if (/CVS$/ =~ File.dirname(f))
    next if (/\.svn/ =~ File.dirname(f))
    path.push f
    dir |= [File.dirname(f)]
  end
  for f in dir
    next if f == "."
    next if f == "CVS"
    next if f == ".svn"
    File::makedirs(File.join(destdir, f))
  end
  for f in path
    next if (/\~$/ =~ f)
    next if (/^\./ =~ File.basename(f))
    File::install(File.join("lib", f), File.join(destdir, f), 0644, true)
  end
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

begin
  while switch = ARGV.switch
    case switch
    when '-d', '--destdir'
      destdir = ARGV.req_arg
    else
      raise "unknown switch #{switch.dump}"
    end
  end
rescue
  STDERR.puts $!.to_s
  STDERR.puts File.basename($0) + 
    " -d <destdir>"
  exit 1
end    

install_rb(nil, destdir)


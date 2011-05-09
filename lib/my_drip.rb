require 'drb/drb'

MyDrip = DRbObject.new_with_uri('drbunix:' + File.expand_path('~/.drip/port'))

require 'sequement/master'

def debug(message)
  puts '[PID %d debug] %s' % [$$, message]
end

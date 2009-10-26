require 'sequement/master'

module Sequement
  COMMAND = {
    :heartbeat => 0,
    :next => 1
  }
  RESPONSE = {
    :ok => 0
  }
end

def debug(message)
  puts '[PID %d debug] %s' % [$$, message]
end

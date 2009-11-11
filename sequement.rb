require 'sequement/master'
require 'sequement/worker'
require 'sequement/sequence'
require 'sequement/writer'

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

# bind a signal handler to multiple signals
def traps(*args, &cmd)
  cmd = args.pop unless block_given?
  args.each { |signal| trap signal, cmd }
end

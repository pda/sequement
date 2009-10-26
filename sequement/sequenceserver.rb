require 'socket'

module Sequement

  class SequenceServer

    TIMEOUT = 2

    def initialize(acceptor, pipe_out, pipe_in)

      signal_init

      @acceptor = acceptor
      @pipe_out = pipe_out.writer!
      @pipe_in = pipe_in.reader!

    end

    def run
      until @stop do
        if IO.select([@acceptor], nil, nil, TIMEOUT) then
          socket, addr = @acceptor.accept
          @pipe_out.puts "next #$$"
          sequence = @pipe_in.gets.chomp
          socket.puts "[PID #$$] %d" % sequence
          socket.close
        end
        heartbeat
      end
      #debug "PID #$$ stopped"
    end

    def heartbeat
      #debug "sending heartbeat"
      @pipe_out.write "heartbeat #$$\n"
      if IO.select([@pipe_in], [], [], TIMEOUT)
        response = @pipe_in.gets.chop
        if response == 'OK'
          #debug 'received OK'
        else
          raise "PID #$$ Unexpected heartbeat response: " + response
        end
      else
        raise "PID #$$ timeout waiting for heartbeat response"
      end
    end

    private

    def signal_init
      @stop = false
      trap('INT') do
        if @stop
          puts "PID #$$ forced exit"
          exit
        else
          #debug "PID #$$ got SIGINT, setting @stop"
          @stop = true
        end
      end
    end

  end

end

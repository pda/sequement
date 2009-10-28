require 'socket'

module Sequement

  class Worker

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

          request = socket.gets.chop
          #debug 'request: %s' % request

          send_command :next, request
          sequence = @pipe_in.gets.chop
          socket.puts sequence
          socket.close
        end
        heartbeat
      end
      #debug "PID #$$ stopped"
    end

    def heartbeat
      send_command :heartbeat
      if IO.select([@pipe_in], [], [], TIMEOUT)
        response = @pipe_in.getc
        if response == RESPONSE[:ok]
          #debug 'received OK'
        else
          raise "PID #$$ Unexpected heartbeat response: " + response
        end
      else
        raise "PID #$$ timeout waiting for heartbeat response"
      end
    end

    private

    def send_command(command, data = nil)
      #debug 'sending %s' % command
      if data
        raise 'data exceeds maximum length' if data.length > 255
        @pipe_out.putc COMMAND[command]
        @pipe_out.putc data.length
        @pipe_out.write data
      else
        @pipe_out.putc COMMAND[command]
      end
    end

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

require 'socket'

module Sequement

  class Worker

    TIMEOUT = 2

    def initialize(acceptor, pipe_out, pipe_in, pipe_sig)

      @acceptor = acceptor
      @pipe_out = pipe_out
      @pipe_in = pipe_in
      @pipe_sig = pipe_sig

    end

    def start

      trap :INT, 'IGNORE'
      trap :TERM, 'DEFAULT'

      loop do
        if selected = IO.select([@acceptor, @pipe_sig], nil, nil, TIMEOUT)

          if selected.first.include? @pipe_sig
            debug "worker stopping"
            return
          end

          begin
            socket, addr = @acceptor.accept_nonblock

            request = socket.gets.chop
            #debug 'request: %s' % request

            send_command :next, request
            sequence = @pipe_in.gets.chop
            socket.puts sequence
            socket.close
          rescue Errno::EAGAIN, Errno::ECONNABORTED
          end
        end
        heartbeat
      end

    end

    def heartbeat
      send_command :heartbeat
      if IO.select([@pipe_in], [], [], TIMEOUT)

        if @pipe_in.eof?
          #debug 'worker got EOF from master, exiting'
          exit
        end

        response = @pipe_in.getc

        if response == RESPONSE[:ok]
          #debug 'received OK'
        else
          raise "PID #$$ Unexpected heartbeat response: " + response.to_s
        end

      else
        raise "PID #$$ timeout waiting for heartbeat response"
      end
    end

    private

    def send_command(command, data = nil)
      #debug 'sending %s' % command
      begin
        if data
          raise 'data exceeds maximum length' if data.length > 255
          @pipe_out.putc COMMAND[command]
          @pipe_out.putc data.length
          @pipe_out.write data
        else
          @pipe_out.putc COMMAND[command]
        end
      rescue Errno::EPIPE
        #debug 'broken pipe to master, worker exiting'
        exit
      end
    end

  end

end

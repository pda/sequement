require 'socket'

module Sequement

  class Worker

    TIMEOUT = 2

    attr_reader :pid, :pipe_from_child, :pipe_to_child

    def initialize(acceptor, pipe_sig)
      @acceptor = acceptor
      @pipe_sig = pipe_sig
    end

    def start

      @pipe_from_child = Pipe.new
      @pipe_to_child = Pipe.new

      if @pid = fork
        @pipe_from_child.reader!
        @pipe_to_child.writer!
      else
        $0 = 'sequement_worker'
        trap :INT, 'IGNORE'
        trap :TERM, 'DEFAULT'
        @pipe_from_child.writer!
        @pipe_to_child.reader!
        @pipe_sig.reader!
        select_loop
        exit
      end

      self

    end

    private

    def select_loop

      loop do
        if selected = IO.select([@acceptor, @pipe_sig], nil, nil, TIMEOUT)

          return if selected.first.include? @pipe_sig

          begin
            socket, addr = @acceptor.accept_nonblock
            request = socket.gets.chop
            send_command :next, request
            sequence = @pipe_to_child.gets.chop
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
      if IO.select([@pipe_to_child], [], [], TIMEOUT)

        if @pipe_to_child.eof?
          debug 'worker got EOF from master, exiting'
          exit
        end

        response = @pipe_to_child.getc

        unless response == RESPONSE[:ok]
          raise "PID #$$ Unexpected heartbeat response: " + response.to_s
        end

      else
        raise "PID #$$ timeout waiting for heartbeat response"
      end
    end

    def send_command(command, data = nil)
      begin
        if data
          raise 'data exceeds maximum length' if data.length > 255
          @pipe_from_child.putc COMMAND[command]
          @pipe_from_child.putc data.length
          @pipe_from_child.write data
        else
          @pipe_from_child.putc COMMAND[command]
        end
      rescue Errno::EPIPE
        debug 'broken pipe to master, worker exiting'
        exit
      end
    end

  end

end

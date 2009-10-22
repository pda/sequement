require 'socket'

module Sequement

  class SequenceServer

    TIMEOUT = 3

    def initialize(acceptor, hb_pipe, seq_pipe)

      signal_init

      @acceptor = acceptor
      @hb_pipe = hb_pipe.writer!
      @seq_pipe = seq_pipe.reader!

    end

    def run
      until @stop do
        if IO.select([@acceptor], nil, nil, TIMEOUT) then
          socket, addr = @acceptor.accept
          @hb_pipe.puts "next #$$"
          sequence = @seq_pipe.gets.chomp
          socket.puts "[PID #$$] %d" % sequence
          socket.close
        end
        heartbeat
      end
    end

    def heartbeat
      @hb_pipe.write "heartbeat #$$\n"
    end

    private

    def signal_init
      @stop = false
      trap('INT') do
        if @stop
          puts "PID #$$ forced exit"
          exit
        end
        @stop = true
      end
    end

  end

end

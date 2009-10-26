require 'sequement/sequenceserver'
require 'sequement/pipe'

module Sequement

  class Master

    SOCKET_TIMEOUT = 2
    SOCKET_BACKLOG = 10

    def initialize(host, port, concurrency)
      @host, @port = host, port
      @concurrency = concurrency
      @pipes_in, @pipes_out = {}, {}
      @sequence = 0
      signal_init
    end

    def start

      #debug 'creating listening socket'
      @socket = create_listen_socket
      trap('EXIT') { @socket.close } # applies to parent & child processes

      loop do
        spawn_up_to_concurrency unless @stop
        break if @pipes_in.empty?
        #debug 'select() on %d read pipes' % @pipes_in.length
        result = IO.select(@pipes_in.values, nil, nil, SOCKET_TIMEOUT) or redo
        result[0].each { |pipe| read_pipe pipe }
      end

      #debug "master loop ended, waiting for child processes.."
      Process.waitall

    end

    #######
    private

    def read_pipe(pipe)

      pid = @pipes_in.index(pipe)

      if pipe.eof
        #debug 'eof from pid %d' % pid
        @pipes_in.delete(pid)
        @pipes_out.delete(pid)
        return
      end

      command = pipe.read(1).unpack('c')[0]

      case command

        when COMMAND[:next]
          @pipes_out[pid].puts @sequence += 1

        when COMMAND[:heartbeat]
          #debug 'received heartbeat from %d' % pid
          @pipes_out[pid].write [RESPONSE[:ok]].pack('c')

        else
          raise "Unrecognized command from pipe: %d" % command
      end

    end

    def create_listen_socket
      #debug "Binding to #{@host}:#{@port}"
      socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
      socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, true)
      socket.bind(Socket.pack_sockaddr_in(@port, @host))
      socket.listen(SOCKET_BACKLOG)
      socket
    end

    # fork workers until configured concurrency
    def spawn_up_to_concurrency
      while @pipes_in.length < CONCURRENCY
        fork_worker
      end
    end

    # Forks a single worker, opening a pair of IPC pipes
    def fork_worker
      pipe_child_to_master = Pipe.new
      pipe_master_to_child = Pipe.new
      if pid = fork
        #debug 'forked PID %d' % pid
        @pipes_in[pid] = pipe_child_to_master.reader!
        @pipes_out[pid] = pipe_master_to_child.writer!
      else
        SequenceServer.new(
          @socket,
          pipe_child_to_master,
          pipe_master_to_child
        ).run
        exit
      end
    end

    def signal_init
      @stop = false
      trap('INT') do
        if @stop
          puts "Master PID #$$ forced exit"
          exit
        else
          puts "\nShutting down... (interrupt again to force exit)"
          @stop = true
        end
      end
    end

  end

end

require 'sequement/worker'
require 'sequement/pipe'

module Sequement

  class Master

    SOCKET_TIMEOUT = 2
    SOCKET_BACKLOG = 10

    def initialize(host, port, dir, concurrency)
      @host, @port = host, port
      @dir = dir
      @concurrency = concurrency
      @pipes_in, @pipes_out = {}, {}
      @sequences = {}
      signal_init
    end

    def start

      #debug 'creating listening socket'
      @socket = create_listen_socket
      trap('EXIT') { @socket.close } # applies to parent & worker processes

      loop do
        spawn_up_to_concurrency unless @stop
        break if @pipes_in.empty?
        #debug 'select() on %d read pipes' % @pipes_in.length
        result = IO.select(@pipes_in.values, nil, nil, SOCKET_TIMEOUT) or redo
        result[0].each { |pipe| read_pipe pipe }
      end

      #debug "master loop ended, waiting for worker processes.."
      Process.waitall

      #debug 'writing sequences to disk'
      @sequences.each_value { |seq| seq.save_sequence }

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

      command = pipe.read(1).unpack('C')[0]

      case command

        when COMMAND[:next]
          length = pipe.read(1).unpack('C')[0]
          seq_name = pipe.read(length)
          #debug 'seq_name: %s' % seq_name
          @pipes_out[pid].puts sequence(seq_name).next

        when COMMAND[:heartbeat]
          #debug 'received heartbeat from %d' % pid
          @pipes_out[pid].write [RESPONSE[:ok]].pack('C')

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
      pipe_worker_to_master = Pipe.new
      pipe_master_to_worker = Pipe.new
      if pid = fork
        #debug 'forked PID %d' % pid
        @pipes_in[pid] = pipe_worker_to_master.reader!
        @pipes_out[pid] = pipe_master_to_worker.writer!
      else
        Worker.new(
          @socket,
          pipe_worker_to_master,
          pipe_master_to_worker
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

    def sequence(name)
      @sequences.fetch name do
        @sequences[name] = Sequement::Sequence.new(name, @dir)
      end
    end

  end

end

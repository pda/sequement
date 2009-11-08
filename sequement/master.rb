require 'sequement/worker'
require 'sequement/sequence'
require 'sequement/writer'
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
      @pipe_sig_read, @pipe_sig_write = IO.pipe
      @pipe_writer_read, @pipe_writer_write = IO.pipe
      @writer = Sequement::Writer.new(@pipe_writer_read)
      signal_init
    end

    def start

      #debug 'creating listening socket'
      @socket = create_listen_socket
      trap('EXIT') { @socket.close } # applies to parent & worker processes

      loop do
        spawn_up_to_concurrency
        break if @pipes_in.empty?
        #debug 'select() on %d read pipes' % @pipes_in.length
        if selected = IO.select(@pipes_in.values + [@pipe_sig_read], nil, nil, SOCKET_TIMEOUT)
          break if selected.first.include? @pipe_sig_read
          selected.first.each { |pipe| read_pipe pipe }
        end
      end

      debug "waiting for worker processes.."
      until @pipes_in.empty? do
        pid = Process.wait
        @pipes_in.delete pid
        @pipes_out.delete pid
      end

      debug 'writing any sequences to disk'
      @sequences.each_value { |seq| seq.save_sequence }

      debug 'stopping writer'
      @pipe_writer_write.putc 0
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

      command = pipe.getc

      case command

        when COMMAND[:next]
          length = pipe.getc
          seq_name = pipe.read(length)
          #debug 'seq_name: %s' % seq_name
          @pipes_out[pid].puts sequence(seq_name).next

        when COMMAND[:heartbeat]
          #debug 'received heartbeat from %d' % pid
          @pipes_out[pid].putc RESPONSE[:ok]

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
        #debug 'forked worker: PID %d' % pid
        @pipes_in[pid] = pipe_worker_to_master.reader!
        @pipes_out[pid] = pipe_master_to_worker.writer!
      else
        Worker.new(
          @socket,
          pipe_worker_to_master,
          pipe_master_to_worker,
          @pipe_sig_read
        ).run
        exit
      end
    end

    def traps(*args, &cmd)
      cmd = args.pop unless block_given?
      args.each { |signal| trap signal, cmd }
    end

    def signal_init
      traps :INT, :TERM do
        traps :INT, :TERM, 'DEFAULT'
        puts "PID #$$ shutting down..."
        @pipe_sig_write.putc 0
      end
    end

    def sequence(name)
      @sequences.fetch name do
        @sequences[name] = Sequement::Sequence.new(name, @dir, @writer)
      end
    end

  end

end

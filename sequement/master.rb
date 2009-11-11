require 'sequement/worker'
require 'sequement/sequence'
require 'sequement/writer'
require 'sequement/pipe'

module Sequement

  class Master

    SOCKET_TIMEOUT = 2
    SOCKET_BACKLOG = 10

    # host, port: listen for TCP connection
    # dir: directory to read/write sequences
    # concurrency: number of workers to fork
    def initialize(host, port, dir, concurrency)
      @host, @port = host, port
      @dir = dir
      @concurrency = concurrency
    end

    # Starts the server:
    # * forks writer and workers
    # * monitors IPC pipes until interrupted.
    def start

      @writer = Sequement::Writer.new.start

      @sequences = {}
      @pipes_in, @pipes_out = {}, {}
      @pipe_sig = Pipe.new

      traps :INT, :TERM do
        traps :INT, :TERM, 'DEFAULT'
        puts "PID #$$ shutting down..."
        @pipe_sig.writer.putc 0
      end

      @socket = create_listen_socket
      trap(:EXIT) { @socket.close }

      master_loop
      wait_for_workers
      shutdown_writer

    end

    #######
    private

    # Spawns workers as needed to fulfill @concurrency.
    # Uses select() to monitor IPC and signal pipes.
    def master_loop
      pipe_sig_reader = @pipe_sig.reader
      loop do
        spawn_up_to_concurrency
        pipes = @pipes_in.values + [pipe_sig_reader]
        #debug 'select() on %d read pipes' % pipes.length
        if selected = IO.select(pipes, nil, nil, SOCKET_TIMEOUT)
          break if selected.first.include? pipe_sig_reader
          selected.first.each { |pipe| read_pipe pipe }
        end
      end
    end

    def wait_for_workers
      debug "waiting for workers %s to exit.." % @pipes_in.keys.join(', ')
      until @pipes_in.empty? do
        pid = Process.wait
        [@pipes_in, @pipes_out].each { |pipe| pipe.delete pid }
      end
    end

    # Persists actual value for each known sequence.
    # Instructs writer to stop its child process, waits for it to exit.
    def shutdown_writer
      debug 'writing sequences to disk, stopping writer'
      @sequences.each_value { |seq| seq.save_sequence }
      @writer.stop
    end

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
          seq_name = pipe.read length
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
        worker_to_master = Pipe.new
        master_to_worker = Pipe.new
        if pid = fork
          #debug 'forked worker: PID %d' % pid
          @pipes_in[pid] = worker_to_master.reader!
          @pipes_out[pid] = master_to_worker.writer!
        else
          $0 = 'sequement_worker'
          Worker.new(
            @socket,
            worker_to_master.writer!,
            master_to_worker.reader!,
            @pipe_sig.reader!
          ).start
          exit
        end
      end
    end

    def sequence(name)
      @sequences.fetch name do
        @sequences[name] = Sequement::Sequence.new(name, @dir, @writer)
      end
    end

  end

end

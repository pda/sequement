require 'sequement/workerpool'
require 'sequement/sequence'
require 'sequement/writer'
require 'sequement/pipe'

module Sequement

  class Master

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

      @worker_pool = WorkerPool.new create_listen_socket
      @writer = Writer.new.start
      @sequences = {}

      traps :INT, :TERM do
        traps :INT, :TERM, 'DEFAULT'
        puts "PID #$$ shutting down..."
        @worker_pool.stop
      end

      master_loop
      shutdown_writer

    end

    #######
    private

    # Spawns workers as needed to fulfill @concurrency.
    # Uses select() to monitor IPC and signal pipes.
    def master_loop
      loop do
        @worker_pool.spawn_to CONCURRENCY
        break unless @worker_pool.select { |worker| worker_read worker }
      end
    end

    # Persists actual value for each known sequence.
     # Instructs writer to stop its child process, waits for it to exit.
    def shutdown_writer
      @sequences.each_value { |seq| seq.save_sequence }
      @writer.stop
    end

    def worker_read(worker)
      pipe = worker.pipe_from_child
      command = pipe.getc
      case command
        when COMMAND[:next]
          length = pipe.getc
          seq_name = pipe.read length
          worker.pipe_to_child.puts sequence(seq_name).next
        when COMMAND[:heartbeat]
          worker.pipe_to_child.putc RESPONSE[:ok]
        else
          raise "Unrecognized command from pipe: %d" % command
      end
    end

    def create_listen_socket
      socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
      socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, true)
      socket.bind(Socket.pack_sockaddr_in(@port, @host))
      socket.listen(SOCKET_BACKLOG)
      socket
    end

    def sequence(name)
      @sequences.fetch name do
        @sequences[name] = Sequement::Sequence.new(name, @dir, @writer)
      end
    end

  end

end

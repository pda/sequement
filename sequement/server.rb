require 'socket'
require 'sequement/master'
require 'sequement/sequenceserver'
require 'sequement/pipe'

module Sequement

  class Server

    CONCURRENCY = 3
    SOCKET_BACKLOG = 10

    def initialize(host, port)
      @host = host
      @port = port
    end

    def run

      socket = create_listen_socket()
      trap('EXIT') { socket.close } # applies to parent & child processes

      hb_pipes = {}
      seq_pipes = {}

      # create IPC pipes, fork socket acceptors
      CONCURRENCY.times do
        hb_pipe, seq_pipe = Pipe.new, Pipe.new
        pid = fork { SequenceServer.new(socket, hb_pipe, seq_pipe).run ; exit }
        hb_pipes[pid], seq_pipes[pid] = hb_pipe, seq_pipe
      end

      trap('INT') do
        puts "Exiting..."
        #Process.waitall
        #exit
      end

      Master.new(hb_pipes, seq_pipes).start_loop # loop

      puts "Master exit, waiting for child processes.."
      Process.waitall
    end

    private

    def create_listen_socket
      puts "Binding to #{@host}:#{@port}"
      socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
      socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, true)
      socket.bind(Socket.pack_sockaddr_in(@port, @host))
      socket.listen(SOCKET_BACKLOG)
      socket
    end

  end

end
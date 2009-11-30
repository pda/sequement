require 'sequement/pipe'

module Sequement

  class WorkerPool

    SOCKET_TIMEOUT = 2

    def initialize(acceptor)
      @acceptor = acceptor
      @pipe_sig = Pipe.new
      @workers = {}
    end

    def pids
      @workers.keys
    end

    def stop
      @pipe_sig.writer.putc 0
      debug "waiting for workers %s to exit.." % pids.join(', ')
      pids.each do |pid|
        Process.waitpid pid
        @workers.delete pid
      end
    end

    def spawn(count = 1)
      count.times do
        worker = Worker.new(@acceptor, @pipe_sig).start
        @workers[worker.pid] = worker
      end
    end

    def spawn_to(concurrency)
      spawn concurrency - @workers.length
    end

    def select
      if selected = IO.select(read_pipes, nil, nil, SOCKET_TIMEOUT)
        return if includes_signal? selected
        selected.first.each do |pipe|
          worker = worker_for_pipe pipe
          if pipe.eof
            @workers.delete worker.pid
            return
          end
          yield worker
        end
      end
      true
    end

    #######
    private

    def read_pipes
      [@pipe_sig.reader] + @workers.map { |pid, worker| worker.pipe_from_child }
    end

    # TODO: this seems inefficient but haven't given it any thought yet.
    def worker_for_pipe(pipe)
      @workers.each_value do |worker|
        return worker if worker.pipe_from_child == pipe
      end
    end

    def includes_signal?(selected)
      selected.first.include? @pipe_sig.reader
    end

  end

end

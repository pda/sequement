module Sequement

  class Writer

    def start
      @pipe, @pipe_sig = Pipe.new, Pipe.new
      if @pid = fork
        initialize_parent
      else
        initialize_child
        exit
      end
      self
    end

    def write(path, data)
      @pipe.puts path
      @pipe.puts data
    end

    def stop
      @pipe_sig.putc 0
      Process.waitpid @pid
    end

    #######
    private

    def initialize_parent
        #debug 'forked writer: PID %d' % @pid
        @pipe.writer!
        @pipe_sig.writer!
    end

    def initialize_child
        traps :INT, :TERM, 'IGNORE'
        $0 = 'sequement_writer'
        @pipe.reader!
        @pipe_sig.reader!
        select_loop
        debug 'writer stopped'
    end

    def select_loop

      loop do

        selected = IO.select [@pipe, @pipe_sig]

        if selected.first.include? @pipe
          if @pipe.eof?
            #debug 'writer got EOF from master, exiting'
            exit
          end

          path = @pipe.gets.chop
          data = @pipe.gets.chop

          #debug "writing #{data} to #{path}"
          File.open(path, 'w') { |file| file.puts data }
        end

        break if selected.first.include? @pipe_sig

      end

    end

  end

end

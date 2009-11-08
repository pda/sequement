module Sequement

  class Writer

    attr_reader :pid

    def initialize(pipe_sig)
      @pipe_sig = pipe_sig
      @pipe = Pipe.new
      fork_writer
    end

    def write(path, data)
      @pipe.puts path
      @pipe.puts data
    end

    #######
    private

    def fork_writer
      if @pid = fork
        #debug 'forked writer: PID %d' % @pid
        @pipe.writer!
      else
        $0 = 'sequement_writer'
        @pipe.reader!
        signal_init
        select_loop
      end
    end

    def signal_init
      trap :INT, 'IGNORE'
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

      debug 'writer stopped'
      exit

    end

  end

end

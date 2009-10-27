module Sequement

  class Writer

    SOCKET_TIMEOUT = 1

    attr_reader :pid

    def initialize
      fork_writer
    end

    def write(path, data)
      @pipe.puts path
      @pipe.puts data
    end

    #######
    private

    def fork_writer
      @pipe = Pipe.new
      if @pid = fork
        @pipe.writer!
      else
        @pipe.reader!
        signal_init
        select_loop
      end
    end

    def signal_init
      @stop = false
      trap('INT', 'IGNORE')
      trap('HUP') do
        #debug 'writer got HUP'
        if @stop
          puts "PID #$$ forced exit"
          exit
        else
          @stop = true
        end
      end
    end

    def select_loop
      until @stop do
        if IO.select([@pipe], nil, nil, SOCKET_TIMEOUT)
          path = @pipe.gets.chop
          data = @pipe.gets.chop
          #debug "writing #{data} to #{path}"
          File.open(path, 'w') { |file| file.puts data }
        end
      end
      #debug 'writer stopped, exiting'
      exit
    end

  end

end

module Sequement

  class Sequence

    RESERVE_SIZE = 100
    RESERVE_THRESHOLD = 20

    def initialize(name, dir)
      @name = name
      @dir = dir
      load_sequence
    end

    def next
      conditionally_reserve
      @current += 1
    end

    def save_sequence
      write @current.to_s
    end

    #######
    private

    def path
      '%s/%s' % [@dir, @name]
    end

    def write(data)
      File.open(path, 'w') { |file| file.puts data }
    end

    def load_sequence
      if File.exists? path
        File.open(path, 'r') do |file|
          @current = @reserved = file.gets.chop.to_i
        end
      else
        @current = @reserved = 0
      end
    end

    def conditionally_reserve
      if @reserved - @current < RESERVE_THRESHOLD
        write @reserved += RESERVE_SIZE
      end
    end

  end

end

module Sequement

  class Master

    TIMEOUT = 1

    def initialize(hb_pipes, seq_pipes)

      @sequence = 0

      @hb_pipes = hb_pipes.each_value { |pipe| pipe.reader! }
      @seq_pipes = seq_pipes.each_value { |pipe| pipe.writer! }

    end

    def start_loop
      loop do
        result = IO.select(@hb_pipes.values, nil, nil, TIMEOUT) or redo

        begin
          result.first.each { |pipe| read_pipe pipe }
        rescue RuntimeError
          return
        end

      end
    end

    private

    def read_pipe(pipe)

      raise "eof" if pipe.eof

      line = pipe.gets

      # heartbeat
      if match = /^heartbeat (\d+)$/.match(line)
        pid = match[1].to_i
      # next
      elsif match = /^next (\d+)$/.match(line)
        pid = match[1].to_i
        @seq_pipes[pid].puts @sequence += 1
      # unrecognized
      else
        raise "Unrecognized read from pipe: %s" % line
      end # if match
    end

  end

end

module Sequement

  # A fork-friendly wrapper around IO#pipe.
  #
  #   pipe = Sequement::Pipe.new
  #
  #   if fork
  #     # select the writer, thus closing the reader
  #     pipe.writer!.puts 'IPC'
  #   else
  #     # select the reader, thus closing the writer
  #     puts pipe.reader!.gets
  #   end
  #
  class Pipe

    def initialize
      @reader, @writer = IO.pipe
    end

    def reader!
      select_role :reader
    end

    def writer!
      select_role :writer
    end

    def method_missing(method, *args)
      to_io.send(method, *args)
    end

    def to_io
      raise 'call reader! or writer! first' unless defined? @role
      @active
    end

    def inspect
      if defined? @role
        "#<%s %s %s>" % [self.class.name, @role, @active.inspect]
      else
        "#<%s reader:%s writer:%s>" % [self.class.name, @reader.inspect, @writer.inspect]
      end
    end

    private

    def select_role(role)

      raise 'Role already selected' if defined? @role

      if role == :reader
        active, inactive = @reader, @writer
      else
        active, inactive = @writer, @reader
      end

      inactive.close
      @role = role
      @reader, @writer = nil, nil
      @active = active
    end

  end

end

require 'monitor'

module SQSRun


class Worker
  def initialize(conf)
    require 'right_aws'
    @key_id = conf[:key_id]
    @secret_key = conf[:secret_key]
    @queue_name = conf[:queue]
    @visibility_timeout = conf[:timeout]
    @extend_timeout = conf[:extend_timeout]
    @kill_timeout = conf[:kill_timeout]
    @interval = conf[:interval]
    @finished = false

    @extender = VisibilityExtender.new(@visibility_timeout, @extend_timeout)
    @sqs = RightAws::SqsGen2.new(@key_id, @secret_key)
    @queue = @sqs.queue(@queue_name)

    @mutex = Mutex.new
    @cond = ConditionVariable.new
  end

  def run(run_proc)
    @run_proc = run_proc
    @extender.start
    until @finished
      msg = @queue.receive(@visibility_timeout)
      if msg
        process(msg)
      else
        cond_wait(@interval)
      end
    end
  end

  def shutdown
    @finished = true
    @extender.shutdown
    receive!
  end

  def receive!
    @mutex.synchronize {
      @cond.broadcast
    }
  end

  def finished?
    @finished
  end

  private
  if ConditionVariable.new.method(:wait).arity == 1
    require 'timeout'
    def cond_wait(sec)
      @mutex.synchronize {
        Timeout.timeout(sec) {
          @cond.wait(@mutex)
        }
      }
    rescue Timeout::Error
    end
  else
    def cond_wait(sec)
      @mutex.synchronize {
        @cond.wait(@mutex, sec)
      }
    end
  end

  def process(msg)
    puts "started id=#{msg.id}"
    thread = Thread.new(msg.to_s, &@run_proc.method(:call))

    @extender.set_message(msg)

    success = false
    begin
      joined = thread.join(@kill_timeout)
      if joined
        thread.value
        success = true
        puts "finished id=#{msg.id}"
      else
        thread.kill
        puts "killed id=#{msg.id}"
      end
    rescue
      puts "failed id=#{msg.id}: #{$!}"
      $!.backtrace.each {|bt|
        puts "  #{bt}"
      }
    end

    @extender.reset_message

    if success
      msg.delete
    else
      msg.visibility = 0
    end
  end

  class VisibilityExtender
    include MonitorMixin

    def initialize(visibility_timeout, extend_timeout)
      super()
      @visibility_timeout = visibility_timeout
      @extend_timeout = extend_timeout
      @extend_time = nil
      @message = nil
      @finished = false
    end

    def start
      @thread = Thread.new(&method(:run))
    end

    def join
      @thread.join
    end

    def set_message(msg)
      synchronize do
        @extend_time = Time.now.to_i + @extend_timeout
        @message = msg
      end
    end

    def reset_message
      synchronize do
        @message = nil
      end
    end

    def shutdown
      @finished = true
    end

    private
    def run
      until @finished
        sleep 1
        synchronize do
          try_extend(@message) if @message
        end
      end
    end

    def try_extend(msg)
      now = Time.now.to_i
      if now > @extend_time
        ntime = msg.visibility + @visibility_timeout
        puts "extending timeout=#{ntime} id=#{msg.id}"
        msg.visibility = ntime
        @extend_time = now + @extend_timeout
      end
    end
  end
end


class ExecRunner
  def initialize(cmd)
    @cmd = cmd + ' ' + ARGV.map {|a| Shellwords.escape(a) }.join(' ')
    @iobuf = ''
  end

  def call(message)
    IO.popen(@cmd, "r+") {|io|
      io.write(message) rescue nil
      io.close_write
      begin
        while true
          io.sysread(1024, @iobuf)
          print @iobuf
        end
      rescue EOFError
      end
    }
    if $?.to_i != 0
      raise "Command failed"
    end
  end
end


def self.worker=(worker)
  @worker = worker
end

def self.receive!
  @worker.receive!
end

def self.finished?
  @worker.finished?
end


end


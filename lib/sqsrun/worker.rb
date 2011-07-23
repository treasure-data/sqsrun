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
    @kill_retry = conf[:kill_retry]
    @interval = conf[:interval]
    @release_on_fail = conf[:release_on_fail]
    @env = conf[:env] || {}
    @finished = false

    @extender = TimerThread.new(@visibility_timeout, @extend_timeout, @kill_timeout, @kill_retry)
    @sqs = RightAws::SqsGen2.new(@key_id, @secret_key)
    @queue = @sqs.queue(@queue_name)

    @mutex = Mutex.new
    @cond = ConditionVariable.new
  end

  def init_proc(run_proc, kill_proc)
    @run_proc = run_proc
    @extender.init_proc(kill_proc)
  end

  def run
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

    @env.each_pair {|k,v|
      ENV[k] = v
    }

    @extender.set_message(msg)

    success = false
    begin
      @run_proc.call(msg.to_s)
      puts "finished id=#{msg.id}"
      success = true
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
      if @release_on_fail
        msg.visibility = 0
      end
    end
  end

  class TimerThread
    include MonitorMixin

    def initialize(visibility_timeout, extend_timeout, kill_timeout, kill_retry)
      super()
      @visibility_timeout = visibility_timeout
      @extend_timeout = extend_timeout
      @kill_timeout = kill_timeout
      @kill_retry = kill_retry
      @kill_time = nil
      @kill_proc = nil
      @extend_time = nil
      @message = nil
      @finished = false
    end

    def init_proc(kill_proc)
      @kill_proc = kill_proc
    end

    def start
      @thread = Thread.new(&method(:run))
    end

    def join
      @thread.join
    end

    def set_message(msg)
      synchronize do
        now = Time.now.to_i
        @extend_time = now + @extend_timeout
        @kill_time = now + @kill_timeout
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
          if @message
            now = Time.now.to_i
            try_kill(now, @message)
            try_extend(now, @message)
          end
        end
      end
    end

    def try_extend(now, msg)
      if now > @extend_time
        ntime = msg.visibility + @visibility_timeout
        puts "extending timeout=#{ntime} id=#{msg.id}"
        msg.visibility = ntime
        @extend_time = now + @extend_timeout
      end
    end

    def try_kill(now, msg)
      if now > @kill_time
        if @kill_proc
          puts "killing #{msg.id}..."
          @kill_proc.call rescue nil
        end
        @kill_time = now + @kill_retry
      end
    end
  end
end


class ExecRunner
  def initialize(cmd)
    @cmd = cmd + ' ' + ARGV.map {|a| Shellwords.escape(a) }.join(' ')
    @iobuf = ''
    @pid = nil
    @next_kill = :TERM
  end

  def call(message)
    IO.popen(@cmd, "r+") {|io|
      @pid = io.pid
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

  def terminate
    Process.kill(@next_kill, @pid)
    @next_kill = :KILL
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


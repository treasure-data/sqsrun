require 'right_aws'
require 'monitor'

module SQSRun


class Worker
  def initialize(conf)
    @key_id = conf[:key_id]
    @secret_key = conf[:secret_key]
    @queue_name = conf[:queue_name]
    @visibility_timeout = conf[:visibility_timeout]
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
    p "receiving"
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
      $!.backtrace.each {|bk|
        puts "  #{bk}"
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
      @finished = false
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


class Controller
  def initialize(conf)
    @key_id = conf[:key_id]
    @secret_key = conf[:secret_key]
    @queue_name = conf[:queue_name]
    @visibility_timeout = conf[:visibility_timeout]
  end

  def push(body)
    @sqs = RightAws::SqsGen2.new(@key_id, @secret_key)
    @queue = @sqs.queue(@queue_name, true, @visibility_timeout)
    @queue.send_message(body)
  end

  def list
    @sqs = RightAws::SqsGen2.new(@key_id, @secret_key)
    @sqs.queues.map {|q| q.name }
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


require 'optparse'

op = OptionParser.new

op.banner += " [-- <ARGV-for-exec-or-run>]"

type = nil
message = nil
command = nil
script = nil

conf = {
  :key_id => nil,
  :secret_key => nil,
  :queue_name => nil,
  :visibility_timeout => 30,
  :extend_timeout => nil,
  :kill_timeout => nil,
  :interval => 1,
  :daemon => nil,
}

op.on('-k', '--key-id ID', 'AWS Access Key ID') {|s|
  conf[:key_id] = s
}

op.on('-s', '--secret-key KEY', 'AWS Secret Access Key') {|s|
  conf[:secret_key] = s
}

op.on('-q', '--queue NAME', 'SQS queue name') {|s|
  conf[:queue_name] = s
}

op.on('-t', '--timeout SEC', 'SQS visibility timeout (default: 30)', Integer) {|i|
  conf[:visibility_timeout] = i
}

op.on('--push MESSAGE', 'Push maessage to the queue') {|s|
  type = :push
  message = s
}

op.on('--list', 'List queues') {|s|
  type = :list
}

op.on('--exec COMMAND', 'Execute command') {|s|
  type = :exec
  command = s
}

op.on('--run SCRIPT.rb', 'Run method named \'run\' defined in the script') {|s|
  type = :run
  script = s
}

op.on('-e', '--extend-timeout SEC', 'Threashold time before extending visibility timeout (default: timeout * 3/4)', Integer) {|i|
  conf[:extend_timeout] = i
}

op.on('-x', '--kill-timeout SEC', 'Threashold time before killing process (default: timeout * 5)', Integer) {|i|
  conf[:kill_timeout] = i
}

op.on('-i', '--interval SEC', 'Polling interval (default: 1)', Integer) {|i|
  conf[:interval] = i
}

op.on('-p', '--daemon PIDFILE', 'Daemonize (default: foreground)') {|s|
  conf[:daemon] = s
}


(class<<self;self;end).module_eval do
  define_method(:usage) do |msg|
    puts op.to_s
    puts "error: #{msg}" if msg
    exit 1
  end
end

begin
  if eqeq = ARGV.index('--')
    argv = ARGV.slice!(0, eqeq)
    ARGV.slice!(0)
  else
    argv = ARGV.slice!(0..-1)
  end
  op.parse!(argv)

  if argv.length != 0
    usage nil
  end

  unless type
    raise "--push, --list, --exec or --run is required"
  end

  unless conf[:key_id]
    raise "-k, --key-id ID option is required"
  end

  unless conf[:secret_key]
    raise "-s, --secret-key KEY option is required"
  end

  unless conf[:extend_timeout]
    conf[:extend_timeout] = conf[:visibility_timeout] / 4 * 3
  end

  unless conf[:kill_timeout]
    conf[:kill_timeout] = conf[:visibility_timeout] * 5
  end

  if !conf[:queue_name] && (type == :push || type == :exec || type == :run)
    raise "-q, --queue NAME option is required"
  end

rescue
  usage $!.to_s
end


case type
when :push
  pro = SQSRun::Controller.new(conf)
  pro.push(message)

when :list
  pro = SQSRun::Controller.new(conf)
  pro.list.each {|name|
    puts name
  }

when :exec, :run
  if conf[:daemon]
    exit!(0) if fork
    Process.setsid
    exit!(0) if fork
    File.umask(0)
    STDIN.reopen("/dev/null")
    STDOUT.reopen("/dev/null", "w")
    STDERR.reopen("/dev/null", "w")
    File.open(conf[:daemon], "w") {|f|
      f.write Process.pid.to_s
    }
  end

  worker = SQSRun::Worker.new(conf)
  SQSRun.worker = worker

  trap :INT do
    puts "shutting down..."
    worker.shutdown
  end

  trap :TERM do
    puts "shutting down..."
    worker.shutdown
  end

  if type == :run
    load File.expand_path(script)
    run_proc = method(:run)
  else
    run_proc = SQSRun::ExecRunner.new(command)
  end

  worker.run(run_proc)
end


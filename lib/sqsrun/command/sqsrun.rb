require 'sqsrun/worker'
require 'sqsrun/controller'
require 'optparse'

op = OptionParser.new

op.banner += " [-- <ARGV-for-exec-or-run>]"

type = nil
message = nil
confout = nil

defaults = {
  :timeout => 30,
  :interval => 1,
}

conf = { }

op.on('-k', '--key-id ID', 'AWS Access Key ID') {|s|
  conf[:key_id] = s
}

op.on('-s', '--secret-key KEY', 'AWS Secret Access Key') {|s|
  conf[:secret_key] = s
}

op.on('-q', '--queue NAME', 'SQS queue name') {|s|
  conf[:queue] = s
}

op.on('-t', '--timeout SEC', 'SQS visibility timeout (default: 30)', Integer) {|i|
  conf[:timeout] = i
}

op.on('--push MESSAGE', 'Push maessage to the queue') {|s|
  type = :push
  message = s
}

op.on('--list', 'List queues') {|s|
  type = :list
}

op.on('--configure PATH.yaml', 'Write configuration file') {|s|
  type = :conf
  confout = s
}

op.on('--exec COMMAND', 'Execute command') {|s|
  type = :exec
  conf[:exec] = s
}

op.on('--run SCRIPT.rb', 'Run method named \'run\' defined in the script') {|s|
  type = :run
  conf[:run] = s
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

op.on('-d', '--daemon PIDFILE', 'Daemonize (default: foreground)') {|s|
  conf[:daemon] = s
}

op.on('-f', '--file PATH.yaml', 'Read configuration file') {|s|
  conf[:file] = s
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

  if conf[:file]
    require 'yaml'
    yaml = YAML.load File.read(conf[:file])
    y = {}
    yaml.each_pair {|k,v| y[k.to_sym] = v }

    conf = defaults.merge(y).merge(conf)

    if ARGV.empty? && conf[:args]
      ARGV.clear
      ARGV.concat conf[:args]
    end
  else
    conf = defaults.merge(conf)
  end

  unless type
    if conf[:run]
      type = :run
    elsif conf[:exec]
      type = :exec
    else
      raise "--push, --list, --configure, --exec or --run is required"
    end
  end

  unless conf[:key_id]
    raise "-k, --key-id ID option is required"
  end

  unless conf[:secret_key]
    raise "-s, --secret-key KEY option is required"
  end

  unless conf[:extend_timeout]
    conf[:extend_timeout] = conf[:timeout] / 4 * 3
  end

  unless conf[:kill_timeout]
    conf[:kill_timeout] = conf[:timeout] * 5
  end

  if !conf[:queue] && (type == :push || type == :exec || type == :run)
    raise "-q, --queue NAME option is required"
  end

rescue
  usage $!.to_s
end


if confout
  require 'yaml'

  conf.delete(:file)
  conf[:args] = ARGV

  y = {}
  conf.each_pair {|k,v| y[k.to_s] = v }

  File.open(confout, "w") {|f|
    f.write y.to_yaml
  }
  exit 0
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
    load File.expand_path(conf[:run])
    run_proc = method(:run)
  else
    run_proc = SQSRun::ExecRunner.new(conf[:exec])
  end

  worker.run(run_proc)
end


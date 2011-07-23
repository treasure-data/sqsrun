require 'sqsrun/worker'
require 'sqsrun/controller'
require 'optparse'

op = OptionParser.new

op.banner += " [-- <ARGV-for-exec-or-run>]"

type = nil
message = nil
attrkv = nil
confout = nil

defaults = {
  :timeout => 30,
  :interval => 1,
  :kill_retry => 60,
  :release_on_fail => false,
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

op.on('--create', 'Create new queue') {|s|
  type = :create
}

op.on('--delete', 'Delete a queue') {|s|
  type = :delete
}

op.on('--attr Key=Value', 'Set attribute') {|s|
  type = :attr
  k, v = s.split('=',2)
  attrkv = [k.to_s, v.to_s]
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

op.on('--env K=V', 'Set environment variable') {|s|
  k, v = s.split('=',2)
  (conf[:env] ||= {})[k] = v
}

op.on('-e', '--extend-timeout SEC', 'Threashold time before extending visibility timeout (default: timeout * 3/4)', Integer) {|i|
  conf[:extend_timeout] = i
}

op.on('-x', '--kill-timeout SEC', 'Threashold time before killing process (default: timeout * 10)', Integer) {|i|
  conf[:kill_timeout] = i
}

op.on('-X', '--kill-retry SEC', 'Threashold time before retrying killing process (default: 60)', Integer) {|i|
  conf[:kill_retry] = i
}

op.on('-i', '--interval SEC', 'Polling interval (default: 1)', Integer) {|i|
  conf[:interval] = i
}

op.on('-U', '--release-on-fail', 'Releases lock if task failed so that other node can retry immediately', TrueClass) {|b|
  conf[:release_on_fail] = b
}

op.on('-d', '--daemon PIDFILE', 'Daemonize (default: foreground)') {|s|
  conf[:daemon] = s
}

op.on('-f', '--file PATH.yaml', 'Read configuration file') {|s|
  (conf[:files] ||= []) << s
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

  if conf[:files]
    require 'yaml'
    docs = ''
    conf[:files].each {|file|
      docs << File.read(file)
    }
    y = {}
    YAML.load_documents(docs) {|yaml|
      yaml.each_pair {|k,v| y[k.to_sym] = v }
    }

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
      raise "--list, --create, --delete, --attr, --push, --configure, --exec or --run is required"
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
    conf[:kill_timeout] = conf[:timeout] * 10
  end

  if !conf[:queue] && (type != :list && type != :conf)
    raise "-q, --queue NAME option is required"
  end

rescue
  usage $!.to_s
end


if confout
  require 'yaml'

  conf.delete(:files)
  conf[:args] = ARGV

  y = {}
  conf.each_pair {|k,v| y[k.to_s] = v }

  File.open(confout, "w") {|f|
    f.write y.to_yaml
  }
  exit 0
end


case type
when :create
  con = SQSRun::Controller.new(conf)
  if con.create
    puts "Queue #{conf[:queue].to_s.dump} is created."
    puts "Note that it takes some seconds before the list is updated."
  else
    puts "queue #{conf[:queue].to_s.dump} already exists."
  end

when :delete
  con = SQSRun::Controller.new(conf)
  if con.delete(true)
    puts "Queue #{conf[:queue].to_s.dump} is deleted."
    puts "Note that it takes some seconds before the list is updated."
  else
    puts "Queue #{conf[:queue].to_s.dump} does not exist."
  end

when :attr
  con = SQSRun::Controller.new(conf)
  k, v = *attrkv
  if con.set_attribute(k, v)
    puts "#{k}=#{v.to_s.dump}"
  else
    puts "Queue #{conf[:queue].to_s.dump} does not exist."
  end

when :push
  con = SQSRun::Controller.new(conf)
  con.push(message)

when :list
  require 'json'
  format = "%10s  %5s   %s"
  con = SQSRun::Controller.new(conf)
  list = con.list
  puts format % ['name', 'size', 'attributes']
  list.each {|queue|
    begin
      name = queue.name
      size = queue.size
      queue.get_attribute.each_pair {|k,v|
        puts format % [name, size, "#{k}=#{v.to_s.dump}"]
        name = ''
        size = ''
      }
    rescue RightAws::AwsError
    end
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
    if defined? terminate
      kill_proc = method(:terminate)
    else
      kill_proc = Proc.new { }
    end
  else
    run_proc = SQSRun::ExecRunner.new(conf[:exec])
    kill_proc = run_proc.method(:terminate)
  end

  worker.init_proc(run_proc, kill_proc)

  worker.run
end


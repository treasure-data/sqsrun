require 'rake'
require 'rake/testtask'
require 'rake/clean'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gemspec|
    gemspec.name = "sqsrun"
    gemspec.summary = "Generic Worker Executor Service for Amazon SQS"
    gemspec.author = "Sadayuki Furuhashi"
    gemspec.email = "frsyuki@gmail.com"
    #gemspec.homepage = "http://.../"
    gemspec.has_rdoc = false
    gemspec.require_paths = ["lib"]
    gemspec.add_dependency "right_aws", "~> 2.1.0"
    gemspec.test_files = Dir["test/**/*.rb", "test/**/*.sh"]
    gemspec.files = Dir["bin/**/*", "lib/**/*", "test/**/*.rb"]
    gemspec.executables = ['sqsrun']
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler not available. Install it with: gem install jeweler"
end

Rake::TestTask.new(:test) do |t|
  t.test_files = Dir['test/*_test.rb']
  t.ruby_opts = ['-rubygems'] if defined? Gem
  t.ruby_opts << '-I.'
end

VERSION_FILE = "lib/sqsrun/version.rb"

file VERSION_FILE => ["VERSION"] do |t|
  version = File.read("VERSION").strip
  File.open(VERSION_FILE, "w") {|f|
    f.write <<EOF
module SQSRun

VERSION = '#{version}'

end
EOF
  }
end

task :default => [VERSION_FILE, :build]


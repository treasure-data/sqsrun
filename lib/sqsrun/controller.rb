
module SQSRun


class Controller
  def initialize(conf)
    require 'right_aws'
    @key_id = conf[:key_id]
    @secret_key = conf[:secret_key]
    @queue_name = conf[:queue]
    @visibility_timeout = conf[:visibility_timeout]
  end

  def push(body)
    @sqs = RightAws::SqsGen2.new(@key_id, @secret_key)
    @queue = @sqs.queue(@queue_name, true, @visibility_timeout)
    @queue.send_message(body)
  end

  def list
    @sqs = RightAws::SqsGen2.new(@key_id, @secret_key)
    @sqs.queues.map {|q| q }
  end
end


end


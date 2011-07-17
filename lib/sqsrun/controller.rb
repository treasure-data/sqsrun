
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

  def create
    @sqs = RightAws::SqsGen2.new(@key_id, @secret_key)
    @queue = @sqs.queue(@queue_name, false)
    if @queue
      return nil
    end
    @queue = @sqs.queue(@queue_name, true)
  end

  def delete(force=false)
    @sqs = RightAws::SqsGen2.new(@key_id, @secret_key)
    @queue = @sqs.queue(@queue_name, false)
    unless @queue
      return nil
    end
    @queue.delete(force)
  end

  def list
    @sqs = RightAws::SqsGen2.new(@key_id, @secret_key)
    @sqs.queues
  end

  def set_attribute(k, v)
    @sqs = RightAws::SqsGen2.new(@key_id, @secret_key)
    @queue = @sqs.queue(@queue_name, false)
    unless @queue
      return nil
    end
    @queue.set_attribute(k, v)
  end
end


end


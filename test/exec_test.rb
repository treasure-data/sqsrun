require File.dirname(__FILE__)+'/test_helper'

class ExecTest < Test::Unit::TestCase
  it 'success' do
    success_sh  = File.expand_path File.dirname(__FILE__)+"/success.sh"
    e = SQSRun::ExecRunner.new(success_sh)

    message = 'me ssa ge'

    assert_nothing_raised do
      e.call(message)
    end
  end

  it 'fail' do
    fail_sh  = File.expand_path File.dirname(__FILE__)+"/fail.sh"
    e = SQSRun::ExecRunner.new(fail_sh)

    message = 'me ssa ge'

    assert_raise(RuntimeError) do
      e.call(message)
    end
  end

  it 'stdin' do
    cat_sh  = File.expand_path File.dirname(__FILE__)+"/cat.sh"
    out_tmp = File.expand_path File.dirname(__FILE__)+"/cat.sh.tmp"
    e = SQSRun::ExecRunner.new("#{cat_sh} #{out_tmp}")

    message = 'me ssa ge'

    e.call(message)

    assert_equal message, File.read(out_tmp)
  end

  it 'huge' do
    huge_sh  = File.expand_path File.dirname(__FILE__)+"/huge.sh"
    e = SQSRun::ExecRunner.new("#{huge_sh}")

    message = 'me ssa ge'

    e.call(message)

    # should finish
  end
end


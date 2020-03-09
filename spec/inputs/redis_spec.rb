require "logstash/devutils/rspec/spec_helper"
require "logstash/devutils/rspec/shared_examples"
require "redis"
require "stud/try"
require 'logstash/inputs/redis'
require 'securerandom'

def populate(key, event_count)
  require "logstash/event"
  redis = Redis.new(:host => "localhost")
  event_count.times do |value|
    event = LogStash::Event.new("sequence" => value)
    Stud.try(10.times) do
      redis.rpush(key, event.to_json)
    end
  end
end

def process(conf, event_count)
  events = input(conf) do |pipeline, queue|
    event_count.times.map{queue.pop}
  end

  expect(events.map{|evt| evt.get("sequence")}).to eq((0..event_count.pred).to_a)
end

# integration tests ---------------------

describe "inputs/redis", :redis => true do

  it "should read events from a list" do
    key = SecureRandom.hex
    event_count = 1000 + rand(50)
    # event_count = 100
    conf = <<-CONFIG
      input {
        redis {
          type => "blah"
          key => "#{key}"
          data_type => "list"
          batch_count => 1
        }
      }
    CONFIG

    populate(key, event_count)
    process(conf, event_count)
  end

  it "should read events from a list using batch_count (default 125)" do
    key = SecureRandom.hex
    event_count = 1000 + rand(50)
    conf = <<-CONFIG
      input {
        redis {
          type => "blah"
          key => "#{key}"
          data_type => "list"
        }
      }
    CONFIG

    populate(key, event_count)
    process(conf, event_count)
  end
end

# unit tests ---------------------

describe LogStash::Inputs::Redis do
  let(:redis) { double('redis') }
  let(:builder) { ->{ redis } }
  let(:connection) { double('redis_connection') }
  let(:connected) { [true] }
  let(:data_type) { 'list' }
  let(:batch_count) { 1 }
  let(:cfg) { {'key' => 'foo', 'data_type' => data_type, 'batch_count' => batch_count} }
  let(:quit_calls) { [:quit] }
  let(:accumulator) { [] }
  let(:command_map) { {} }

  subject do
    LogStash::Plugin.lookup("input", "redis")
      .new(cfg).add_external_redis_builder(builder)
  end

  context 'construction' do
    it 'registers the input' do
      expect {subject.register}.not_to raise_error
    end
  end

  context 'renamed redis commands' do
    let(:cfg) {
      {'key' => 'foo',
      'data_type' => data_type,
      'command_map' =>
        {
        'blpop' => 'testblpop',
        'evalsha' => 'testevalsha',
        'lrange' => 'testlrange',
        'ltrim' => 'testltrim',
        'script' => 'testscript',
        'subscribe' => 'testsubscribe',
        'psubscribe' => 'testpsubscribe',
        },
        'batch_count' => 2
      }
    }

    before do
      subject.register
      allow(redis).to receive(:connected?)
      allow(redis).to receive(:client).and_return(connection)
      allow(connection).to receive(:command_map).and_return(command_map)
    end

    it 'sets the renamed commands in the command map' do
      allow(redis).to receive(:script)
      allow(redis).to receive(:evalsha).and_return([])

      tt = Thread.new do
        sleep 0.01
        subject.do_stop
      end

      subject.run(accumulator)
      tt.join

      expect(command_map[:blpop]).to eq cfg['command_map']['blpop'].to_sym
      expect(command_map[:evalsha]).to eq cfg['command_map']['evalsha'].to_sym
      expect(command_map[:lrange]).to eq cfg['command_map']['lrange'].to_sym
      expect(command_map[:ltrim]).to eq cfg['command_map']['ltrim'].to_sym
      expect(command_map[:script]).to eq cfg['command_map']['script'].to_sym
      expect(command_map[:subscribe]).to eq cfg['command_map']['subscribe'].to_sym
      expect(command_map[:psubscribe]).to eq cfg['command_map']['psubscribe'].to_sym
    end

    it 'loads the batch script with the renamed command' do
      capture = nil
      allow(redis).to receive(:script) { |load, lua_script| capture = lua_script }
      allow(redis).to receive(:evalsha).and_return([])

      tt = Thread.new do
        sleep 0.01
        subject.do_stop
      end

      subject.run(accumulator)
      tt.join

      expect(capture).to include "redis.call('#{cfg['command_map']['lrange']}', KEYS[1], 0, batchsize)"
      expect(capture).to include "redis.call('#{cfg['command_map']['ltrim']}', KEYS[1], batchsize + 1, -1)"
    end
  end


  context 'runtime for list data_type' do
    before do
      subject.register
    end

    context 'close when redis is unset' do
      let(:quit_calls) { [:quit, :unsubscribe, :punsubscribe, :connection, :disconnect!] }

      it 'does not attempt to quit' do
        allow(redis).to receive(:nil?).and_return(true)
        quit_calls.each do |call|
          expect(redis).not_to receive(call)
        end
        expect {subject.do_stop}.not_to raise_error
      end
    end

    it 'calling the run method, adds events to the queue' do
      expect(redis).to receive(:blpop).at_least(:once).and_return(['foo', 'l1'])

      allow(redis).to receive(:connected?).and_return(connected.last)
      allow(redis).to receive(:quit)

      tt = Thread.new do
        sleep 0.01
        subject.do_stop
      end

      subject.run(accumulator)

      tt.join

      expect(accumulator.size).to be > 0
    end

    context "when the batch size is greater than 1" do
      let(:batch_count) { 10 }
      let(:rates) { [] }

      before do
        allow(redis).to receive(:connected?).and_return(connected.last)
        allow(redis).to receive(:script)
        allow(redis).to receive(:quit)
      end

      it 'calling the run method, adds events to the queue' do
        expect(redis).to receive(:evalsha).at_least(:once).and_return(['a', 'b'])

        tt = Thread.new do
          sleep 0.01
          subject.do_stop
        end

        subject.run(accumulator)

        tt.join
        expect(accumulator.size).to be > 0
      end
    end

    context "when there is no data" do
      let(:batch_count) { 10 }
      let(:rates) { [] }

      it 'will throttle the loop' do
        allow(redis).to receive(:evalsha) do
          rates.unshift Time.now.to_f
          []
        end
        allow(redis).to receive(:connected?).and_return(connected.last)
        allow(redis).to receive(:script)
        allow(redis).to receive(:quit)

        tt = Thread.new do
          sleep 1
          subject.do_stop
        end

        subject.run(accumulator)

        tt.join

        inters = []
        rates.each_cons(2) do |x, y|
          inters << x - y
        end

        expect(accumulator.size).to eq(0)
        inters.each do |delta|
          expect(delta).to be_within(0.01).of(LogStash::Inputs::Redis::BATCH_EMPTY_SLEEP)
        end
      end
    end

    it 'multiple close calls, calls to redis once' do
      subject.use_redis(redis)
      allow(redis).to receive(:blpop).and_return(['foo', 'l1'])
      expect(redis).to receive(:connected?).and_return(connected.last)
      quit_calls.each do |call|
        expect(redis).to receive(call).at_most(:once)
      end

      subject.do_stop
      connected.push(false) #can't use let block here so push to array
      expect {subject.do_stop}.not_to raise_error
      subject.do_stop
    end
  end

  context 'for the subscribe data_types' do
    def run_it_thread(inst)
      Thread.new(inst) do |subj|
        subj.run(accumulator)
      end
    end

    def publish_thread(new_redis, prefix)
      Thread.new(new_redis, prefix) do |r, p|
        sleep 0.1
        2.times do |i|
          r.publish('foo', "#{p}#{i.next}")
        end
      end
    end

    def close_thread(inst, rt)
      Thread.new(inst, rt) do |subj, runner|
        # block for the messages
        e1 = accumulator.pop
        e2 = accumulator.pop
        # put em back for the tests
        accumulator.push(e1)
        accumulator.push(e2)
        runner.raise(LogStash::ShutdownSignal)
        subj.close
      end
    end

    let(:accumulator) { Queue.new }

    let(:instance) do
      inst = described_class.new(cfg)
      inst.register
      inst
    end

    before(:example, type: :mocked) do
      subject.register
      subject.use_redis(redis)
      allow(connection).to receive(:is_a?).and_return(true)
      allow(redis).to receive(:client).and_return(connection)
      expect(redis).to receive(:connected?).and_return(connected.last)
      allow(connection).to receive(:unsubscribe)
      allow(connection).to receive(:punsubscribe)

      quit_calls.each do |call|
        expect(redis).to receive(call).at_most(:once)
      end
    end

    context 'runtime for channel data_type' do
      let(:data_type) { 'channel' }
      let(:quit_calls) { [:unsubscribe, :connection] }

      context 'mocked redis' do
        it 'multiple stop calls, calls to redis once', type: :mocked do
          subject.do_stop
          connected.push(false) #can't use let block here so push to array
          expect {subject.do_stop}.not_to raise_error
          subject.do_stop
        end
      end

      context 'real redis', :redis => true do
        it 'calling the run method, adds events to the queue' do
          #simulate the input thread
          rt = run_it_thread(instance)
          #simulate the other system thread
          publish_thread(instance.new_redis_instance, 'c').join
          #simulate the pipeline thread
          close_thread(instance, rt).join

          expect(accumulator.size).to eq(2)
        end
        it 'events had redis_channel' do
          #simulate the input thread
          rt = run_it_thread(instance)
          #simulate the other system thread
          publish_thread(instance.new_redis_instance, 'c').join
          #simulate the pipeline thread
          close_thread(instance, rt).join
          e1 = accumulator.pop
          e2 = accumulator.pop
          expect(e1.get('[@metadata][redis_channel]')).to eq('foo')
          expect(e2.get('[@metadata][redis_channel]')).to eq('foo')
        end
      end
    end

    context 'runtime for pattern_channel data_type' do
      let(:data_type)  { 'pattern_channel' }
      let(:quit_calls) { [:punsubscribe, :connection] }

      context 'mocked redis' do
        it 'multiple stop calls, calls to redis once', type: :mocked do
          subject.do_stop
          connected.push(false) #can't use let block here so push to array
          expect {subject.do_stop}.not_to raise_error
          subject.do_stop
        end
      end

      context 'real redis', :redis => true do
        it 'calling the run method, adds events to the queue' do
          #simulate the input thread
          rt = run_it_thread(instance)
          #simulate the other system thread
          publish_thread(instance.new_redis_instance, 'pc').join
          #simulate the pipeline thread
          close_thread(instance, rt).join

          expect(accumulator.size).to eq(2)
        end
        it 'events had redis_channel' do
          #simulate the input thread
          rt = run_it_thread(instance)
          #simulate the other system thread
          publish_thread(instance.new_redis_instance, 'pc').join
          #simulate the pipeline thread
          close_thread(instance, rt).join
          e1 = accumulator.pop
          e2 = accumulator.pop
          expect(e1.get('[@metadata][redis_channel]')).to eq('foo')
          expect(e2.get('[@metadata][redis_channel]')).to eq('foo')
        end
      end
    end
  end

  describe LogStash::Inputs::Redis do
    context "when using data type" do
      ["list", "channel", "pattern_channel"].each do |data_type|
        context data_type do
          it_behaves_like "an interruptible input plugin" do
            let(:config) { {'key' => 'foo', 'data_type' => data_type } }
          end
        end
      end
    end
  end
end

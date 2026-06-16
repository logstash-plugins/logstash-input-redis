require "logstash/devutils/rspec/spec_helper"
require "logstash/devutils/rspec/shared_examples"
require 'logstash/inputs/redis'
require 'securerandom'

STANDALONE_REDIS_PARAMS = { host: '127.0.0.1', port: 6379 }.freeze

SENTINEL_REDIS_PARAMS = {
  name: 'mymaster',
  sentinels: [
    { host: '127.0.0.1', port: 26379 },
    { host: '127.0.0.1', port: 26380 },
    { host: '127.0.0.1', port: 26381 }
  ],
  role: :master
}.freeze

CLUSTER_REDIS_PARAMS = {
  nodes: %w[redis://127.0.0.1:7003 redis://127.0.0.1:7004 redis://127.0.0.1:7005]
}.freeze

SENTINEL_EXTRA_CONFIG = <<~CONF.strip
  sentinel_hosts => ["127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"]
CONF

CLUSTER_EXTRA_CONFIG = <<~CONF.strip
  cluster_hosts => ["redis://127.0.0.1:7003", "redis://127.0.0.1:7004", "redis://127.0.0.1:7005"]
CONF

def populate(key, event_count, redis_params = STANDALONE_REDIS_PARAMS)
  require "logstash/event"
  require "redis"
  require "redis-clustering"
  require "stud/try"
  redis = redis_params.key?(:nodes) ? ::Redis::Cluster.new(redis_params) : ::Redis.new(redis_params)
  event_count.times do |value|
    event = LogStash::Event.new("sequence" => value)
    Stud.try(10.times) do
      redis.rpush(key, event.to_json)
    end
  end
end

def process(conf, event_count)
  events = input(conf) do |_, queue|
    sleep 0.1 until queue.size >= event_count
    queue.size.times.map { queue.pop }
  end
  # due multiple workers we get events out-of-order in the output
  events.sort! { |a, b| a.get('sequence') <=> b.get('sequence') }
  expect(events[0].get('sequence')).to eq(0)
  expect(events[100].get('sequence')).to eq(100)
  expect(events[1000].get('sequence')).to eq(1000)
end

# integration tests ---------------------

shared_examples "redis list integration" do |redis_params, extra_config|
  it "should read events from a list" do
    key = SecureRandom.hex
    event_count = 1001 + rand(50)
    conf = <<-CONFIG
      input {
        redis {
          type => "blah"
          key => "#{key}"
          data_type => "list"
          batch_count => 1
          #{extra_config}
        }
      }
    CONFIG
    populate(key, event_count, redis_params)
    process(conf, event_count)
  end

  it "should read events from a list using batch_count (default 125)" do
    key = SecureRandom.hex
    event_count = 1001 + rand(50)
    conf = <<-CONFIG
      input {
        redis {
          type => "blah"
          key => "#{key}"
          data_type => "list"
          #{extra_config}
        }
      }
    CONFIG
    populate(key, event_count, redis_params)
    process(conf, event_count)
  end
end

describe "inputs/redis standalone", :redis => true do
  include_examples "redis list integration", STANDALONE_REDIS_PARAMS, ""
end

describe "inputs/redis sentinel", :redis => true do
  include_examples "redis list integration", SENTINEL_REDIS_PARAMS, SENTINEL_EXTRA_CONFIG
end

describe "inputs/redis cluster", :redis => true do
  include_examples "redis list integration", CLUSTER_REDIS_PARAMS, CLUSTER_EXTRA_CONFIG
end

describe LogStash::Inputs::Redis do
  let(:queue) { Queue.new }

  let(:data_type) { 'list' }
  let(:batch_count) { 1 }
  let(:config) { {'key' => 'foo', 'data_type' => data_type, 'batch_count' => batch_count} }
  let(:quit_calls) { [:quit] }

  subject do
    LogStash::Inputs::Redis.new(config)
  end

  context 'construction' do
    it 'registers the input' do
      expect { subject.register }.not_to raise_error
    end
  end

  context 'renamed redis commands' do
    let(:config) do
      {
          'key' => 'foo',
          'data_type' => data_type,
          'command_map' => {
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
    end

    it 'registers and connects without error' do
      allow_any_instance_of( Redis::Client ).to receive(:call_v) do |_, command|
        expect(command[0]).to eql :script
        expect(command[1].to_s.downcase).to eql 'load'
      end.and_return('a' * 40)

      subject.register
      expect { subject.send :connect }.not_to raise_error
    end

    it 'loads the batch script with the renamed command' do
      expect_any_instance_of( Redis::Client ).to receive(:call_v) do |_, command|
        expect(command[0]).to eql :script
        expect(command[1].to_s.downcase).to eql 'load'

        script = command[2]
        expect(script).to include "redis.call('#{config['command_map']['lrange']}', KEYS[1], 0, batchsize)"
        expect(script).to include "redis.call('#{config['command_map']['ltrim']}', KEYS[1], batchsize + 1, -1)"
      end.and_return('a' * 40)

      subject.register
      subject.send :connect
    end
  end

  context 'runtime for list data_type' do

    before do
      subject.register
      allow_any_instance_of( Redis::Client ).to receive(:connected?).and_return true
      allow_any_instance_of( Redis::Client ).to receive(:disconnect)
      allow_any_instance_of( Redis ).to receive(:quit)
    end

    after do
      subject.stop
    end

    context 'close when redis is unset' do

      it 'does not attempt to quit' do
        expect_any_instance_of( Redis::Client ).to_not receive(:call)
        expect_any_instance_of( Redis::Client ).to_not receive(:disconnect)

        expect { subject.do_stop }.not_to raise_error
      end
    end

    it 'calling the run method, adds events to the queue' do
      allow_any_instance_of( Redis::Client ).to receive(:blocking_call_v) do |_, timeout, command|
        expect(command[0]).to eql :blpop
        expect(command[1]).to eql 'foo'
        expect(command[2]).to eql 1
      end.and_return ['foo', "{\"foo1\":\"bar\"}"], nil

      tt = Thread.new do
        sleep 0.25
        subject.do_stop
      end

      subject.run(queue)

      tt.join

      expect( queue.size ).to be > 0
    end

    it 'keeps running when a connection error occurs' do
      raised = false
      allow_any_instance_of( Redis::Client ).to receive(:blocking_call_v) do |_, timeout, command|
        expect(command[0]).to eql :blpop
        unless raised
          raised = true
          raise Redis::CannotConnectError.new('test')
        end
        ['foo', "{\"after\":\"raise\"}"]
      end

      expect(subject.logger).to receive(:warn).with('Redis connection error',
                                                    hash_including(:message=>"test", :exception=>Redis::CannotConnectError)
      ).and_call_original

      tt = Thread.new do
        sleep 2.0 # allow for retry (sleep) after handle_error
        subject.do_stop
      end

      subject.run(queue)

      tt.join

      try(3) { expect( queue.size ).to be > 0 }
    end

    context 'error handling' do

      let(:config) do
        super().merge 'batch_count' => 2
      end

      it 'keeps running when a (non-Redis) io error occurs' do
        raised = false
        allow(subject).to receive(:connect).and_return redis = double('redis')
        allow(redis).to receive(:blpop).and_return nil
        expect(redis).to receive(:evalsha) do
          unless raised
            raised = true
            raise IOError.new('closed stream')
          end
          []
        end.at_least(1)
        redis
        allow(subject).to receive(:stop)

        expect(subject.logger).to receive(:error).with('Unexpected error',
                                                       hash_including(:message=>'closed stream', :exception=>IOError)
        ).and_call_original

        tt = Thread.new do
          sleep 2.0 # allow for retry (sleep) after handle_error
          subject.do_stop
        end

        subject.run(queue)

        tt.join
      end

    end

    context "when the batch size is greater than 1" do
      let(:batch_count) { 10 }

      it 'calling the run method, adds events to the queue' do
        allow_any_instance_of( Redis ).to receive(:script).and_return('a' * 40)
        allow_any_instance_of( Redis::Client ).to receive(:call_v) do |_, command|
          expect(command[0]).to eql :evalsha
        end.and_return ['{"a": 1}', '{"b": 2}'], []

        tt = Thread.new do
          sleep 0.25
          subject.do_stop
        end

        subject.run(queue)

        tt.join

        expect( queue.size ).to be > 0
      end
    end

    context "when there is no data" do
      let(:batch_count) { 10 }
      let(:rates) { [] }

      it 'will throttle the loop' do
        allow_any_instance_of( Redis ).to receive(:script).and_return('a' * 40)
        allow_any_instance_of( Redis::Client ).to receive(:call_v) do |_, command|
          expect(command[0]).to eql :evalsha
          rates.unshift Time.now.to_f
        end.and_return []

        tt = Thread.new do
          sleep 0.25
          subject.do_stop
        end

        subject.run(queue)

        tt.join

        inters = []
        rates.each_cons(2) do |x, y|
          inters << x - y
        end

        expect( queue.size ).to eq(0)
        inters.each do |delta|
          expect(delta).to be_within(0.05).of(LogStash::Inputs::Redis::BATCH_EMPTY_SLEEP)
        end
      end
    end

    it 'multiple close calls, calls to redis once' do
      allow_any_instance_of( Redis::Client ).to receive(:connected?).and_return true, false
      # allow_any_instance_of( Redis::Client ).to receive(:disconnect)
      quit_calls.each do |call|
        allow_any_instance_of( Redis ).to receive(call).at_most(:once)
      end

      subject.do_stop
      expect { subject.do_stop }.not_to raise_error
      subject.do_stop
    end
  end

  context 'for the subscribe data_types' do

    before { subject.register }

    let(:channel_name) { 'foo' }

    let(:redis_client) { subject.send(:new_redis_instance) }

    def run_it_thread(inst)
      Thread.new(inst) do |subj|
        subj.run(queue)
      end
    end

    def publish_thread(new_redis, prefix)
      Thread.new(new_redis, prefix) do |r, p|
        sleep 0.1
        2.times do |i|
          r.publish(channel_name, { data: "#{p}#{i.next}" }.to_json)
        end
      end
    end

    def close_thread(inst, rt)
      Thread.new(inst, rt) do |subj, runner|
        # block for the messages
        e1 = queue.pop
        e2 = queue.pop
        # put em back for the tests
        queue.push(e1)
        queue.push(e2)
        runner.raise(LogStash::ShutdownSignal)
        subj.close
      end
    end

    before(:example, type: :mocked) do
      subject.register
      allow_any_instance_of( Redis::Client ).to receive(:connected?).and_return true, false
      quit_calls.each do |call|
        allow_any_instance_of( Redis ).to receive(call).at_most(:once)
      end
    end

    context 'runtime for channel data_type' do
      let(:data_type) { 'channel' }
      let(:quit_calls) { [:unsubscribe, :connection] }

      before { subject.register }

      context 'mocked redis' do
        it 'multiple stop calls, calls to redis once', type: :mocked do
          subject.do_stop
          expect { subject.do_stop }.not_to raise_error
          subject.do_stop
        end
      end

      context 'real redis', :redis => true do

        it 'calling the run method, adds events to the queue' do
          #simulate the input thread
          rt = run_it_thread(subject)
          try(10) { expect(redis_client.pubsub('channels')).to include(channel_name) }
          #simulate the other system thread
          publish_thread(subject.send(:new_redis_instance), 'c').join
          #simulate the pipeline thread
          close_thread(subject, rt).join

          expect(queue.size).to eq(2)
        end

        it 'events had redis_channel' do
          #simulate the input thread
          rt = run_it_thread(subject)
          try(10) { expect(redis_client.pubsub('channels')).to include(channel_name) }
          #simulate the other system thread
          publish_thread(subject.send(:new_redis_instance), 'c').join
          #simulate the pipeline thread
          close_thread(subject, rt).join
          e1 = queue.pop
          e2 = queue.pop
          expect(e1.get('[@metadata][redis_channel]')).to eq(channel_name)
          expect(e2.get('[@metadata][redis_channel]')).to eq(channel_name)
        end
      end
    end

    context 'runtime for pattern_channel data_type' do
      let(:data_type)  { 'pattern_channel' }
      let(:quit_calls) { [:punsubscribe, :connection] }

      context 'mocked redis' do
        it 'multiple stop calls, calls to redis once', type: :mocked do
          subject.do_stop
          expect { subject.do_stop }.not_to raise_error
          subject.do_stop
        end
      end

      context 'real redis', :redis => true do

        it 'calling the run method, adds events to the queue' do
          #simulate the input thread
          rt = run_it_thread(subject)
          try(10) { expect(redis_client.pubsub('numpat') > 0) }
          #simulate the other system thread
          publish_thread(subject.send(:new_redis_instance), 'pc').join
          #simulate the pipeline thread
          close_thread(subject, rt).join

          expect(queue.size).to eq(2)
        end

        it 'events had redis_channel' do
          #simulate the input thread
          rt = run_it_thread(subject)
          try(10) { expect(redis_client.pubsub('numpat') > 0) }
          #simulate the other system thread
          publish_thread(subject.send(:new_redis_instance), 'pc').join
          #simulate the pipeline thread
          close_thread(subject, rt).join
          e1 = queue.pop
          e2 = queue.pop
          expect(e1.get('[@metadata][redis_channel]')).to eq(channel_name)
          expect(e2.get('[@metadata][redis_channel]')).to eq(channel_name)
        end
      end
    end
  end

  context "when using data type" do

    ["list", "channel", "pattern_channel"].each do |data_type|
      context data_type do
        # TODO pending
        # redis-rb ends up in a read wait loop since we do not use subscribe_with_timeout
        next unless data_type == 'list'

        it_behaves_like "an interruptible input plugin", :redis => true do
          let(:config) { { 'key' => 'foo', 'data_type' => data_type } }
        end
      end
    end

  end
end

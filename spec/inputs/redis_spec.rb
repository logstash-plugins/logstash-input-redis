require "logstash/devutils/rspec/spec_helper"
require "redis"
require "stud/try"
require 'logstash/inputs/redis'

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

  events.each_with_index do |event, i|
    insist { event["sequence"] } == i
  end
end # process

# integration tests ---------------------

describe "inputs/redis", :redis => true do

  it "should read events from a list" do
    key = 10.times.collect { rand(10).to_s }.join("")
    event_count = 1000 + rand(50)
    # event_count = 100
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

  it "should read events from a list using batch_count" do
    key = 10.times.collect { rand(10).to_s }.join("")
    event_count = 1000 + rand(50)
    conf = <<-CONFIG
      input {
        redis {
          type => "blah"
          key => "#{key}"
          data_type => "list"
          batch_count => #{rand(20)+1}
        }
      }
    CONFIG

    populate(key, event_count)
    process(conf, event_count)
  end
end

# unit tests ---------------------

describe Logstash::Inputs::Redis do
  let(:redis) { double('redis') }
  let(:builder) { ->{ redis } }
  let(:connection) { double('redis_connection') }
  let(:connected) { [true] }
  let(:data_type) { 'list' }
  let(:cfg) { {'key' => 'foo', 'data_type' => data_type} }
  let(:quit_calls) { [:quit] }
  let(:accumulator) { [] }

  subject do
    described_class.new(cfg).add_external_redis_builder(builder)
  end

  context 'construction' do
    it 'registers the input' do
      expect {subject.register}.not_to raise_error
    end
  end

  context 'runtime for list data_type' do
    before do
      subject.register
    end

    context 'teardown when redis is unset' do
      let(:quit_calls) { [:quit, :unsubscribe, :punsubscribe, :connection, :disconnect!] }

      it 'does not attempt to quit' do
        allow(redis).to receive(:nil?).and_return(true)
        quit_calls.each do |call|
          expect(redis).not_to receive(call)
        end
        expect {subject.teardown}.not_to raise_error
      end
    end

    it 'calling the run method, adds events to the queue' do
      expect(redis).to receive(:blpop).at_least(:once).and_return(['foo', 'l1'])

      allow(redis).to receive(:connected?).and_return(connected.last)
      allow(redis).to receive(:quit)

      tt = Thread.new do
        sleep 0.01
        subject.teardown
      end

      subject.run(accumulator)

      tt.join

      expect(accumulator.size).to be > 0
    end

    it 'multiple teardown calls, calls to redis once' do
      subject.use_redis(redis)
      allow(redis).to receive(:blpop).and_return(['foo', 'l1'])
      expect(redis).to receive(:connected?).and_return(connected.last)
      quit_calls.each do |call|
        expect(redis).to receive(call).at_most(:once)
      end

      subject.teardown
      connected.push(false) #can't use let block here so push to array
      expect {subject.teardown}.not_to raise_error
      subject.teardown
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

    def teardown_thread(inst, rt)
      Thread.new(inst, rt) do |subj, runner|
        sleep 0.4 # allow the messages through
        runner.raise(LogStash::ShutdownSignal)
        subj.teardown
      end
    end

    let(:instance) do
      inst = described_class.new(cfg)
      inst.register
      inst
    end

    before do
      subject.register
      subject.use_redis(redis)
      allow(connection).to receive(:is_a?).and_return(true)
      allow(redis).to receive(:client).and_return(connection)
    end

    before(:example, type: :mocked) do
      expect(redis).to receive(:connected?).and_return(connected.last)
      expect(connection).to receive(:unsubscribe)

      quit_calls.each do |call|
        expect(redis).to receive(call).at_most(:once)
      end
    end

    context 'runtime for channel data_type' do
      let(:data_type) { 'channel' }
      let(:quit_calls) { [:unsubscribe, :connection] }

      context 'mocked redis' do
        it 'multiple teardown calls, calls to redis once', type: :mocked do
          subject.teardown
          connected.push(false) #can't use let block here so push to array
          expect {subject.teardown}.not_to raise_error
          subject.teardown
        end
      end

      context 'real redis' do
        it 'calling the run method, adds events to the queue' do
          #simulate the input thread
          rt = run_it_thread(instance)
          #simulate the other system thread
          publish_thread(instance.new_redis_instance, 'c').join
          #simulate the pipeline thread
          teardown_thread(instance, rt).join

          expect(accumulator.size).to eq(2)
        end
      end
    end

    context 'runtime for pattern_channel data_type' do
      let(:data_type)  { 'pattern_channel' }
      let(:quit_calls) { [:punsubscribe, :connection] }

      context 'mocked redis' do
        it 'multiple teardown calls, calls to redis once', type: :mocked do
          subject.teardown
          connected.push(false) #can't use let block here so push to array
          expect {subject.teardown}.not_to raise_error
          subject.teardown
        end
      end

      context 'real redis' do
        it 'calling the run method, adds events to the queue' do
          #simulate the input thread
          rt = run_it_thread(instance)
          #simulate the other system thread
          publish_thread(instance.new_redis_instance, 'pc').join
          #simulate the pipeline thread
          teardown_thread(instance, rt).join

          expect(accumulator.size).to eq(2)
        end
      end
    end
  end

end

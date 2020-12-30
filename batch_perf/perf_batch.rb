require "benchmark"
require "redis"
require "securerandom"

require "logstash/event"
require "logstash/java_pipeline"
require_relative "../lib/logstash/inputs/redis"

class BenchOptions
  attr_reader :output_width, :redis
  attr_reader :key, :event_count, :sizes

  def initialize
    @output_width = 70
    @redis = Redis.new(:host => "localhost")
    @event_count = 5000
    @key = SecureRandom.hex
    @sizes = [1, 10, 100, 125, 250, 1000, 1, 10, 100, 125, 250, 1000]
  end

  def cfg_batch(d)
    <<-CONFIG
      input {
        redis {
          type => "blah"
          key => "#{key}"
          data_type => "list"
          batch_count => #{d}
        }
      }
    CONFIG
  end
end

bench_options = BenchOptions.new

def input(cfg, slow, &block)
  pipeline = LogStash::JavaPipeline.new(cfg)
  queue = Queue.new

  pipeline.instance_eval do
    # create closure to capture queue
    @output_func = lambda do |event|
      (0...slow).to_a.reduce(&:+) if slow > 0
      queue << event
    end

    # output_func is now a method, call closure
    def output_func(event)
      @output_func.call(event)
    end
  end

  pipeline_thread = Thread.new { pipeline.run }
  sleep 0.1 while !pipeline.ready?

  result = block.call(pipeline, queue)

  pipeline.shutdown
  pipeline_thread.join

  result
end

def setup(bo, multiplier)
  temp = []
  (bo.event_count * multiplier).times do |value|
    temp << LogStash::Event.new("sequence" => value).to_json
  end
  temp.each_cons(10) do |arr|
    bo.redis.rpush(bo.key, arr)
  end
end

def teardown(bo)
  bo.redis.del(bo.key)
end

Benchmark.bm(bench_options.output_width) do |x|
  setup(bench_options, bench_options.sizes.size + 2)
  delay = 2000
  bench_options.sizes.reverse.each do |batchs|
    input(bench_options.cfg_batch(batchs), delay) do |pipeline, queue|
      x.report("redis input batch: #{batchs}, size: #{bench_options.event_count}, slow: #{delay} delay") do
        bench_options.event_count.times.map{queue.pop}
      end
    end
  end
  teardown(bench_options)
end

# encoding: utf-8
require "logstash/namespace"
require "logstash/inputs/base"
require "logstash/inputs/threadable"

# This input will read events from a Redis instance; it supports both Redis channels and lists.
# The list command (BLPOP) used by Logstash is supported in Redis v1.3.1+, and
# the channel commands used by Logstash are found in Redis v1.3.8+.
# While you may be able to make these Redis versions work, the best performance
# and stability will be found in more recent stable versions.  Versions 2.6.0+
# are recommended.
#
# For more information about Redis, see <http://redis.io/>
#
# `batch_count` note: If you use the `batch_count` setting, you *must* use a Redis version 2.6.0 or
# newer. Anything older does not support the operations used by batching.
#
module Logstash module Inputs class Redis < LogStash::Inputs::Threadable
# class LogStash::Inputs::Redis < LogStash::Inputs::Threadable

  config_name "redis"

  default :codec, "json"

  # The `name` configuration is used for logging in case there are multiple instances.
  # This feature has no real function and will be removed in future versions.
  config :name, :validate => :string, :default => "default", :deprecated => true

  # The hostname of your Redis server.
  config :host, :validate => :string, :default => "127.0.0.1"

  # The port to connect on.
  config :port, :validate => :number, :default => 6379

  # The Redis database number.
  config :db, :validate => :number, :default => 0

  # Initial connection timeout in seconds.
  config :timeout, :validate => :number, :default => 5

  # Password to authenticate with. There is no authentication by default.
  config :password, :validate => :password

  # The name of the Redis queue (we'll use BLPOP against this).
  # TODO: remove soon.
  config :queue, :validate => :string, :deprecated => true

  # The name of a Redis list or channel.
  # TODO: change required to true
  config :key, :validate => :string, :required => false

  # Specify either list or channel.  If `redis\_type` is `list`, then we will BLPOP the
  # key.  If `redis\_type` is `channel`, then we will SUBSCRIBE to the key.
  # If `redis\_type` is `pattern_channel`, then we will PSUBSCRIBE to the key.
  # TODO: change required to true
  config :data_type, :validate => [ "list", "channel", "pattern_channel" ], :required => false

  # The number of events to return from Redis using EVAL.
  config :batch_count, :validate => :number, :default => 1

  public
  # public API

  REDIS_INPUT_POISON_MSG = '}-{ poison message from teardown }-{'.freeze

  # use to store a proc that can provide a redis instance or mock
  def add_external_redis_builder(builder) #callable
    @redis_builder = builder
    self
  end

  # use to apply an instance directly and bypass the builder
  def use_redis(instance)
    @redis = instance
    self
  end

  def new_redis_instance
    @redis_builder.call
  end

  def register
    require 'redis'
    @redis_url = "redis://#{@password}@#{@host}:#{@port}/#{@db}"

    # TODO remove after setting key and data_type to true
    if @queue
      if @key || @data_type
        raise RuntimeError.new(
          "Cannot specify queue parameter and key or data_type"
        )
      end
      @key = @queue
      @data_type = 'list'
    end

    if !@key || !@data_type
      raise RuntimeError.new(
        "Must define queue, or key and data_type parameters"
      )
    end
    # end TODO

    @redis_builder ||= method(:internal_redis_builder)

    # just switch on data_type once
    if @data_type == 'list' || @data_type == 'dummy'
      @run_method = method(:list_runner)
      @teardown_method = method(:list_teardown)
    elsif @data_type == 'channel'
      @run_method = method(:channel_runner)
      @teardown_method = method(:subscribe_teardown)
    elsif @data_type == 'pattern_channel'
      @run_method = method(:pattern_channel_runner)
      @teardown_method = method(:subscribe_teardown)
    end

    # TODO(sissel, boertje): set @identity directly when @name config option is removed.
    @identity = @name != 'default' ? @name : "#{@redis_url} #{@data_type}:#{@key}"
    @logger.info("Registering Redis", :identity => @identity)
  end # def register

  def run(output_queue)
    @run_method.call(output_queue)
  rescue LogStash::ShutdownSignal
    # ignore and quit
  end # def run

  def teardown
    @shutdown_requested = true
    @teardown_method.call
  end

  # private methods -----------------------------
  private

  def batched?
    @batch_count > 1
  end

  # private
  def is_list_type?
    @data_type == 'list'
  end

  # private
  def redis_params
    {
      :host => @host,
      :port => @port,
      :timeout => @timeout,
      :db => @db,
      :password => @password.nil? ? nil : @password.value
    }
  end

  # private
  def internal_redis_builder
    ::Redis.new(redis_params)
  end

  # private
  def connect
    redis = new_redis_instance
    load_batch_script(redis) if batched? && is_list_type?
    redis
  end # def connect

  # private
  def load_batch_script(redis)
    #A Redis Lua EVAL script to fetch a count of keys
    #in case count is bigger than current items in queue whole queue will be returned without extra nil values
    redis_script = <<EOF
          local i = tonumber(ARGV[1])
          local res = {}
          local length = redis.call('llen',KEYS[1])
          if length < i then i = length end
          while (i > 0) do
            local item = redis.call("lpop", KEYS[1])
            if (not item) then
              break
            end
            table.insert(res, item)
            i = i-1
          end
          return res
EOF
    @redis_script_sha = redis.script(:load, redis_script)
  end

  # private
  def queue_event(msg, output_queue)
    begin
      @codec.decode(msg) do |event|
        decorate(event)
        output_queue << event
      end
    rescue LogStash::ShutdownSignal => e
      # propagate up
      raise(e)
    rescue => e # parse or event creation error
      @logger.error("Failed to create event", :message => msg, :exception => e, :backtrace => e.backtrace);
    end
  end

  # private
  def shutting_down?
    @shutdown_requested
  end

  # private
  def running?
    !@shutdown_requested
  end

  # private
  def list_teardown
    return if @redis.nil? || !@redis.connected?

    @redis.quit rescue nil
    @redis = nil
  end

  # private
  def list_runner(output_queue)
    while running?
      begin
        @redis ||= connect
        list_listener(@redis, output_queue)
      rescue ::Redis::BaseError => e
        @logger.warn("Redis connection problem", :exception => e)
        # Reset the redis variable to trigger reconnect
        @redis = nil
        sleep 1
      end
    end
  end

  # private
  def list_listener(redis, output_queue)

    item = redis.blpop(@key, 0, :timeout => 1)
    return unless item # from timeout or other conditions

    # blpop returns the 'key' read from as well as the item result
    # we only care about the result (2nd item in the list).
    queue_event(item.last, output_queue)

    # If @batch_count is 1, there's no need to continue.
    return if !batched?

    begin
      redis.evalsha(@redis_script_sha, [@key], [@batch_count-1]).each do |item|
        queue_event(item, output_queue)
      end

      # Below is a commented-out implementation of 'batch fetch'
      # using pipelined LPOP calls. This in practice has been observed to
      # perform exactly the same in terms of event throughput as
      # the evalsha method. Given that the EVALSHA implementation uses
      # one call to Redis instead of N (where N == @batch_count) calls,
      # I decided to go with the 'evalsha' method of fetching N items
      # from Redis in bulk.
      #redis.pipelined do
        #error, item = redis.lpop(@key)
        #(@batch_count-1).times { redis.lpop(@key) }
      #end.each do |item|
        #queue_event(item, output_queue) if item
      #end
      # --- End commented out implementation of 'batch fetch'
    rescue ::Redis::CommandError => e
      if e.to_s =~ /NOSCRIPT/ then
        @logger.warn("Redis may have been restarted, reloading Redis batch EVAL script", :exception => e);
        load_batch_script(redis)
        retry
      else
        raise e
      end
    end
  end

  # private
  def subscribe_teardown
    return if @redis.nil? || !@redis.connected?

    # the underlying client in redis is blocked on subscribe
    # need new redis instance, send poison message, causes
    # an unsubscribe
    publish_poison_message

    # if its a SubscribedClient then:
    # a) the poison_message did not work
    # b) it does not have a disconnect method (yet)
    if @redis.client.is_a?(::Redis::Client)
      @redis.client.disconnect
    end
    @redis = nil
  end

  # private
  def publish_poison_message
    new_redis_instance.publish(@key, REDIS_INPUT_POISON_MSG)
  end

  # private
  def redis_runner
    begin
      @redis ||= connect
      yield
    rescue ::Redis::BaseError => e
      @logger.warn("Redis connection problem", :exception => e)
      # Reset the redis variable to trigger reconnect
      @redis = nil
      sleep 1
      retry
    end
  end

  # private
  def channel_runner(output_queue)
    redis_runner do
      channel_listener(output_queue)
    end
  end

  # private
  def channel_listener(output_queue)
    @redis.subscribe(@key) do |on|
      on.subscribe do |channel, count|
        @logger.info("Subscribed", :channel => channel, :count => count)
      end

      on.message do |channel, message|
        if message != REDIS_INPUT_POISON_MSG
          queue_event(message, output_queue)
        end
        if shutting_down?
          @redis.unsubscribe(@key)
        end
      end

      on.unsubscribe do |channel, count|
        @logger.info("Unsubscribed", :channel => channel, :count => count)
      end
    end
  end

  def pattern_channel_runner(output_queue)
    redis_runner do
      pattern_channel_listener(output_queue)
    end
  end

  # private
  def pattern_channel_listener(output_queue)
    @redis.psubscribe @key do |on|
      on.psubscribe do |channel, count|
        @logger.info("Subscribed", :channel => channel, :count => count)
      end

      on.pmessage do |pattern, channel, message|
        if message != REDIS_INPUT_POISON_MSG
          queue_event(message, output_queue)
        end
        if shutting_down?
          @redis.punsubscribe(@key)
        end
      end

      on.punsubscribe do |channel, count|
        @logger.info("Unsubscribed", :channel => channel, :count => count)
      end
    end
  end

# end

end end end # Redis Inputs  LogStash

# encoding: utf-8
require "logstash/namespace"
require "logstash/inputs/base"
require "logstash/inputs/threadable"
require 'redis'

# This input will read events from a Redis instance; it supports both Redis channels and lists
# with or without priority mode.
#
# The list command (BLPOP) used by Logstash is supported in Redis v1.3.1+, and
# the channel commands used by Logstash are found in Redis v1.3.8+.
# While you may be able to make these Redis versions work, the best performance
# and stability will be found in more recent stable versions.  Versions 2.6.0+
# are recommended.
#
# The priority 'list' commands (ZREMRANGEBYRANK, ZRANGE, ZREVRANGE) used by Logstash are supported
# in Redis v2.0.0+
#
# For more information about Redis, see <http://redis.io/>
#
# `batch_count` note: If you use the `batch_count` setting, you *must* use a Redis version 2.6.0 or
# newer. Anything older does not support the operations used by batching.
#
module LogStash module Inputs class Redis < LogStash::Inputs::Threadable
  BATCH_EMPTY_SLEEP = 0.25

  config_name "redis"

  default :codec, "json"

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

  # The name of a Redis list or channel.
  config :key, :validate => :string, :required => true

  # Pop high scores item first in sortedset. No effect for data_type
  config :priority_reverse, :validate => :boolean, :default => false

  # Specify either list or channel.  If `redis\_type` is `list`, then we will BLPOP
  # the key.  If `redis\_type` is `channel`, then we will SUBSCRIBE to the key.
  # If `redis\_type` is `pattern_channel`, then we will PSUBSCRIBE to the key. 
  # If `redis\_type` is `sortedset`, then we will ZRANGE/ZREVRANGE
  config :data_type, :validate => [ "list", "channel", "pattern_channel", "sortedset" ], :required => true

  # The number of events to return from Redis using EVAL.
  config :batch_count, :validate => :number, :default => 125

  public
  # public API
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
    @redis_url = "redis://#{@password}@#{@host}:#{@port}/#{@db}"

    @redis_builder ||= method(:internal_redis_builder)

    # just switch on data_type once
    if @data_type == 'list' || @data_type == 'dummy'
      @run_method = method(:list_runner)
      @stop_method = method(:list_stop)
    elsif @data_type == 'sortedset'
      @run_method = method(:sortedset_runner)
      @stop_method = method(:sortedset_stop)
    elsif @data_type == 'channel'
      @run_method = method(:channel_runner)
      @stop_method = method(:subscribe_stop)
    elsif @data_type == 'pattern_channel'
      @run_method = method(:pattern_channel_runner)
      @stop_method = method(:subscribe_stop)
    end

    #TODO voir à terme comment fusionner ces deux méthodes
    @list_method = batched? ? method(:list_batch_listener) : method(:list_single_listener)
    @sortedset_method = batched? ? method(:sortedset_batch_listener) : method(:sortedset_single_listener) 

    @identity = "#{@redis_url} #{@data_type}:#{@key}"
    @logger.info("Registering Redis", :identity => @identity)
  end # def register

  def run(output_queue)
    @run_method.call(output_queue)
  rescue LogStash::ShutdownSignal
    # ignore and quit
  end # def run

  def stop
    @stop_method.call
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
  def is_sortedset_type?
    @data_type == 'sortedset'
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
    list_load_batch_script(redis) if batched? && is_list_type?
    sortedset_load_batch_script(redis) if batched? && is_sortedset_type?
    redis
  end # def connect

  # private
  def list_load_batch_script(redis)
    #A Redis Lua EVAL script to fetch a count of keys
    redis_script = "local batchsize = tonumber(ARGV[1])\n"
    redis_script << "local result = redis.call('"
    redis_script << 'lrange'
    redis_script << "', KEYS[1], 0, batchsize)\n"
    redis_script << "redis.call('"
    redis_script << "ltrim', KEYS[1], batchsize + 1, -1"
    redis_script << ")\nreturn result\n"
    
    @redis_script_sha = redis.script(:load, redis_script)
  end

  # private
  def sortedset_load_batch_script(redis)
    #A Redis Lua EVAL script to fetch a count of keys
    redis_script = "local batchsize = tonumber(ARGV[1])\n"
    redis_script << "local zcard = tonumber(redis.call('zcard', KEYS[1]))\n"
    redis_script << "local result = redis.call('"
    if @priority_reverse then
      redis_script << 'zrevrange'
    else
      redis_script << 'zrange'
    end
    redis_script << "', KEYS[1], 0, batchsize)\n"
    redis_script << "redis.call('"

    redis_script << "zremrangebyrank', KEYS[1], "
    if @priority_reverse then
      redis_script << "zcard - batchsize - 1, zcard"
    else
      redis_script << "0, batchsize"
    end    
    redis_script << ")\nreturn result\n"
    
    @redis_script_sha = redis.script(:load, redis_script)
  end

  # private
  def queue_event(msg, output_queue)
    begin
      @codec.decode(msg) do |event|
        decorate(event)
        output_queue << event
      end
    rescue => e # parse or event creation error
      @logger.error("Failed to create event", :message => msg, :exception => e, :backtrace => e.backtrace);
    end
  end

  # private
  def list_stop
    return if @redis.nil? || !@redis.connected?

    @redis.quit rescue nil
    @redis = nil
  end

  # private
  def list_runner(output_queue)
    while !stop?
      begin
        @redis ||= connect
        @list_method.call(@redis, output_queue)
      rescue ::Redis::BaseError => e
        @logger.warn("Redis connection problem", :exception => e)
        # Reset the redis variable to trigger reconnect
        @redis = nil
        # this sleep does not need to be stoppable as its
        # in a while !stop? loop
        sleep 1
      end
    end
  end

  def list_batch_listener(redis, output_queue)
    begin
      results = redis.evalsha(@redis_script_sha, [@key], [@batch_count-1])
      results.each do |item|
        queue_event(item, output_queue)
      end

      if results.size.zero?
        sleep BATCH_EMPTY_SLEEP
      end

      # Below is a commented-out implementation of 'batch fetch'
      # using pipelined LPOP calls. This in practice has been observed to
      # perform exactly the same in terms of event throughput as
      # the evalsha method. Given that the EVALSHA implementation uses
      # one call to Redis instead of N (where N == @batch_count) calls,
      # I decided to go with the 'evalsha' method of fetching N items
      # from Redis in bulk.
      # This method doesn't use zset
      #redis.pipelined do
        #error, item = redis.lpop(@key)
        #(@batch_count-1).times { redis.lpop(@key) }
      #end.each do |item|
        #queue_event(item, output_queue) if item
      #end
      # --- End commented out implementation of 'batch fetch'
      # further to the above, the LUA script now uses lrange and trim
      # which should further improve the efficiency of the script
    rescue ::Redis::CommandError => e
      if e.to_s =~ /NOSCRIPT/ then
        @logger.warn("Redis may have been restarted, reloading Redis batch EVAL script", :exception => e);
        list_load_batch_script(redis)
        retry
      else
        raise e
      end
    end
  end

  def list_single_listener(redis, output_queue)
    item = redis.blpop(@key, 0, :timeout => 1)
    return unless item # from timeout or other conditions

    # blpop returns the 'key' read from as well as the item result
    # we only care about the result (2nd item in the list).
    queue_event(item.last, output_queue)
  end

  # private
  def sortedset_stop
    return if @redis.nil? || !@redis.connected?

    @redis.quit rescue nil
    @redis = nil
  end

  # private
  def sortedset_runner(output_queue)
    while !stop?
      begin
        @redis ||= connect
        @sortedset_method.call(@redis, output_queue)
      rescue ::Redis::BaseError => e
        @logger.warn("Redis connection problem", :exception => e)
        # Reset the redis variable to trigger reconnect
        @redis = nil
        # this sleep does not need to be stoppable as its
        # in a while !stop? loop
        sleep 1
      end
    end
  end

  def sortedset_batch_listener(redis, output_queue)
    begin
      results = redis.evalsha(@redis_script_sha, [@key], [@batch_count-1])
      results.each do |item|
        queue_event(item, output_queue)
      end

      if results.size.zero?
        sleep BATCH_EMPTY_SLEEP
      end
    rescue ::Redis::CommandError => e
      if e.to_s =~ /NOSCRIPT/ then
        @logger.warn("Redis may have been restarted, reloading Redis batch EVAL script", :exception => e);
        sortedset_load_batch_script(redis)
        retry
      else
        raise e
      end
    end
  end

  def sortedset_single_listener(redis, output_queue)
    redis.watch(@key) do
      if @priority_reverse then
        item = redis.zrevrange(@key, 0, 0, :timeout => 1)
      else
        item = redis.zrange(@key, 0, 0, :timeout => 1)
      end
      
      if item.size.zero?
        sleep BATCH_EMPTY_SLEEP
      end

      return unless item.size > 0

      redis.multi do |multi|
        redis.zrem(@key, item)
      end

      queue_event(item.first, output_queue)
    end
  end


  # private
  def subscribe_stop
    return if @redis.nil? || !@redis.connected?
    # if its a SubscribedClient then:
    # it does not have a disconnect method (yet)
    if @redis.client.is_a?(::Redis::SubscribedClient)
      if @data_type == 'pattern_channel'
        @redis.client.punsubscribe
      else
        @redis.client.unsubscribe
      end
    else
      @redis.client.disconnect
    end
    @redis = nil
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
      Stud.stoppable_sleep(1) { stop? }
      retry if !stop?
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
        queue_event(message, output_queue)
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
        queue_event(message, output_queue)
      end

      on.punsubscribe do |channel, count|
        @logger.info("Unsubscribed", :channel => channel, :count => count)
      end
    end
  end

# end

end end end # Redis Inputs  LogStash

require 'logstash/namespace'
require 'logstash/inputs/base'
require 'logstash/inputs/threadable'
require 'redis'

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
module LogStash
  module Inputs
    class Redis < LogStash::Inputs::Threadable
      BATCH_EMPTY_SLEEP = 0.25

      config_name 'redis'

      default :codec, 'json'

      # The hostname of your Redis server.
      config :host, validate: :string, default: '127.0.0.1'

      # The hostname or address of your sentinel hosts
      config :sentinel_hosts, validate: :array

      config :master, validate: :string, default: 'mymaster'

      # The port to connect on.
      config :port, validate: :number, default: 6379
      # Each sentinel port.
      config :sentinel_port, validate: :number, default: 26_379

      # SSL
      config :ssl, validate: :boolean, default: false

      # The unix socket path to connect on. Will override host and port if defined.
      # There is no unix socket path by default.
      config :path, validate: :string

      # The Redis database number.
      config :db, validate: :number, default: 0

      # Initial connection timeout in seconds.
      config :timeout, validate: :number, default: 5

      # Password to authenticate with. There is no authentication by default.
      config :password, validate: :password

      # The name of a Redis list or channel.
      config :key, validate: :string, required: true

      # Specify either list or channel.  If `data_type` is `list`, then we will BLPOP the
      # key.  If `data_type` is `channel`, then we will SUBSCRIBE to the key.
      # If `data_type` is `pattern_channel`, then we will PSUBSCRIBE to the key.
      config :data_type, validate: %w[list channel pattern_channel], required: true

      # The number of events to return from Redis using EVAL.
      config :batch_count, validate: :number, default: 125

      # Redefined Redis commands to be passed to the Redis client.
      config :command_map, validate: :hash, default: {}

      # public API
      # use to store a proc that can provide a Redis instance or mock
      def add_external_redis_builder(builder)
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
        @redis_builder ||= method(:internal_redis_builder)
        prepare_data_type
        @list_method = batched? ? method(:list_batch_listener) : method(:list_single_listener)
        @logger.info('Registering Redis', identity: "#{redis_connection_metadata} #{@data_type}:#{@key}")
      end

      def run(output_queue)
        @run_method.call(output_queue)
      rescue LogStash::ShutdownSignal
        @logger.info('Logstash shutdown signal')
      end

      def stop
        @stop_method.call
      end

      # private methods -----------------------------
      private

      def batched?
        @batch_count > 1
      end

      def list_type?
        @data_type == 'list'
      end

      def redis_params
        connection_params.merge(base_params)
      end

      def base_params
        {
          timeout: @timeout,
          password: password,
          ssl: @ssl
        }
      end

      def password
        return @password.value if @password
      end

      def connection_params
        if @path.nil? && sentinel_hosts.nil?
          single_host_connection_params
        elsif @path.nil? && sentinel_hosts
          sentinels_connection_params
        else
          path_connection_params
        end
      end

      def single_host_connection_params
        { host: @host, port: @port, db: @db }
      end

      def sentinels_connection_params
        hosts = @sentinel_hosts.map do |sentinel_host|
          host, port = sentinel_host.split(':')
          port = @sentinel_port if port.nil?
          { host: host, port: port }
        end

        { url: "redis://#{@master}/#{@db}", sentinels: hosts, role: :master }
      end

      def path_connection_params
        @logger.warn("Parameter 'path' is set, ignoring parameters: 'host' and 'port'")
        { path: @path }
      end

      def internal_redis_builder
        ::Redis.new(redis_params)
      end

      def connect
        redis = new_redis_instance
        if @command_map.any?
          client_command_map = redis.client.command_map
          @command_map.each do |name, renamed|
            client_command_map[name.to_sym] = renamed.to_sym
          end
        end

        load_batch_script(redis) if batched? && list_type?
        redis
      end

      def load_batch_script(redis)
        redis_script = <<EOF
          local batchsize = tonumber(ARGV[1])
          local result = redis.call(\'#{@command_map.fetch('lrange', 'lrange')}\', KEYS[1], 0, batchsize)
          redis.call(\'#{@command_map.fetch('ltrim', 'ltrim')}\', KEYS[1], batchsize + 1, -1)
          return result
EOF

        @redis_script_sha = redis.script(:load, redis_script)
      end

      def queue_event(msg, output_queue, channel=nil)
        @codec.decode(msg) do |event|
          decorate(event)
          event.set('[@metadata][redis_channel]', channel) if channel
          output_queue << event
        end
      rescue => error
        @logger.error('Failed to create event', message: msg, exception: error, backtrace: error.backtrace)
      end

      def list_stop
        return if @redis.nil? || !@redis.connected?

        @redis.quit
        @redis = nil
      end

      def list_runner(output_queue)
        until stop?
          begin
            @redis ||= connect
            @logger.info("############# Redis: #{@redis.inspect} ##############")
            @list_method.call(@redis, output_queue)
          rescue ::Redis::BaseError => error
            @logger.warn('Redis connection problem', exception: error)
            @redis = nil
            sleep 1
          end
        end
      end

      def list_batch_listener(redis, output_queue)
        results = redis.evalsha(@redis_script_sha, [@key], [@batch_count - 1])

        results.clone.each do |item|
          queue_event(item, output_queue)
        end

        sleep BATCH_EMPTY_SLEEP if results.size.zero?

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
        # further to the above, the LUA script now uses lrange and trim
        # which should further improve the efficiency of the script
      rescue ::Redis::CommandError => error
        raise error unless e.to_s =~ /NOSCRIPT/

        @logger.warn('Redis may have been restarted, reloading Redis batch EVAL script', exception: error)
        load_batch_script(redis)
        retry
      end

      def list_single_listener(redis, output_queue)
        item = redis.blpop(@key, 0, timeout: 1)
        return unless item # from timeout or other conditions

        # blpop returns the 'key' read from as well as the item result
        # we only care about the result (2nd item in the list).
        queue_event(item.last, output_queue)
      end

      def subscribe_stop
        return if @redis.nil? || !@redis.connected?

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

      def redis_runner
        @redis ||= connect
        yield
      rescue ::Redis::BaseError => error
        @logger.warn('Redis connection problem', exception: error)
        # Reset the redis variable to trigger reconnect
        @redis = nil
        Stud.stoppable_sleep(1) { stop? }
        retry unless stop?
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
            @logger.info('Subscribed', channel: channel, count: count)
          end

          on.message do |channel, message|
            queue_event(message, output_queue, channel)
          end

          on.unsubscribe do |channel, count|
            @logger.info('Unsubscribed', channel: channel, count: count)
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
            @logger.info('Subscribed', channel: channel, count: count)
          end

          on.pmessage do |channel, message|
            queue_event(message, output_queue, channel)
          end

          on.punsubscribe do |channel, count|
            @logger.info('Unsubscribed', channel: channel, count: count)
          end
        end
      end

      def redis_connection_metadata
        if @sentinel_hosts
          return "url: redis://#{@password}@#{@master}/#{@db} sentinels: " \
                 "#{@sentinel_hosts} data_type: #{@data_type}:#{@key}"
        end
        return "#{@password}@#{@path}/#{@db}" if @path

        "redis://#{@password}@#{@host}:#{@port}/#{@db}"
      end

      def prepare_data_type
        if @data_type == 'list' || @data_type == 'dummy'
          @run_method = method(:list_runner)
          @stop_method = method(:list_stop)
        elsif @data_type == 'channel'
          @run_method = method(:channel_runner)
          @stop_method = method(:subscribe_stop)
        elsif @data_type == 'pattern_channel'
          @run_method = method(:pattern_channel_runner)
          @stop_method = method(:subscribe_stop)
        end
      end
    end
  end
end

module LogStash
  module Inputs
    module RedisCommon

      def self.included(base)
        # The hostname of your Redis server.
        base.config :host, :validate => :string, :default => "127.0.0.1:6379"

        # The default port to connect to.
        base.config :port, :validate => :number, :default => 6379
      end

      private
      def connection_host
        host, port = @host.split(":")
        "#{host}:#{(port.nil? ? @port : port)}"
      end
    end
  end
end

require 'redis'
require 'mondrian_redis_segment_cache/created_event'
require 'mondrian_redis_segment_cache/deleted_event'

module MondrianRedisSegmentCache
  class Cache
    include Java::MondrianSpi::SegmentCache

    attr_reader :created_listener_connection, 
                :deleted_listener_connection,
                :evicted_listener_connection,
                :expired_listener_connection,
                :listeners, 
                :mondrian_redis,
                :options

    SEGMENT_HEADERS_SET_KEY = "MONDRIAN_SEGMENT_HEADERS_SET"

    ##
    # Constructor
    #
    def initialize(mondrian_redis_connection, new_options = {})
      @mondrian_redis = mondrian_redis_connection
      @created_listener_connection = ::Redis.new(client_options)
      @deleted_listener_connection = ::Redis.new(client_options)
      @evicted_listener_connection = ::Redis.new(client_options)
      @expired_listener_connection = ::Redis.new(client_options)
      @options = Marshal.load(Marshal.dump(new_options))

      reset_listeners
      register_for_redis_events
      reconcile_set_and_keys
    end

    ##
    # Public Instance Methods
    #
    def addListener(segment_cache_listener)
      listeners << segment_cache_listener
      eager_load_listener(segment_cache_listener)
    end

    # returns boolean
    # takes mondrian.spi.SegmentHeader
    def contains(segment_header)
      segment_header.description # Hazel adapter says this affects serialization
      header_base64 = segment_header_to_base64(segment_header)

      if header_base64
        return mondrian_redis.exists(header_base64)
      end

      return false
    end

    def created_event_key
      @created_event_key ||= "__keyevent@#{client_options[:db]}__:set"
    end

    def deleted_event_key
      @deleted_event_key ||= "__keyevent@#{client_options[:db]}__:del"
    end

    def evicted_event_key
      @evicted_event_key ||= "__keyevent@#{client_options[:db]}__:evicted"
    end

    def expired_event_key
      @expired_event_key ||= "__keyevent@#{client_options[:db]}__:expired"
    end

    def eager_load_listener(listener)
      mondrian_redis.sscan_each(SEGMENT_HEADERS_SET_KEY) do |segment_header_base64|
        publish_created_to_listener(segment_header_base64, listener)
      end
    end

    # returns mondrian.spi.SegmentBody
    # takes mondrian.spi.SegmentHeader
    def get(segment_header)
      segment_header.description # Hazel adapter says this affects serialization
      header_base64 = segment_header_to_base64(segment_header)

      if header_base64
        body_base64 = mondrian_redis.get(header_base64)
        return segment_body_from_base64(body_base64)
      end

      return nil
    end

    # returns ArrayList<SegmentHeader>
    def getSegmentHeaders()
      segment_headers = ::Java::JavaUtil::ArrayList.new

      mondrian_redis.sscan_each(SEGMENT_HEADERS_SET_KEY) do |segment_header_base64|
        segment_header = segment_header_from_base64(segment_header_base64)

        if segment_header
          segment_headers << segment_header
        end
      end

      return segment_headers
    end

    def publish_created_to_listener(message, listener)
      segment_header = segment_header_from_base64(message)

      if segment_header
        created_event = ::MondrianRedisSegmentCache::CreatedEvent.new(segment_header)
        listener.handle(created_event)
      end
    end

    def publish_created_to_listeners(message)
      listeners.each do |listener|
        publish_created_to_listener(message, listener)
      end
    end

    def publish_deleted_to_listener(message, listener)
      segment_header = segment_header_from_base64(message)

      if segment_header
        deleted_event = ::MondrianRedisSegmentCache::DeletedEvent.new(segment_header)
        listener.handle(deleted_event)
      end
    end

    def publish_deleted_to_listeners(message)
      listeners.each do |listener|
        publish_deleted_to_listener(message, listener)
      end

      # Each server can tell the Set to remove the key as it may be expired
      if mondrian_redis.sismember(SEGMENT_HEADERS_SET_KEY, message)
        mondrian_redis.srem(SEGMENT_HEADERS_SET_KEY, message)
      end
    end
    alias_method :publish_evicted_to_listeners, :publish_deleted_to_listeners
    alias_method :publish_expired_to_listeners, :publish_deleted_to_listeners

    def put(segment_header, segment_body)
      segment_header.description # Hazel adapter says this affects serialization
      header_base64 = segment_header_to_base64(segment_header)
      body_base64 = segment_body_to_base64(segment_body)
      mondrian_redis.sadd(SEGMENT_HEADERS_SET_KEY, header_base64)
      
      if options[:ttl]
        set_success = mondrian_redis.setex(header_base64, options[:ttl], body_base64)
      else
        set_success = mondrian_redis.set(header_base64, body_base64)
      end

      return ("#{set_success}".upcase == "OK" || set_success == true) # weird polymorphic return ?
    end

    def remove(segment_header)
      segment_header.description # Hazel adapter says this affects serialization
      header_base64 = segment_header_to_base64(segment_header)
      mondrian_redis.srem(SEGMENT_HEADERS_SET_KEY, header_base64)
      deleted_keys = mondrian_redis.del(header_base64)

      return deleted_keys >= 1
    end

    def removeListener(segment_cache_listener)
      listeners.delete(segment_cache_listener)
    end

    def reset_listeners
      @listeners = Set.new
    end

    def supportsRichIndex()
      true # this is why we are serializing the headers to base64
    end

    def tearDown()
      if options[:delete_all_on_tear_down]
        # Remove all of the headers and the set that controls them
        mondrian_redis.sscan_each(SEGMENT_HEADERS_SET_KEY) do |segment_header_base64|
          mondrian_redis.del(segment_header_base64)
        end

        mondrian_redis.del(SEGMENT_HEADERS_SET_KEY)
      end
    end

    private

    ##
    # Private Instance Methods
    #
    def client_options
      # Redis 3.0.4 does not have options where 3.1 does
      unless mondrian_redis.client.respond_to?(:options)
        class << mondrian_redis.client
          def options
            @options
          end
        end
      end

      return mondrian_redis.client.options
    end

    def reconcile_set_and_keys
      mondrian_redis.sscan_each(SEGMENT_HEADERS_SET_KEY) do |segment_header_base64|
        # Spin through Header Set and remove any keys that are not in redis at all (they may have been deleted while offline)
        unless mondrian_redis.exists(segment_header_base64)
          mondrian_redis.srem(SEGMENT_HEADERS_SET_KEY, segment_header_base64)
        end
      end
    end

    def register_for_redis_events
      return if @listeners_registered

      # Not the best multi-threaded code, but its something that "works" for now and we will
      # worry about "best" later

      Thread.new(created_listener_connection, self) do |created_redis_connection, mondrian_cache|
        created_redis_connection.subscribe(mondrian_cache.created_event_key) do |on|
          on.message do |channel, message|
            mondrian_cache.publish_created_to_listeners(message)
          end
        end
      end

      Thread.new(deleted_listener_connection, self) do |deleted_redis_connection, mondrian_cache|
        deleted_redis_connection.subscribe(mondrian_cache.deleted_event_key) do |on|
          on.message do |channel, message|
            mondrian_cache.publish_deleted_to_listeners(message)
          end
        end
      end

      Thread.new(expired_listener_connection, self) do |expired_redis_connection, mondrian_cache|
        expired_redis_connection.subscribe(mondrian_cache.expired_event_key) do |on|
          on.message do |channel, message|
            mondrian_cache.publish_expired_to_listeners(message)
          end
        end
      end

      Thread.new(evicted_listener_connection, self) do |evicted_redis_connection, mondrian_cache|
        evicted_redis_connection.subscribe(mondrian_cache.evicted_event_key) do |on|
          on.message do |channel, message|
            mondrian_cache.publish_evicted_to_listeners(message)
          end
        end
      end

      @listeners_registered = true
    end

    def segment_body_from_base64(segment_body_base64)
      return nil unless segment_body_base64
      return ::Java::MondrianSpi::SegmentBody.from_base64(segment_body_base64)
    rescue
      return nil
    end

    def segment_body_to_base64(segment_body)
      return nil unless segment_body
      return segment_body.to_base64
    end

    def segment_header_from_base64(segment_header_base64)
      return nil unless segment_header_base64
      return ::Java::MondrianSpi::SegmentHeader.from_base64(segment_header_base64)
    rescue
      return nil
    end

    def segment_header_to_base64(segment_header)
      return nil unless segment_header
      return segment_header.to_base64
    end
  end
end

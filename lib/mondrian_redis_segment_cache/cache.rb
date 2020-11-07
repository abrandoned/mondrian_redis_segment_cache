require 'redis'
require 'concurrent'
require 'jruby/synchronized'
require 'mondrian_redis_segment_cache/created_event'
require 'mondrian_redis_segment_cache/deleted_event'
require 'set'

module MondrianRedisSegmentCache

  class SynchronizedSet < ::Set
    include ::JRuby::Synchronized
  end

  class Cache
    include Java::MondrianSpi::SegmentCache

    attr_reader :listeners,
      :local_cache_set,
      :mondrian_redis,
      :options

    SEGMENT_HEADERS_SET_KEY = "MONDRIAN_SEGMENT_HEADERS_SET"

    ##
    # Constructor
    #
    def initialize(mondrian_redis_connection, new_options = {})
      @mondrian_redis = mondrian_redis_connection
      @options = Marshal.load(Marshal.dump(new_options))
      @listeners = SynchronizedSet.new
      @local_cache_set = SynchronizedSet.new

      ##
      # Having a TimerTask reconcile every 6 minutes so the local listeners are eventually consistent with
      # respect to what is in the cache and what has been done .... allows us to get rid of the event
      # subscribers in the redis API ... consider the job to have timed out after 60 seconds
      @reconcile_task = ::Concurrent::TimerTask.new(:execution_interval => 360, :timeout_interval => 60) do
        reload
      end

      @reconcile_task.execute
      reload
    end

    ##
    # Public Instance Methods
    #
    def addListener(segment_cache_listener)
      listeners << segment_cache_listener
    end

    # returns mondrian.spi.SegmentBody
    # takes mondrian.spi.SegmentHeader
    def get(segment_header)
      segment_header.getDescription # Hazel adapter says this affects serialization
      header_base64 = segment_header_to_base64(segment_header)

      if header_base64
        body_base64 = mondrian_redis.with do |connection|
          connection.get(header_base64)
        end

        return segment_body_from_base64(body_base64)
      end

      return nil
    end

    # returns ArrayList<SegmentHeader>
    def getSegmentHeaders()
      segment_headers = ::Java::JavaUtil::ArrayList.new

      mondrian_redis.with do |connection|
        connection.sscan_each(SEGMENT_HEADERS_SET_KEY) do |segment_header_base64|
          segment_header = segment_header_from_base64(segment_header_base64)
          segment_headers << segment_header if segment_header
        end
      end

      return segment_headers
    end

    def put(segment_header, segment_body)
      set_success = nil
      segment_header.getDescription # Hazel adapter says this affects serialization
      header_base64 = segment_header_to_base64(segment_header)
      body_base64 = segment_body_to_base64(segment_body)
      @local_cache_set << header_base64
      mondrian_redis.with do |connection|
        connection.sadd(SEGMENT_HEADERS_SET_KEY, header_base64)
      end

      if has_expiry?
        set_success = mondrian_redis.with do |connection|
          connection.setex(header_base64, expires_in_seconds, body_base64)
        end
      else
        set_success = mondrian_redis.with do |connection|
          connection.set(header_base64, body_base64)
        end
      end

      return ("#{set_success}".upcase == "OK" || set_success == true) # weird polymorphic return ?
    end

    def reload
      existing_hash_vals = ::Set.new

      mondrian_redis.with do |connection|
        connection.sscan_each(SEGMENT_HEADERS_SET_KEY) do |segment_header_base64|
          unless connection.exists?(segment_header_base64)
            connection.srem(SEGMENT_HEADERS_SET_KEY, segment_header_base64)
            next
          end

          existing_hash_vals << segment_header_base64.hash

          unless local_cache_set.include?(segment_header_base64)
            @local_cache_set << segment_header_base64
            publish_created_to_listeners(segment_header_base64)
          end
        end
      end

      local_cache_set.select! do |segment|
        unless existing_hash_vals.include?(segment.hash)
          publish_deleted_to_listeners(segment)
        end

        existing_hash_vals.include?(segment.hash)
      end

      true
    end

    def remove(segment_header)
      segment_header.getDescription # Hazel adapter says this affects serialization
      header_base64 = segment_header_to_base64(segment_header)
      mondrian_redis.with do |connection|
        connection.srem(SEGMENT_HEADERS_SET_KEY, header_base64)
      end

      deleted_keys = mondrian_redis.with do |connection|
        connection.del(header_base64)
      end

      return deleted_keys >= 1
    end

    def removeListener(segment_cache_listener)
      listeners.delete(segment_cache_listener)
    end

    def supportsRichIndex()
      true # this is why we are serializing the headers to base64
    end

    def tearDown()
      #no-op
    end

    private

    ##
    # Private Instance Methods
    #
    def expires_in_seconds
      return options[:ttl] if options[:ttl]

      expires_at = nil
      now = Time.now

      case
      when options[:expires_callback]
        expires_at = options[:expires_callback].call # for future configurability of expiry through registering callback
      when options[:expires_at].is_a?(::Time)
        expires_at = options[:expires_at]
      when options[:expires] == :hourly
        one_hour = now + 4200 # one hour, 10 minutes to cover the 10:59 problem where it would expire at 11
        expires_at = ::Time.new(one_hour.year, one_hour.month, one_hour.day, one_hour.hour)
      else
        expires_at = ::Time.new(now.year, now.month, now.day, options[:expires_at])
      end

      difference_from_now = expires_at.to_i - now.to_i

      until difference_from_now > 0 do
        difference_from_now = difference_from_now + 86_400 # already passed today, move to time tomorrow
      end

      return difference_from_now
    end

    def has_expiry?
      options.has_key?(:ttl) || options.has_key?(:expires_at) || options.has_key?(:expires) || options.has_key?(:expires_callback)
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
    end

    def segment_body_from_base64(segment_body_base64)
      return nil unless segment_body_base64
      return nil if segment_body_base64.empty?
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
      return nil if segment_header_base64.empty?
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

require 'redis'
require 'concurrent'
require 'mondrian_redis_segment_cache/created_event'
require 'mondrian_redis_segment_cache/deleted_event'

module MondrianRedisSegmentCache
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
      @listeners = Set.new
      @local_cache_set = Set.new

      ##
      # Having a TimerTask reconcile every 5 minutes so the local listeners are eventually consistent with
      # respect to what is in the cache and what has been done .... allows us to get rid of the event
      # subscribers in the redis API ... consider the job to have timed out after 45 seconds
      @reconcile_task = ::Concurrent::TimerTask.new(:execution_interval => 360, :timeout_interval => 45) do
        reconcile_set_and_keys
        reconcile_local_set_with_redis
      end

      reconcile_set_and_keys
      reconcile_local_set_with_redis
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

      now = Time.now

      if options[:expires_at].is_a?(::Time)
        expires_at = options[:expires_at]
      else
        expires_at = ::Time.new(now.year, now.month, now.day, options[:expires_at])
      end

      difference_from_now = now.to_i - expires_at.to_i

      until difference_from_now > 0 do
        difference_from_now = difference_from_now + 86_400 # already passed today, move to time tomorrow
      end

      return difference_from_now
    end

    def has_expiry?
      options.has_key?(:ttl) || options.has_key?(:expires_at)
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

    def reconcile_local_set_with_redis
      remote_set = Set.new

      mondrian_redis.with do |connection|
        connection.sscan_each(SEGMENT_HEADERS_SET_KEY) do |segment_header_base64|
          remote_set << segment_header_base64
        end
      end

      remote_added_keys = remote_set - local_cache_set
      remote_removed_keys = local_cache_set - remote_set
      @local_cache_set = remote_set

      remote_added_keys.each do |remote_added_key|
        publish_created_to_listeners(remote_added_key)
      end

      remote_removed_keys.each do |remote_removed_key|
        publish_deleted_to_listeners(remote_removed_key)
      end
    end

    def reconcile_set_and_keys
      headers = []
      mondrian_redis.with do |connection|
        connection.sscan_each(SEGMENT_HEADERS_SET_KEY) do |segment_header_base64|
          headers << segment_header_base64
        end
      end

      headers.each do |header|
        # Spin through Header Set and remove any keys that are not in redis at all (they may have been deleted while offline)
        next if mondrian_redis.with do |connection|
          connection.exists(header)
        end

        mondrian_redis.with do |connection|
          connection.srem(SEGMENT_HEADERS_SET_KEY, header)
        end
      end
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

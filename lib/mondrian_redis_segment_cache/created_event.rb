module MondrianRedisSegmentCache
  class CreatedEvent
    include ::Java::MondrianSpi::SegmentCache::SegmentCacheListener::SegmentCacheEvent

    def initialize(segment_header)
      @segment_header = segment_header
    end

    def getEventType()
      Java::MondrianSpi::SegmentCache::SegmentCacheListener::SegmentCacheEvent::EventType::ENTRY_CREATED
    end

    def getSource()
      @segment_header
    end

    def isLocal()
      false
    end
  end
end

module MondrianRedisSegmentCache
  class DeletedEvent
    include ::Java::MondrianSpi::SegmentCache::SegmentCacheListener::SegmentCacheEvent

    def initialize(segment_header)
      @segment_header = segment_header
    end

    def getEventType()
      Java::MondrianSpi::SegmentCache::SegmentCacheListener::SegmentCacheEvent::EventType::ENTRY_DELETED
    end

    def getSource()
      @segment_header
    end

    def isLocal()
      false
    end
  end
end

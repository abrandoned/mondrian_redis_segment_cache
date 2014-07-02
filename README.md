# MondrianRedisSegmentCache

Mondrian ships with an in memory segment cache that is great for standalone deployments of Mondrian, but doesn't
scale out with multiple nodes.  An interface is provided for extending Mondrian with a shared Segment Cache and
examples of other implementations are in the links below.

In order to use Mondrian with Redis (our preferred caching layer) and Ruby (our preferred language -- Jruby) we had
to implement the SegmentCache interface from Mondrian and use the Redis notifications api.

Mondrian's segment cache needs to be able to get/set/remove cache items and also get any updates from the caching server
as other nodes are getting/setting/removing entries.  This means that we need to use both the notifications and subscribe
api's from Redis.

http://stackoverflow.com/questions/17533594/implementing-a-mondrian-shared-segmentcache
http://mondrian.pentaho.com/api/mondrian/spi/SegmentCache.html
https://github.com/pentaho/mondrian/blob/master/src/main/mondrian/rolap/cache/MemorySegmentCache.java
https://github.com/webdetails/cdc
http://redis.io/topics/notifications

## Installation

Add this line to your application's Gemfile:

    gem 'mondrian_redis_segment_cache'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install mondrian_redis_segment_cache

## Usage

If using Rails you can put the configuration in an initializer

```ruby
require 'redis'
require 'mondrian_redis_segment_cache'

# Setup a Redis connection
MONDRIAN_REDIS_CONNECTION = Redis.new(:url => "redis://localhost:1234/2")
MONDRIAN_SEGMENT_CACHE = ::MondrianRedisSegmentCache::Cache.new(MONDRIAN_REDIS_CONNECTION)

# Register the segment cache with the Mondrian Injector
::Java::MondrianSpi::SegmentCache::SegmentCacheInjector::add_cache(MONDRIAN_SEGMENT_CACHE)
```

In Redis we use the notifications api, so you must turn it on!
It is off by default because it is a new feature and can be CPU intensive. Redis does a ton, so there is a minimum of notifications
that must be turned on for this gem to work.

`notify-keyspace-events Egex$`

This tells Redis to publish keyevent events (which means we can subscribe to things like set/del) and to publish the generic commands
(like DEL, EXPIRE) and finally String commands (like SET)

The SegmentCache uses these notifications to keep Mondrian in sync across your Mondrian instances.
It also eager loads the current cached items into the listeners when they are added to the cache.  This allows
an existing cache to be reused between deploys.

Cache expiry is handled by the options `:ttl` and `:expires_at`

If you want a static ttl (time to live) then each key that is inserted will be set to expire after the ttl completes.  This is
not always optimal for an analytics cache and you may want all keys to expire at the same time (potentially on a daily basis).

If you want all keys to expire at the same time you should use `:expires_at` in the options hash. This should be the hour that
you want all keys to expire on.  1 being 1am, 2 being 2am, 15 being 3pm and so on.


## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

# MondrianRedisSegmentCache

Mondrian ships with an in memory segment cache that is great for standalone deployments of Mondrian, but doesn't
scale out with multiple nodes.  An interface is provided for extending Mondrian with a shared Segment Cache and
examples of other implementations are in the links below.

In order to use Mondrian with Redis (our preferred caching layer) and Ruby (our preferred language -- Jruby) we had
to implement the SegmentCache interface from Mondrian and use the Redis notifications api.

http://stackoverflow.com/questions/17533594/implementing-a-mondrian-shared-segmentcache
http://mondrian.pentaho.com/api/mondrian/spi/SegmentCache.html
https://github.com/pentaho/mondrian/blob/master/src/main/mondrian/rolap/cache/MemorySegmentCache.java
https://github.com/webdetails/cdc

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

Cache expiry is handled by the options `:ttl` and `:expires_at`

If you want a static ttl (time to live) then each key that is inserted will be set to expire after the ttl completes.  This is
not always optimal for an analytics cache and you may want all keys to expire at the same time (potentially on a daily basis).

If you want all keys to expire at the same time you should use `:expires_at` in the options hash. This should either be the hour
that you want all keys to expire on, or a `Time` object with a custom time. If you use `:expires_at` as an hour of expiration then
1 is 1am, 2 is 2am, 15 is 3pm and so on.


## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

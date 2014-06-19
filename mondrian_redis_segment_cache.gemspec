# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mondrian_redis_segment_cache/version'

Gem::Specification.new do |gem|
  gem.name          = "mondrian_redis_segment_cache"
  gem.version       = MondrianRedisSegmentCache::VERSION
  gem.authors       = ["Brandon Dewitt"]
  gem.email         = ["brandonsdewitt@gmail.com"]
  gem.description   = %q{TODO: Write a gem description}
  gem.summary       = %q{TODO: Write a gem summary}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_dependency "redis"
  gem.add_dependency "java_to_base64"
  gem.add_dependency "mondrian-olap"
end

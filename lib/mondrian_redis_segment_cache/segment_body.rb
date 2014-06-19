require 'java_to_base64'

# SegmentBody is an interface and thus a module in Jruby
module Java::MondrianSpi::SegmentBody
  include JavaToBase64::InstanceMethods
  extend JavaToBase64::ClassMethods
end

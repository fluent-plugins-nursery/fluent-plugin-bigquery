require 'bundler/setup'
require 'test/unit'

$LOAD_PATH.unshift(File.join(__dir__, '..', 'lib'))
$LOAD_PATH.unshift(__dir__)
require 'fluent/test'

require 'fluent/plugin/buffer'
require 'fluent/plugin/buf_memory'
require 'fluent/plugin/buf_file'
require 'fluent/test/driver/output'

require 'fluent/plugin/out_bigquery'
require 'google/apis/bigquery_v2'
require 'google/api_client/auth/key_utils'
require 'googleauth'

require 'test/unit/rr'

class Test::Unit::TestCase
end

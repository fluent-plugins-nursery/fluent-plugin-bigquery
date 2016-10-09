require 'rubygems'
require 'bundler'
begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end
require 'test/unit'

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'fluent/test'
unless ENV.has_key?('VERBOSE')
  nulllogger = Object.new
  nulllogger.instance_eval {|obj|
    def method_missing(method, *args)
      # pass
    end
  }
  $log = nulllogger
end

require 'fluent/plugin/buffer'
require 'fluent/plugin/buf_memory'
require 'fluent/plugin/buf_file'
require 'fluent/test/driver/output'

require 'fluent/plugin/out_bigquery'
require 'google/apis/bigquery_v2'
require 'google/api_client/auth/key_utils'
require 'googleauth'

require 'rr'
require 'test/unit/rr'

class Test::Unit::TestCase
end

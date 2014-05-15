# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'fluent/plugin/bigquery/version'

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-bigquery"
  spec.version       = Fluent::BigQueryPlugin::VERSION
  spec.authors       = ["TAGOMORI Satoshi"]
  spec.email         = ["tagomoris@gmail.com"]
  spec.description   = %q{Fluentd plugin to store data on Google BigQuery, by load, or by stream inserts}
  spec.summary       = %q{Fluentd plugin to store data on Google BigQuery}
  spec.homepage      = "https://github.com/tagomoris/fluent-plugin-bigquery"
  spec.license       = "APLv2"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "rake"
  spec.add_development_dependency "rr"
  spec.add_runtime_dependency "google-api-client", "~> 0.7.1"
  spec.add_runtime_dependency "fluentd"
  spec.add_runtime_dependency "fluent-mixin-plaintextformatter", '>= 0.2.1'
  spec.add_runtime_dependency "fluent-mixin-config-placeholders", ">= 0.2.0"
  spec.add_runtime_dependency "fluent-plugin-buffer-lightening", ">= 0.0.2"

  spec.add_development_dependency "fluent-plugin-dummydata-producer"
end

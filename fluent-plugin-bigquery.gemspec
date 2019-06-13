# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'fluent/plugin/bigquery/version'

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-bigquery"
  spec.version       = Fluent::BigQueryPlugin::VERSION
  spec.authors       = ["Naoya Ito", "joker1007"]
  spec.email         = ["i.naoya@gmail.com", "kakyoin.hierophant@gmail.com"]
  spec.description   = %q{Fluentd plugin to store data on Google BigQuery, by load, or by stream inserts}
  spec.summary       = %q{Fluentd plugin to store data on Google BigQuery}
  spec.homepage      = "https://github.com/kaizenplatform/fluent-plugin-bigquery"
  spec.license       = "Apache-2.0"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "rake"
  spec.add_development_dependency "rr"
  spec.add_development_dependency "test-unit"
  spec.add_development_dependency "test-unit-rr"

  spec.add_runtime_dependency "avro"
  spec.add_runtime_dependency "google-api-client", ">= 0.11.0"
  spec.add_runtime_dependency "googleauth", ">= 0.5.0"
  spec.add_runtime_dependency "multi_json"
  spec.add_runtime_dependency "fluentd", ">= 0.14.0", "< 2"
end

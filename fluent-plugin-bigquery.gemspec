# coding: utf-8
Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-bigquery"
  spec.version       = "0.0.1"
  spec.authors       = ["TAGOMORI Satoshi"]
  spec.email         = ["tagomoris@gmail.com"]
  spec.description   = %q{TODO: Write a gem description}
  spec.summary       = %q{TODO: Write a gem summary}
  spec.homepage      = ""
  spec.license       = "APLv2"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "rake"
  gem.add_runtime_dependency "fluentd"
  gem.add_runtime_dependency "fluent-mixin-plaintextformatter", '>= 0.2.1'
  gem.add_runtime_dependency "fluent-mixin-config-placeholders", ">= 0.2.0"
  gem.add_runtime_dependency "google-api-client", "~> 0.6.4"
end

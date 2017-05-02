module Fluent
  module BigQuery
    module Helper
      class << self
        def deep_symbolize_keys(object)
          case object
          when Hash
            object.each_with_object({}) do |(key, value), result|
              result[key.to_sym] = deep_symbolize_keys(value)
            end
          when Array
            object.map {|e| deep_symbolize_keys(e) }
          else
            object
          end
        end

        def deep_stringify_keys(object)
          case object
          when Hash
            object.each_with_object({}) do |(key, value), result|
              result[key.to_s] = deep_stringify_keys(value)
            end
          when Array
            object.map {|e| deep_stringify_keys(e) }
          else
            object
          end
        end
      end
    end
  end
end

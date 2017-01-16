require 'multi_json'

module Fluent
  module BigQuery
    class FieldSchema
      def initialize(name, mode = :nullable)
        unless [:nullable, :required, :repeated].include?(mode)
          raise ConfigError, "Unrecognized mode for #{name}: #{mode}"
        end
        ### https://developers.google.com/bigquery/docs/tables
        # Each field has the following properties:
        #
        # name - The name must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_),
        #        and must start with a letter or underscore. The maximum length is 128 characters.
        #        https://cloud.google.com/bigquery/docs/reference/v2/tables#schema.fields.name
        unless name =~ /^[_A-Za-z][_A-Za-z0-9]{,127}$/
          raise ConfigError, "invalid bigquery field name: '#{name}'"
        end

        @name = name
        @mode = mode
      end

      attr_reader :name, :mode

      def format(value)
        case @mode
        when :nullable
          format_one(value) unless value.nil?
        when :required
          if value.nil?
            log.warn "Required field #{name} cannot be null"
            nil
          else
            format_one(value)
          end
        when :repeated
          value.nil? ? [] : value.each_with_object([]) { |v, arr| arr << format_one(v) if v }
        end
      end

      def format_one(value)
        raise NotImplementedError, "Must implement in a subclass"
      end

      def to_h
        {
          :name => name,
          :type => type.to_s.upcase,
          :mode => mode.to_s.upcase,
        }
      end
    end

    class StringFieldSchema < FieldSchema
      def type
        :string
      end

      def format_one(value)
        if value.is_a?(Hash) || value.is_a?(Array)
          MultiJson.dump(value)
        else
          value.to_s
        end
      end
    end

    class IntegerFieldSchema < FieldSchema
      def type
        :integer
      end

      def format_one(value)
        value.to_i
      end
    end

    class FloatFieldSchema < FieldSchema
      def type
        :float
      end

      def format_one(value)
        value.to_f
      end
    end

    class BooleanFieldSchema < FieldSchema
      def type
        :boolean
      end

      def format_one(value)
        !!value
      end
    end

    class TimestampFieldSchema < FieldSchema
      INTEGER_REGEXP = /\A-?[[:digit:]]+\z/.freeze
      FLOAT_REGEXP = /\A-?[[:digit:]]+(\.[[:digit:]]+)\z/.freeze

      def type
        :timestamp
      end

      def format_one(value)
        case value
        when Time
          value.strftime("%Y-%m-%d %H:%M:%S.%6L %:z")
        when String
          if value =~ INTEGER_REGEXP
            value.to_i
          elsif value =~ FLOAT_REGEXP
            value.to_f
          else
            value
          end
        else
          value
        end
      end
    end

    class RecordSchema < FieldSchema
      FIELD_TYPES = {
        string: StringFieldSchema,
        integer: IntegerFieldSchema,
        float: FloatFieldSchema,
        boolean: BooleanFieldSchema,
        timestamp: TimestampFieldSchema,
        record: RecordSchema
      }.freeze

      def initialize(name, mode = :nullable)
        super(name, mode)
        @fields = {}
      end

      def type
        :record
      end

      def [](name)
        @fields[name]
      end

      def empty?
        @fields.empty?
      end

      def to_a
        @fields.map do |_, field_schema|
          field_schema.to_h
        end
      end

      def to_h
        {
          :name => name,
          :type => type.to_s.upcase,
          :mode => mode.to_s.upcase,
          :fields => self.to_a,
        }
      end

      def load_schema(schema, allow_overwrite=true)
        schema.each do |field|
          raise ConfigError, 'field must have type' unless field.key?('type')

          name = field['name']
          mode = (field['mode'] || 'nullable').downcase.to_sym

          type = field['type'].downcase.to_sym
          field_schema_class = FIELD_TYPES[type]
          raise ConfigError, "Invalid field type: #{field['type']}" unless field_schema_class

          next if @fields.key?(name) and !allow_overwrite

          field_schema = field_schema_class.new(name, mode)
          @fields[name] = field_schema
          if type == :record
            raise ConfigError, "record field must have fields" unless field.key?('fields')
            field_schema.load_schema(field['fields'], allow_overwrite)
          end
        end
      end

      def register_field(name, type)
        if @fields.key?(name) and @fields[name].type != :timestamp
          raise ConfigError, "field #{name} is registered twice"
        end
        if name[/\./]
          recordname = $`
          fieldname = $'
          register_record_field(recordname)
          @fields[recordname].register_field(fieldname, type)
        else
          schema = FIELD_TYPES[type]
          raise ConfigError, "[Bug] Invalid field type #{type}" unless schema
          @fields[name] = schema.new(name)
        end
      end

      def format_one(record)
        out = {}
        record.each do |key, value|
          next if value.nil?
          schema = @fields[key]
          out[key] = schema ? schema.format(value) : value
        end
        out
      end

      private
      def register_record_field(name)
        if !@fields.key?(name)
          @fields[name] = RecordSchema.new(name)
        else
          unless @fields[name].kind_of?(RecordSchema)
            raise ConfigError, "field #{name} is required to be a record but already registered as #{@field[name]}"
          end
        end
      end
    end
  end
end

# -*- coding: utf-8 -*-

require 'fluent/plugin/bigquery/version'

require 'fluent/mixin/config_placeholders'
require 'fluent/mixin/plaintextformatter'

require 'fluent/plugin/bigquery/errors'
require 'fluent/plugin/bigquery/bigquery_client'

## TODO: load implementation
# require 'fluent/plugin/bigquery/load_request_body_wrapper'

module Fluent
  ### TODO: error classes for each api error responses
  # class BigQueryAPIError < StandardError
  # end

  class BigQueryOutput < BufferedOutput
    Fluent::Plugin.register_output('bigquery', self)

    # https://developers.google.com/bigquery/browser-tool-quickstart
    # https://developers.google.com/bigquery/bigquery-api-quickstart

    config_set_default :buffer_type, 'lightening'

    config_set_default :flush_interval, 0.25
    config_set_default :try_flush_interval, 0.05

    config_set_default :buffer_chunk_records_limit, 500
    config_set_default :buffer_chunk_limit, 1000000
    config_set_default :buffer_queue_limit, 1024

    ### for loads
    ### TODO: different default values for buffering between 'load' and insert
    # config_set_default :flush_interval, 1800 # 30min => 48 imports/day
    # config_set_default :buffer_chunk_limit, 1000**4 # 1.0*10^12 < 1TB (1024^4)

    ### OAuth credential
    # config_param :client_id, :string
    # config_param :client_secret, :string

    # Available methods are:
    # * private_key -- Use service account credential
    # * compute_engine -- Use access token available in instances of ComputeEngine
    config_param :auth_method, :string, default: 'private_key'

    ### Service Account credential
    config_param :email, :string, default: nil
    config_param :private_key_path, :string, default: nil
    config_param :private_key_passphrase, :string, default: 'notasecret'

    # see as simple reference
    #   https://github.com/abronte/BigQuery/blob/master/lib/bigquery.rb
    config_param :project, :string

    # dataset_name
    #   The name can be up to 1,024 characters long, and consist of A-Z, a-z, 0-9, and the underscore,
    #   but it cannot start with a number or underscore, or have spaces.
    config_param :dataset, :string

    # table_id
    #   In Table ID, enter a name for your new table. Naming rules are the same as for your dataset.
    config_param :table, :string, default: nil
    config_param :tables, :string, default: nil

    config_param :auto_create_table, :bool, default: false

    config_param :schema_path, :string, default: nil
    config_param :fetch_schema, :bool, default: false
    config_param :field_string,  :string, default: nil
    config_param :field_integer, :string, default: nil
    config_param :field_float,   :string, default: nil
    config_param :field_boolean, :string, default: nil
    config_param :field_timestamp, :string, default: nil
    ### TODO: record field stream inserts doesn't works well?
    ###  At table creation, table type json + field type record -> field type validation fails
    ###  At streaming inserts, schema cannot be specified
    # config_param :field_record,  :string, defualt: nil
    # config_param :optional_data_field, :string, default: nil

    config_param :time_format, :string, default: nil
    config_param :localtime, :bool, default: nil
    config_param :utc, :bool, default: nil
    config_param :time_field, :string, default: nil

    config_param :insert_id_field, :string, default: nil

    config_param :method, :string, default: 'insert' # or 'load' # TODO: not implemented now

    config_param :load_size_limit, :integer, default: 1000**4 # < 1TB (1024^4) # TODO: not implemented now
    ### method: 'load'
    #   https://developers.google.com/bigquery/loading-data-into-bigquery
    # Maximum File Sizes:
    # File Type   Compressed   Uncompressed
    # CSV         1 GB         With new-lines in strings: 4 GB
    #                          Without new-lines in strings: 1 TB
    # JSON        1 GB         1 TB

    config_param :row_size_limit, :integer, default: 100*1000 # < 100KB # configurable in google ?
    # config_param :insert_size_limit, :integer, default: 1000**2 # < 1MB
    # config_param :rows_per_second_limit, :integer, default: 1000 # spike limit
    ### method: ''Streaming data inserts support
    #  https://developers.google.com/bigquery/streaming-data-into-bigquery#usecases
    # Maximum row size: 100 KB
    # Maximum data size of all rows, per insert: 1 MB
    # Maximum rows per second: 100 rows per second, per table, with allowed and occasional bursts of up to 1,000 rows per second.
    #                          If you exceed 100 rows per second for an extended period of time, throttling might occur.
    ### Toooooooooooooo short/small per inserts and row!

    ### Table types
    # https://developers.google.com/bigquery/docs/tables
    #
    # type - The following data types are supported; see Data Formats for details on each data type:
    # STRING
    # INTEGER
    # FLOAT
    # BOOLEAN
    # RECORD A JSON object, used when importing nested records. This type is only available when using JSON source files.
    #
    # mode - Whether a field can be null. The following values are supported:
    # NULLABLE - The cell can be null.
    # REQUIRED - The cell cannot be null.
    # REPEATED - Zero or more repeated simple or nested subfields. This mode is only supported when using JSON source files.

    # Define `log` method for v0.10.42 or earlier
    unless method_defined?(:log)
      define_method("log") { $log }
    end

    def configure(conf)
      super

      case @auth_method
      when 'private_key'
        unless @email && @private_key_path
          raise Fluent::ConfigError, "'email' and 'private_key_path' must be specified if auth_method == 'private_key'"
        end
      when 'compute_engine'
        # Do nothing
      else
        raise Fluent::ConfigError, "unrecognized 'auth_method': #{@auth_method}"
      end

      unless @table.nil? ^ @tables.nil?
        raise Fluent::ConfigError, "'table' or 'tables' must be specified, and both are invalid"
      end

      @tablelist = @tables ? @tables.split(',') : [@table]

      @fields = RecordSchema.new('record')
      if @schema_path
        @fields.load_schema(JSON.parse(File.read(@schema_path)))
      end

      types = %w(string integer float boolean timestamp)
      types.each do |type|
        raw_fields = instance_variable_get("@field_#{type}")
        next unless raw_fields
        raw_fields.split(',').each do |field|
          @fields.register_field field.strip, type.to_sym
        end
      end

      @localtime = false if @localtime.nil? && @utc

      @timef = TimeFormatter.new(@time_format, @localtime)

      if @time_field
        keys = @time_field.split('.')
        last_key = keys.pop
        @add_time_field = ->(record, time) {
          keys.inject(record) { |h, k| h[k] ||= {} }[last_key] = @timef.format(time)
          record
        }
      else
        @add_time_field = ->(record, time) { record }
      end

      if @insert_id_field
        insert_id_keys = @insert_id_field.split('.')
        @get_insert_id = ->(record) {
          insert_id_keys.inject(record) {|h, k| h[k] }
        }
      else
        @get_insert_id = nil
      end
    end

    def start
      super

      @tables_queue = @tablelist.dup.shuffle
      @tables_mutex = Mutex.new

      @client =
        BigQueryPlugin::BigQueryClient.new(
          project:                @project,
          dataset:                @dataset,
          email:                  @email,
          private_key_path:       @private_key_path,
          private_key_passphrase: @private_key_passphrase,
          auth_method:            @auth_method
        )

      if @fetch_schema
        table = generate_table_id(@tablelist.first, Time.now)
        schema = @client.fetch_schema(table)
        log.debug "Load schema from BigQuery: #{@project}:#{@dataset}.#{table} #{schema}"
        @fields.load_schema(schema, false)
      end
    rescue Fluent::BigQueryPlugin::BigQueryAPIError => error
      log.error "tables.get API", project_id: @project, dataset: @dataset, table: table, error: error.class.to_s, message: error.to_s
      raise error
    end

    def generate_table_id(table_id_format, current_time)
      current_time.strftime(table_id_format)
    end

    def create_table(table)
      @client.create_table(table, @fields.to_a)
    rescue Fluent::BigQueryPlugin::Conflict => error
      return if /Already Exists:/ =~ error.to_s
      log.error "tables.insert API", project_id: @project, dataset: @dataset, table: table, error: error.class.to_s, message: error.to_s
      raise error
    rescue Fluent::BigQueryPlugin::BigQueryAPIError => error
      log.error "tables.insert API", project_id: @project, dataset: @dataset, table: table, error: error.class.to_s, message: error.to_s
      raise error
    end

    def load
      # https://developers.google.com/bigquery/loading-data-into-bigquery#loaddatapostrequest
      raise NotImplementedError # TODO
    end

    def format_stream(tag, es)
      super
      buf = ''
      es.each do |time, record|
        row = @fields.format(@add_time_field.call(record, time))
        unless row.empty?
          row = {"json" => row}
          row['insertId'] = @get_insert_id.call(record) if @get_insert_id
          buf << row.to_msgpack
        end
      end
      buf
    end

    def write(chunk)
      rows = []
      chunk.msgpack_each do |row_object|
        # TODO: row size limit
        rows << row_object
      end

      # TODO: method

      insert_table = @tables_mutex.synchronize do
        t = @tables_queue.shift
        @tables_queue.push t
        t
      end

      table = generate_table_id(insert_table, Time.now)
      @client.insert(table, rows)
    rescue Fluent::BigQueryPlugin::NotFound => error
      # Table Not Found: Auto Create Table
      create_table(table) if @auto_create_table && (/Not Found: Table/ =~ error.to_s)
      log.error "tabledata.insertAll API", project_id: @project, dataset: @dataset, table: table, error: error.class.to_s, message: error.to_s
      raise error
    rescue Fluent::BigQueryPlugin::BigQueryAPIError => error
      log.error "tabledata.insertAll API", project_id: @project, dataset: @dataset, table: table, error: error.class.to_s, message: error.to_s
      raise error
    end

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
          raise Fluent::ConfigError, "invalid bigquery field name: '#{name}'"
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
          raise "Required field #{name} cannot be null" if value.nil?
          format_one(value)
        when :repeated
          value.nil? ? [] : value.map {|v| format_one(v) }
        end
      end

      def format_one(value)
        raise NotImplementedError, "Must implement in a subclass" 
      end

      def to_h
        {
          'name' => name,
          'type' => type.to_s.upcase,
          'mode' => mode.to_s.upcase,
        }
      end
    end

    class StringFieldSchema < FieldSchema
      def type
        :string
      end

      def format_one(value)
        value.to_s
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
      def type
        :timestamp
      end

      def format_one(value)
        value
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

      def to_a
        @fields.map do |_, field_schema|
          field_schema.to_h
        end
      end

      def to_h
        {
          'name' => name,
          'type' => type.to_s.upcase,
          'mode' => mode.to_s.upcase,
          'fields' => self.to_a,
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
        @fields.each do |key, schema|
          value = record[key]
          formatted = schema.format(value)
          next if formatted.nil? # field does not exists, or null value
          out[key] = formatted
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

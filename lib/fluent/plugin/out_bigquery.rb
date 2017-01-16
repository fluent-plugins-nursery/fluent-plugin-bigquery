# -*- coding: utf-8 -*-

require 'fluent/plugin/bigquery/version'

require 'fluent/mixin/config_placeholders'
require 'fluent/mixin/plaintextformatter'

require 'fluent/plugin/bigquery/errors'
require 'fluent/plugin/bigquery/schema'
require 'fluent/plugin/bigquery/writer'

## TODO: load implementation
# require 'fluent/plugin/bigquery/load_request_body_wrapper'

module Fluent
  class BigQueryOutput < TimeSlicedOutput
    Fluent::Plugin.register_output('bigquery', self)

    # https://developers.google.com/bigquery/browser-tool-quickstart
    # https://developers.google.com/bigquery/bigquery-api-quickstart

    ### default for insert
    def configure_for_insert(conf)
      raise ConfigError unless conf["method"] != "load"

      conf["buffer_type"]                = "lightening"  unless conf["buffer_type"]
      conf["flush_interval"]             = 0.25          unless conf["flush_interval"]
      conf["try_flush_interval"]         = 0.05          unless conf["try_flush_interval"]
      conf["buffer_chunk_limit"]         = 1 * 1024 ** 2 unless conf["buffer_chunk_limit"] # 1MB
      conf["buffer_queue_limit"]         = 1024          unless conf["buffer_queue_limit"]
      conf["buffer_chunk_records_limit"] = 500           unless conf["buffer_chunk_records_limit"]
    end

    ### default for loads
    def configure_for_load(conf)
      raise ConfigError unless conf["method"] == "load"

      # buffer_type, flush_interval, try_flush_interval is TimeSlicedOutput default
      conf["buffer_chunk_limit"] = 1 * 1024 ** 3 unless conf["buffer_chunk_limit"] # 1GB
      conf["buffer_queue_limit"] = 32            unless conf["buffer_queue_limit"]
    end

    # Available methods are:
    # * private_key -- Use service account credential from pkcs12 private key file
    # * compute_engine -- Use access token available in instances of ComputeEngine
    # * json_key -- Use service account credential from JSON key
    # * application_default -- Use application default credential
    config_param :auth_method, :enum, list: [:private_key, :compute_engine, :json_key, :application_default], default: :private_key

    ### Service Account credential
    config_param :email, :string, default: nil
    config_param :private_key_path, :string, default: nil
    config_param :private_key_passphrase, :string, default: 'notasecret', secret: true
    config_param :json_key, default: nil, secret: true

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
    config_param :tables, :string, default: nil # TODO: use :array with value_type: :string

    # template_suffix (only insert)
    #   https://cloud.google.com/bigquery/streaming-data-into-bigquery#template_table_details
    config_param :template_suffix, :string, default: nil

    config_param :auto_create_table, :bool, default: false

    # skip_invalid_rows (only insert)
    #   Insert all valid rows of a request, even if invalid rows exist.
    #   The default value is false, which causes the entire request to fail if any invalid rows exist.
    config_param :skip_invalid_rows, :bool, default: false
    # max_bad_records (only load)
    #   The maximum number of bad records that BigQuery can ignore when running the job.
    #   If the number of bad records exceeds this value, an invalid error is returned in the job result.
    #   The default value is 0, which requires that all records are valid.
    config_param :max_bad_records, :integer, default: 0
    # ignore_unknown_values
    #   Accept rows that contain values that do not match the schema. The unknown values are ignored.
    #   Default is false, which treats unknown values as errors.
    config_param :ignore_unknown_values, :bool, default: false

    config_param :schema_path, :string, default: nil
    config_param :fetch_schema, :bool, default: false
    config_param :fetch_schema_table, :string, default: nil
    config_param :schema_cache_expire, :time, default: 600
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

    REGEXP_MAX_NUM = 10
    config_param :replace_record_key, :bool, default: false
    (1..REGEXP_MAX_NUM).each {|i| config_param :"replace_record_key_regexp#{i}", :string, default: nil }

    config_param :convert_hash_to_json, :bool, default: false

    config_param :time_format, :string, default: nil
    config_param :localtime, :bool, default: nil
    config_param :utc, :bool, default: nil
    config_param :time_field, :string, default: nil

    # insert_id_field (only insert)
    config_param :insert_id_field, :string, default: nil
    # prevent_duplicate_load (only load)
    config_param :prevent_duplicate_load, :bool, default: false

    config_param :method, :enum, list: [:insert, :load], default: :insert, skip_accessor: true

    # TODO
    # config_param :row_size_limit, :integer, default: 100*1000 # < 100KB # configurable in google ?
    # config_param :insert_size_limit, :integer, default: 1000**2 # < 1MB
    # config_param :rows_per_second_limit, :integer, default: 1000 # spike limit
    ### method: ''Streaming data inserts support
    #  https://developers.google.com/bigquery/streaming-data-into-bigquery#usecases
    # Maximum row size: 100 KB
    # Maximum data size of all rows, per insert: 1 MB
    # Maximum rows per second: 100 rows per second, per table, with allowed and occasional bursts of up to 1,000 rows per second.
    #                          If you exceed 100 rows per second for an extended period of time, throttling might occur.
    ### Toooooooooooooo short/small per inserts and row!

    ## Timeout
    # request_timeout_sec
    #   Bigquery API response timeout
    # request_open_timeout_sec
    #   Bigquery API connection, and request timeout
    config_param :request_timeout_sec, :time, default: nil
    config_param :request_open_timeout_sec, :time, default: 60

    ## Partitioning
    config_param :time_partitioning_type, :enum, list: [:day], default: nil
    config_param :time_partitioning_expiration, :time, default: nil

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

    def initialize
      super
      require 'multi_json'
      require 'google/apis/bigquery_v2'
      require 'googleauth'
      require 'active_support/json'
      require 'active_support/core_ext/hash'
      require 'active_support/core_ext/object/json'

      # MEMO: signet-0.6.1 depend on Farady.default_connection
      Faraday.default_connection.options.timeout = 60
    end

    def configure(conf)
      if conf["method"] == "load"
        configure_for_load(conf)
      else
        configure_for_insert(conf)
      end
      super

      case @method
      when :insert
        extend(InsertImplementation)
      when :load
        raise Fluent::ConfigError, "'template_suffix' is for only `insert` mode, instead use 'fetch_schema_table' and formatted table name" if @template_suffix
        extend(LoadImplementation)
      else
        raise Fluent::ConfigError "'method' must be 'insert' or 'load'"
      end

      case @auth_method
      when :private_key
        unless @email && @private_key_path
          raise Fluent::ConfigError, "'email' and 'private_key_path' must be specified if auth_method == 'private_key'"
        end
      when :compute_engine
        # Do nothing
      when :json_key
        unless @json_key
          raise Fluent::ConfigError, "'json_key' must be specified if auth_method == 'json_key'"
        end
      when :application_default
        # Do nothing
      else
        raise Fluent::ConfigError, "unrecognized 'auth_method': #{@auth_method}"
      end

      unless @table.nil? ^ @tables.nil?
        raise Fluent::ConfigError, "'table' or 'tables' must be specified, and both are invalid"
      end

      @tablelist = @tables ? @tables.split(',') : [@table]

      @fields = Fluent::BigQuery::RecordSchema.new('record')
      if @schema_path
        @fields.load_schema(MultiJson.load(File.read(@schema_path)))
      end

      types = %w(string integer float boolean timestamp)
      types.each do |type|
        raw_fields = instance_variable_get("@field_#{type}")
        next unless raw_fields
        raw_fields.split(',').each do |field|
          @fields.register_field field.strip, type.to_sym
        end
      end

      @regexps = {}
      (1..REGEXP_MAX_NUM).each do |i|
        next unless conf["replace_record_key_regexp#{i}"]
        regexp, replacement = conf["replace_record_key_regexp#{i}"].split(/ /, 2)
        raise ConfigError, "replace_record_key_regexp#{i} does not contain 2 parameters" unless replacement
        raise ConfigError, "replace_record_key_regexp#{i} contains a duplicated key, #{regexp}" if @regexps[regexp]
        @regexps[regexp] = replacement
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

      warn "[DEPRECATION] `convert_hash_to_json` param is deprecated. If Hash value is inserted string field, plugin convert it to json automatically." if @convert_hash_to_json
    end

    def start
      super

      @tables_queue = @tablelist.dup.shuffle
      @tables_mutex = Mutex.new
      @fetch_schema_mutex = Mutex.new

      @last_fetch_schema_time = 0
      fetch_schema(false) if @fetch_schema
    end

    def writer
      @writer ||= Fluent::BigQuery::Writer.new(@log, @auth_method, {
        private_key_path: @private_key_path, private_key_passphrase: @private_key_passphrase,
        email: @email,
        json_key: @json_key,
      })
    end

    def generate_table_id(table_id_format, current_time, row = nil, chunk = nil)
      format, col = table_id_format.split(/@/)
      time = if col && row
               keys = col.split('.')
               t = keys.inject(row[:json]) {|obj, attr| obj[attr.to_sym] }
               Time.at(t)
             else
               current_time
             end
      if row && format =~ /\$\{/
        format.gsub!(/\$\{\s*(\w+)\s*\}/) do |m|
          row[:json][$1.to_sym].to_s.gsub(/[^\w]/, '')
        end
      end
      table_id = time.strftime(format)

      if chunk
        table_id.gsub(%r(%{time_slice})) { |expr|
          chunk.key
        }
      else
        table_id.gsub(%r(%{time_slice})) { |expr|
          current_time.strftime(@time_slice_format)
        }
      end
    end

    def replace_record_key(record)
      new_record = {}
      record.each do |key, _|
        new_key = key
        @regexps.each do |regexp, replacement|
          new_key = new_key.gsub(/#{regexp}/, replacement)
        end
        new_key = new_key.gsub(/\W/, '')
        new_record.store(new_key, record[key])
      end
      new_record
    end

    def convert_hash_to_json(record)
      record.each do |key, value|
        if value.class == Hash
          record[key] = MultiJson.dump(value)
        end
      end
      record
    end

    def write(chunk)
      table_id_format = @tables_mutex.synchronize do
        t = @tables_queue.shift
        @tables_queue.push t
        t
      end
      template_suffix_format = @template_suffix
      _write(chunk, table_id_format, template_suffix_format)
    end

    def fetch_schema(allow_overwrite = true)
      table_id = nil
      @fetch_schema_mutex.synchronize do
        if Fluent::Engine.now - @last_fetch_schema_time > @schema_cache_expire
          table_id_format = @fetch_schema_table || @tablelist[0]
          table_id = generate_table_id(table_id_format, Time.at(Fluent::Engine.now))
          schema = writer.fetch_schema(@project, @dataset, table_id)

          if schema
            if allow_overwrite
              fields = Fluent::BigQuery::RecordSchema.new("record")
              fields.load_schema(schema, allow_overwrite)
              @fields = fields
            else
              @fields.load_schema(schema, allow_overwrite)
            end
          else
            if @fields.empty?
              raise "failed to fetch schema from bigquery"
            else
              log.warn "#{table_id} uses previous schema"
            end
          end

          @last_fetch_schema_time = Fluent::Engine.now
        end
      end
    end

    module InsertImplementation
      def format(tag, time, record)
        fetch_schema if @template_suffix

        if @replace_record_key
          record = replace_record_key(record)
        end

        if @convert_hash_to_json
          record = convert_hash_to_json(record)
        end

        buf = String.new
        row = @fields.format(@add_time_field.call(record, time))
        unless row.empty?
          row = {"json" => row}
          row['insert_id'] = @get_insert_id.call(record) if @get_insert_id
          buf << row.to_msgpack
        end
        buf
      end

      def _write(chunk, table_format, template_suffix_format)
        rows = []
        chunk.msgpack_each do |row_object|
          # TODO: row size limit
          rows << row_object.deep_symbolize_keys
        end

        now = Time.at(Fluent::Engine.now)
        group = rows.group_by do |row|
          [
            generate_table_id(table_format, now, row, chunk),
            template_suffix_format ? generate_table_id(template_suffix_format, now, row, chunk) : nil,
          ]
        end
        group.each do |(table_id, template_suffix), group_rows|
          insert(table_id, group_rows, template_suffix)
        end
      end

      def insert(table_id, rows, template_suffix)
        writer.insert_rows(@project, @dataset, table_id, rows, skip_invalid_rows: @skip_invalid_rows, ignore_unknown_values: @ignore_unknown_values, template_suffix: template_suffix)
      rescue Fluent::BigQuery::Error => e
        if @auto_create_table && e.status_code == 404 && /Not Found: Table/i =~ e.message
          # Table Not Found: Auto Create Table
          writer.create_table(@project, @dataset, table_id, @fields, time_partitioning_type: @time_partitioning_type, time_partitioning_expiration: @time_partitioning_expiration)
          raise "table created. send rows next time."
        end

        if e.retryable?
          raise e # TODO: error class
        elsif @secondary
          flush_secondary(@secondary)
        end
      end
    end

    module LoadImplementation
      def format(tag, time, record)
        fetch_schema if @fetch_schema_table

        if @replace_record_key
          record = replace_record_key(record)
        end

        if @convert_hash_to_json
          record = convert_hash_to_json(record)
        end

        buf = String.new
        row = @fields.format(@add_time_field.call(record, time))
        unless row.empty?
          buf << MultiJson.dump(row) + "\n"
        end
        buf
      end

      def _write(chunk, table_id_format, _)
        now = Time.at(Fluent::Engine.now)
        table_id = generate_table_id(table_id_format, now, nil, chunk)
        load(chunk, table_id)
      end

      def load(chunk, table_id)
        res = nil

        create_upload_source(chunk) do |upload_source|
          res = writer.create_load_job(chunk.unique_id, @project, @dataset, table_id, upload_source, @fields, {
            prevent_duplicate_load: @prevent_duplicate_load,
            ignore_unknown_values: @ignore_unknown_values, max_bad_records: @max_bad_records,
            timeout_sec: @request_timeout_sec,  open_timeout_sec: @request_open_timeout_sec, auto_create_table: @auto_create_table,
            time_partitioning_type: @time_partitioning_type, time_partitioning_expiration: @time_partitioning_expiration
          })
        end
      rescue Fluent::BigQuery::Error => e
        if e.retryable?
          raise e
        elsif @secondary
          flush_secondary(@secondary)
        end
      end

      private

      def create_upload_source(chunk)
        chunk_is_file = @buffer_type == 'file'
        if chunk_is_file
          File.open(chunk.path) do |file|
            yield file
          end
        else
          Tempfile.open("chunk-tmp") do |file|
            file.binmode
            chunk.write_to(file)
            file.sync
            file.rewind
            yield file
          end
        end
      end
    end
  end
end

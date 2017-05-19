# -*- coding: utf-8 -*-

require 'fluent/plugin/output'

require 'fluent/plugin/bigquery/version'

require 'fluent/plugin/bigquery/helper'
require 'fluent/plugin/bigquery/errors'
require 'fluent/plugin/bigquery/schema'
require 'fluent/plugin/bigquery/writer'

module Fluent
  module Plugin
    class BigQueryOutput < Output
      Fluent::Plugin.register_output('bigquery', self)

      helpers :inject

      # https://developers.google.com/bigquery/browser-tool-quickstart
      # https://developers.google.com/bigquery/bigquery-api-quickstart

      ### default for insert
      def configure_for_insert(conf)
        raise ConfigError unless conf["method"].nil? || conf["method"] == "insert"

        buffer_config = conf.elements("buffer")[0]
        return unless buffer_config
        buffer_config["@type"]                       = "memory"      unless buffer_config["@type"]
        buffer_config["flush_mode"]                  = :interval     unless buffer_config["flush_mode"]
        buffer_config["flush_interval"]              = 0.25          unless buffer_config["flush_interval"]
        buffer_config["flush_thread_interval"]       = 0.05          unless buffer_config["flush_thread_interval"]
        buffer_config["flush_thread_burst_interval"] = 0.05          unless buffer_config["flush_thread_burst_interval"]
        buffer_config["chunk_limit_size"]            = 1 * 1024 ** 2 unless buffer_config["chunk_limit_size"] # 1MB
        buffer_config["total_limit_size"]            = 1 * 1024 ** 3 unless buffer_config["total_limit_size"] # 1GB
        buffer_config["chunk_records_limit"]         = 500           unless buffer_config["chunk_records_limit"]
      end

      ### default for loads
      def configure_for_load(conf)
        raise ConfigError unless conf["method"] == "load"

        buffer_config = conf.elements("buffer")[0]
        return unless buffer_config
        buffer_config["@type"]                       = "file"         unless buffer_config["@type"]
        buffer_config["flush_mode"]                  = :interval      unless buffer_config["flush_mode"]
        buffer_config["chunk_limit_size"]            = 1 * 1024 ** 3  unless buffer_config["chunk_limit_size"] # 1GB
        buffer_config["total_limit_size"]            = 32 * 1024 ** 3 unless buffer_config["total_limit_size"] # 32GB
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
      config_param :tables, :array, value_type: :string, default: nil

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

      config_param :schema, :array, default: nil
      config_param :schema_path, :string, default: nil
      config_param :fetch_schema, :bool, default: false
      config_param :fetch_schema_table, :string, default: nil
      config_param :schema_cache_expire, :time, default: 600
      config_param :field_string,    :array, value_type: :string, default: nil
      config_param :field_integer,   :array, value_type: :string, default: nil
      config_param :field_float,     :array, value_type: :string, default: nil
      config_param :field_boolean,   :array, value_type: :string, default: nil
      config_param :field_timestamp, :array, value_type: :string, default: nil
      ### TODO: record field stream inserts doesn't works well?
      ###  At table creation, table type json + field type record -> field type validation fails
      ###  At streaming inserts, schema cannot be specified
      # config_param :field_record,  :string, defualt: nil
      # config_param :optional_data_field, :string, default: nil

      REGEXP_MAX_NUM = 10
      config_param :replace_record_key, :bool, default: false
      (1..REGEXP_MAX_NUM).each {|i| config_param :"replace_record_key_regexp#{i}", :string, default: nil }

      config_param :convert_hash_to_json, :bool, default: false

      # insert_id_field (only insert)
      config_param :insert_id_field, :string, default: nil
      # prevent_duplicate_load (only load)
      config_param :prevent_duplicate_load, :bool, default: false

      # add_insert_timestamp (only insert)
      # adds a timestamp just before sending the rows to bigquery, so that
      # buffering time is not taken into account. Gives a field in bigquery
      # which represents the insert time of the row.
      config_param :add_insert_timestamp, :string, default: nil

      config_param :method, :enum, list: [:insert, :load], default: :insert, skip_accessor: true

      # allow_retry_insert_errors (only insert)
      # If insert_id_field is not specified, true means to allow duplicate rows
      config_param :allow_retry_insert_errors, :bool, default: false

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

        @tablelist = @tables ? @tables : [@table]

        legacy_schema_config_deprecation
        @table_schema = Fluent::BigQuery::RecordSchema.new('record')
        if @schema
          @table_schema.load_schema(@schema)
        end
        if @schema_path
          @table_schema.load_schema(MultiJson.load(File.read(@schema_path)))
        end

        types = %i(string integer float boolean timestamp)
        types.each do |type|
          fields = instance_variable_get("@field_#{type}")
          next unless fields
          fields.each do |field|
            @table_schema.register_field field, type
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

        if @insert_id_field
          insert_id_keys = @insert_id_field.split('.')
          @get_insert_id = ->(record) {
            insert_id_keys.inject(record) {|h, k| h[k] }
          }
        else
          @get_insert_id = nil
        end

        placeholder_params = "project=#{@project}/dataset=#{@dataset}/table=#{@tablelist.join(",")}/fetch_schema_table=#{@fetch_schema_table}/template_suffix=#{@template_suffix}"
        placeholder_validate!(:bigquery, placeholder_params)

        warn "[DEPRECATION] `convert_hash_to_json` param is deprecated. If Hash value is inserted string field, plugin convert it to json automatically." if @convert_hash_to_json
      end

      def start
        super

        @tables_queue = @tablelist.shuffle
        @tables_mutex = Mutex.new
        @fetched_schemas = {}
        @last_fetch_schema_time = Hash.new(0)
      end

      def writer
        @writer ||= Fluent::BigQuery::Writer.new(@log, @auth_method, {
          private_key_path: @private_key_path, private_key_passphrase: @private_key_passphrase,
          email: @email,
          json_key: @json_key,
          skip_invalid_rows: @skip_invalid_rows,
          ignore_unknown_values: @ignore_unknown_values,
          max_bad_records: @max_bad_records,
          allow_retry_insert_errors: @allow_retry_insert_errors,
          prevent_duplicate_load: @prevent_duplicate_load,
          auto_create_table: @auto_create_table,
          time_partitioning_type: @time_partitioning_type,
          time_partitioning_expiration: @time_partitioning_expiration,
          timeout_sec: @request_timeout_sec,
          open_timeout_sec: @request_open_timeout_sec,
        })
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

      def format(tag, time, record)
        if @replace_record_key
          record = replace_record_key(record)
        end

        if @convert_hash_to_json
          record = convert_hash_to_json(record)
        end

        record = inject_values_to_record(tag, time, record)

        begin
          meta = metadata(tag, time, record)
          schema =
            if @fetch_schema
              fetch_schema(meta)
            else
              @table_schema
            end
        ensure
          @buffer.metadata_list.delete(meta)
        end

        begin
          buf = String.new
          row = schema.format(record)
          unless row.empty?
            buf << MultiJson.dump(row) + "\n"
          end
          buf
        rescue
          log.error("format error", record: record, schema: schema)
          raise
        end
      end

      def write(chunk)
        table_id_format = @tables_mutex.synchronize do
          t = @tables_queue.shift
          @tables_queue.push t
          t
        end
        _write(chunk, table_id_format)
      end

      def legacy_schema_config_deprecation
        if [@field_string, @field_integer, @field_float, @field_boolean, @field_timestamp].any?
          warn "[DEPRECATION] `field_*` style schema config is deprecated. Instead of it, use `schema` config params that is array of json style."
        end
      end

      def fetch_schema(metadata)
        table_id = nil
        project = extract_placeholders(@project, metadata)
        dataset = extract_placeholders(@dataset, metadata)
        table_id = fetch_schema_target_table(metadata)

        if Fluent::Engine.now - @last_fetch_schema_time["#{project}.#{dataset}.#{table_id}"] > @schema_cache_expire
          schema = writer.fetch_schema(project, dataset, table_id)

          if schema
            table_schema = Fluent::BigQuery::RecordSchema.new("record")
            table_schema.load_schema(schema)
            @fetched_schemas["#{project}.#{dataset}.#{table_id}"] = table_schema
          else
            if @fetched_schemas["#{project}.#{dataset}.#{table_id}"].empty?
              raise "failed to fetch schema from bigquery"
            else
              log.warn "#{table_id} uses previous schema"
            end
          end

          @last_fetch_schema_time["#{project}.#{dataset}.#{table_id}"] = Fluent::Engine.now
        end

        @fetched_schemas["#{project}.#{dataset}.#{table_id}"]
      end

      def fetch_schema_target_table(metadata)
        extract_placeholders(@fetch_schema_table || @tablelist[0], metadata)
      end

      def get_schema(project, dataset, metadata)
        if @fetch_schema
          @fetched_schemas["#{project}.#{dataset}.#{fetch_schema_target_table(metadata)}"] || fetch_schema(metadata)
        else
          @table_schema
        end
      end

      module InsertImplementation
        def _write(chunk, table_format)
          now = Time.now.utc.strftime("%Y-%m-%d %H:%M:%S.%6N") if @add_insert_timestamp
          rows = chunk.open do |io|
            io.map do |line|
              record = MultiJson.load(line)
              record[@add_insert_timestamp] = now if @add_insert_timestamp
              row = {"json" => record}
              row["insert_id"] = @get_insert_id.call(record) if @get_insert_id
              Fluent::BigQuery::Helper.deep_symbolize_keys(row)
            end
          end

          project = extract_placeholders(@project, chunk.metadata)
          dataset = extract_placeholders(@dataset, chunk.metadata)
          table_id = extract_placeholders(table_format, chunk.metadata)
          template_suffix = @template_suffix ? extract_placeholders(@template_suffix, chunk.metadata) : nil

          schema = get_schema(project, dataset, chunk.metadata)

          insert(project, dataset, table_id, rows, schema, template_suffix)
        end

        def insert(project, dataset, table_id, rows, schema, template_suffix)
          writer.insert_rows(project, dataset, table_id, rows, template_suffix: template_suffix)
        rescue Fluent::BigQuery::Error => e
          if @auto_create_table && e.status_code == 404 && /Not Found: Table/i =~ e.message
            # Table Not Found: Auto Create Table
            writer.create_table(project, dataset, table_id, schema)
            raise "table created. send rows next time."
          end

          raise if e.retryable?

          if @secondary
            # TODO: find better way
            @retry = retry_state_create(
              :output_retries, @buffer_config.retry_type, @buffer_config.retry_wait, @buffer_config.retry_timeout,
              forever: false, max_steps: @buffer_config.retry_max_times, backoff_base: @buffer_config.retry_exponential_backoff_base,
              max_interval: @buffer_config.retry_max_interval,
              secondary: true, secondary_threshold: Float::EPSILON,
              randomize: @buffer_config.retry_randomize
            )
          else
            @retry = retry_state_create(
              :output_retries, @buffer_config.retry_type, @buffer_config.retry_wait, @buffer_config.retry_timeout,
              forever: false, max_steps: 0, backoff_base: @buffer_config.retry_exponential_backoff_base,
              max_interval: @buffer_config.retry_max_interval,
              randomize: @buffer_config.retry_randomize
            )
          end

          raise
        end
      end

      module LoadImplementation
        def _write(chunk, table_id_format)
          project = extract_placeholders(@project, chunk.metadata)
          dataset = extract_placeholders(@dataset, chunk.metadata)
          table_id = extract_placeholders(table_id_format, chunk.metadata)

          schema = get_schema(project, dataset, chunk.metadata)

          load(chunk, project, dataset, table_id, schema)
        end

        def load(chunk, project, dataset, table_id, schema)
          res = nil

          create_upload_source(chunk) do |upload_source|
            res = writer.create_load_job(chunk.unique_id, project, dataset, table_id, upload_source, schema)
          end
        rescue Fluent::BigQuery::Error => e
          raise if e.retryable?

          if @secondary
            # TODO: find better way
            @retry = retry_state_create(
              :output_retries, @buffer_config.retry_type, @buffer_config.retry_wait, @buffer_config.retry_timeout,
              forever: false, max_steps: @buffer_config.retry_max_times, backoff_base: @buffer_config.retry_exponential_backoff_base,
              max_interval: @buffer_config.retry_max_interval,
              secondary: true, secondary_threshold: Float::EPSILON,
              randomize: @buffer_config.retry_randomize
            )
          else
            @retry = retry_state_create(
              :output_retries, @buffer_config.retry_type, @buffer_config.retry_wait, @buffer_config.retry_timeout,
              forever: false, max_steps: 0, backoff_base: @buffer_config.retry_exponential_backoff_base,
              max_interval: @buffer_config.retry_max_interval,
              randomize: @buffer_config.retry_randomize
            )
          end

          raise
        end

        private

        def create_upload_source(chunk)
          chunk_is_file = @buffer_config["@type"] == 'file'
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
end

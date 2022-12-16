require 'fluent/plugin/output'

require 'fluent/plugin/bigquery/version'

require 'fluent/plugin/bigquery/helper'
require 'fluent/plugin/bigquery/errors'
require 'fluent/plugin/bigquery/schema'
require 'fluent/plugin/bigquery/writer'

require 'multi_json'
require 'google/apis/bigquery_v2'
require 'googleauth'

module Fluent
  module Plugin
    # This class is abstract class
    class BigQueryBaseOutput < Output
      helpers :inject, :formatter

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
      # The geographic location of the job. Required except for US and EU.
      # https://github.com/googleapis/google-api-ruby-client/blob/master/generated/google/apis/bigquery_v2/service.rb#L350
      config_param :location, :string, default: nil

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

      config_param :auto_create_table, :bool, default: false

      # ignore_unknown_values
      #   Accept rows that contain values that do not match the schema. The unknown values are ignored.
      #   Default is false, which treats unknown values as errors.
      config_param :ignore_unknown_values, :bool, default: false

      config_param :schema, :array, default: nil
      config_param :schema_path, :string, default: nil
      config_param :fetch_schema, :bool, default: false
      config_param :fetch_schema_table, :string, default: nil
      config_param :schema_cache_expire, :time, default: 600

      ## Timeout
      # request_timeout_sec
      #   Bigquery API response timeout
      # request_open_timeout_sec
      #   Bigquery API connection, and request timeout
      config_param :request_timeout_sec, :time, default: nil
      config_param :request_open_timeout_sec, :time, default: 60

      ## Partitioning
      config_param :time_partitioning_type, :enum, list: [:day], default: nil
      config_param :time_partitioning_field, :string, default: nil
      config_param :time_partitioning_expiration, :time, default: nil

      ## Clustering
      config_param :clustering_fields, :array, default: nil

      ## Formatter
      config_section :format do
        config_set_default :@type, 'json'
      end

      def configure(conf)
        super

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

        @table_schema = Fluent::BigQuery::RecordSchema.new('record')
        if @schema
          @table_schema.load_schema(@schema)
        end

        formatter_config = conf.elements("format")[0]
        @formatter = formatter_create(usage: 'out_bigquery_for_insert', default_type: 'json', conf: formatter_config)
      end

      def start
        super

        @tables_queue = @tablelist.shuffle
        @tables_mutex = Mutex.new
        @fetched_schemas = {}
        @last_fetch_schema_time = Hash.new(0)
        @read_schemas = {}
      end

      def multi_workers_ready?
        true
      end

      def writer
        @writer ||= Fluent::BigQuery::Writer.new(@log, @auth_method,
          private_key_path: @private_key_path, private_key_passphrase: @private_key_passphrase,
          email: @email,
          json_key: @json_key,
          location: @location,
          source_format: @source_format,
          skip_invalid_rows: @skip_invalid_rows,
          ignore_unknown_values: @ignore_unknown_values,
          max_bad_records: @max_bad_records,
          allow_retry_insert_errors: @allow_retry_insert_errors,
          prevent_duplicate_load: @prevent_duplicate_load,
          auto_create_table: @auto_create_table,
          time_partitioning_type: @time_partitioning_type,
          time_partitioning_field: @time_partitioning_field,
          time_partitioning_expiration: @time_partitioning_expiration,
          require_partition_filter: @require_partition_filter,
          clustering_fields: @clustering_fields,
          timeout_sec: @request_timeout_sec,
          open_timeout_sec: @request_open_timeout_sec,
        )
      end

      def format(tag, time, record)
        if record.nil?
          log.warn("nil record detected. corrupted chunks? tag=#{tag}, time=#{time}")
          return
        end

        record = inject_values_to_record(tag, time, record)

        meta = metadata(tag, time, record)
        schema =
          if @fetch_schema
            fetch_schema(meta)
          elsif @schema_path
            read_schema(meta)
          else
            @table_schema
          end

        begin
          row = schema.format(record, is_load: !!@is_load)
          return if row.empty?
          @formatter.format(tag, time, row)
        rescue
          log.error("format error", record: record, schema: schema)
          raise
        end
      end

      def write(chunk)
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
            if @fetched_schemas["#{project}.#{dataset}.#{table_id}"].nil?
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

      def read_schema(metadata)
        schema_path = read_schema_target_path(metadata)

        unless @read_schemas[schema_path]
          table_schema = Fluent::BigQuery::RecordSchema.new("record")
          table_schema.load_schema(MultiJson.load(File.read(schema_path)))
          @read_schemas[schema_path] = table_schema
        end
        @read_schemas[schema_path]
      end

      def read_schema_target_path(metadata)
        extract_placeholders(@schema_path, metadata)
      end

      def get_schema(project, dataset, metadata)
        if @fetch_schema
          @fetched_schemas["#{project}.#{dataset}.#{fetch_schema_target_table(metadata)}"] || fetch_schema(metadata)
        elsif @schema_path
          @read_schemas[read_schema_target_path(metadata)] || read_schema(metadata)
        else
          @table_schema
        end
      end
    end
  end
end

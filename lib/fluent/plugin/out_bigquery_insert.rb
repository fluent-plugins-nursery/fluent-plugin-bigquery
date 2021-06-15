require 'fluent/plugin/out_bigquery_base'

module Fluent
  module Plugin
    class BigQueryInsertOutput < BigQueryBaseOutput
      Fluent::Plugin.register_output('bigquery_insert', self)

      helpers :record_accessor

      # template_suffix (only insert)
      #   https://cloud.google.com/bigquery/streaming-data-into-bigquery#template_table_details
      config_param :template_suffix, :string, default: nil

      # skip_invalid_rows (only insert)
      #   Insert all valid rows of a request, even if invalid rows exist.
      #   The default value is false, which causes the entire request to fail if any invalid rows exist.
      config_param :skip_invalid_rows, :bool, default: false

      # insert_id_field (only insert)
      config_param :insert_id_field, :string, default: nil

      # add_insert_timestamp (only insert)
      # adds a timestamp just before sending the rows to bigquery, so that
      # buffering time is not taken into account. Gives a field in bigquery
      # which represents the insert time of the row.
      config_param :add_insert_timestamp, :string, default: nil

      # allow_retry_insert_errors (only insert)
      # If insert_id_field is not specified, true means to allow duplicate rows
      config_param :allow_retry_insert_errors, :bool, default: false

      ## RequirePartitionFilter
      config_param :require_partition_filter, :bool, default: false

      ## Buffer
      config_section :buffer do
        config_set_default :@type, "memory"
        config_set_default :flush_mode, :interval
        config_set_default :flush_interval, 1
        config_set_default :flush_thread_interval, 0.05
        config_set_default :flush_thread_burst_interval, 0.05
        config_set_default :chunk_limit_size, 1 * 1024 ** 2 # 1MB
        config_set_default :total_limit_size, 1 * 1024 ** 3 # 1GB
        config_set_default :chunk_limit_records, 500
      end

      def configure(conf)
        super

        if @insert_id_field
          if @insert_id_field !~ /^\$[\[\.]/ && @insert_id_field =~ /\./
            warn "[BREAKING CHANGE] insert_id_field format is changed. Use fluentd record_accessor helper. (https://docs.fluentd.org/v1.0/articles/api-plugin-helper-record_accessor)"
          end
          @get_insert_id = record_accessor_create(@insert_id_field)
        end

        formatter_config = conf.elements("format")[0]
        if formatter_config && formatter_config['@type'] != "json"
          raise ConfigError, "`bigquery_insert` supports only json formatter."
        end
        @formatter = formatter_create(usage: 'out_bigquery_for_insert', type: 'json', conf: formatter_config)

        placeholder_params = "project=#{@project}/dataset=#{@dataset}/table=#{@tablelist.join(",")}/fetch_schema_table=#{@fetch_schema_table}/template_suffix=#{@template_suffix}"
        placeholder_validate!(:bigquery_insert, placeholder_params)
      end

      # for Fluent::Plugin::Output#implement? method
      def format(tag, time, record)
        super
      end

      def write(chunk)
        table_format = @tables_mutex.synchronize do
          t = @tables_queue.shift
          @tables_queue.push t
          t
        end

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

        metadata = chunk.metadata
        project = extract_placeholders(@project, metadata)
        dataset = extract_placeholders(@dataset, metadata)
        table_id = extract_placeholders(table_format, metadata)
        template_suffix = @template_suffix ? extract_placeholders(@template_suffix, metadata) : nil
        schema = get_schema(project, dataset, metadata)

        insert(project, dataset, table_id, rows, schema, template_suffix)
      rescue MultiJson::ParseError => e
        raise Fluent::UnrecoverableError.new(e)
      end

      def insert(project, dataset, table_id, rows, schema, template_suffix)
        writer.insert_rows(project, dataset, table_id, rows, schema, template_suffix: template_suffix)
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
    end
  end
end

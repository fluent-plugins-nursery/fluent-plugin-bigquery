require 'fluent/plugin/out_bigquery_base'

module Fluent
  module Plugin
    class BigQueryLoadOutput < BigQueryBaseOutput
      Fluent::Plugin.register_output('bigquery_load', self)

      ### default for loads
      def configure_for_load(conf)
        raise ConfigError unless conf["method"] == "load"

        formatter_config = conf.elements("format")[0]
        @formatter = formatter_create(usage: 'out_bigquery_for_load', conf: formatter_config, default_type: 'json')

        buffer_config = conf.elements("buffer")[0]
        return unless buffer_config
        buffer_config["@type"]                       = "file"         unless buffer_config["@type"]
        buffer_config["flush_mode"]                  = :interval      unless buffer_config["flush_mode"]
        buffer_config["chunk_limit_size"]            = 1 * 1024 ** 3  unless buffer_config["chunk_limit_size"] # 1GB
        buffer_config["total_limit_size"]            = 32 * 1024 ** 3 unless buffer_config["total_limit_size"] # 32GB
      end

      config_param :source_format, :enum, list: [:json, :avro, :csv], default: :json

      # max_bad_records (only load)
      #   The maximum number of bad records that BigQuery can ignore when running the job.
      #   If the number of bad records exceeds this value, an invalid error is returned in the job result.
      #   The default value is 0, which requires that all records are valid.
      config_param :max_bad_records, :integer, default: 0

      # prevent_duplicate_load (only load)
      config_param :prevent_duplicate_load, :bool, default: false

      ## Buffer
      config_section :buffer do
        config_set_default :@type, "file"
        config_set_default :flush_mode, :interval
        config_set_default :flush_interval, 3600 # 1h
        config_set_default :flush_thread_interval, 5
        config_set_default :flush_thread_burst_interval, 5
        config_set_default :chunk_limit_size, 1 * 1024 ** 3 # 1GB
        config_set_default :total_limit_size, 32 * 1024 ** 3 # 32GB
      end

      def configure(conf)
        super

        placeholder_params = "project=#{@project}/dataset=#{@dataset}/table=#{@tablelist.join(",")}/fetch_schema_table=#{@fetch_schema_table}"
        placeholder_validate!(:bigquery_load, placeholder_params)
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

        metadata = chunk.metadata
        project = extract_placeholders(@project, metadata)
        dataset = extract_placeholders(@dataset, metadata)
        table_id = extract_placeholders(table_format, metadata)
        schema = get_schema(project, dataset, metadata)

        create_load_job(chunk, project, dataset, table_id, schema)
      end

      def create_load_job(chunk, project, dataset, table_id, schema)
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

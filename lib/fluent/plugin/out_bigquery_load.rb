require 'fluent/plugin/out_bigquery_base'

module Fluent
  module Plugin
    class BigQueryLoadOutput < BigQueryBaseOutput
      Fluent::Plugin.register_output('bigquery_load', self)

      helpers :timer

      config_param :source_format, :enum, list: [:json, :avro, :csv], default: :json

      # max_bad_records (only load)
      #   The maximum number of bad records that BigQuery can ignore when running the job.
      #   If the number of bad records exceeds this value, an invalid error is returned in the job result.
      #   The default value is 0, which requires that all records are valid.
      config_param :max_bad_records, :integer, default: 0

      # prevent_duplicate_load (only load)
      config_param :prevent_duplicate_load, :bool, default: false

      config_param :use_delayed_commit, :bool, default: true
      config_param :wait_job_interval, :time, default: 3

      ## Buffer
      config_section :buffer do
        config_set_default :@type, "file"
        config_set_default :flush_mode, :interval
        config_set_default :flush_interval, 3600 # 1h
        config_set_default :flush_thread_interval, 5
        config_set_default :flush_thread_burst_interval, 5
        config_set_default :chunk_limit_size, 1 * 1024 ** 3 # 1GB
        config_set_default :total_limit_size, 32 * 1024 ** 3 # 32GB

        config_set_default :delayed_commit_timeout, 1800 # 30m
      end

      def configure(conf)
        super
        @is_load = true

        placeholder_params = "project=#{@project}/dataset=#{@dataset}/table=#{@tablelist.join(",")}/fetch_schema_table=#{@fetch_schema_table}"
        placeholder_validate!(:bigquery_load, placeholder_params)
      end

      def start
        super

        if prefer_delayed_commit
          @polling_targets = []
          @polling_mutex = Mutex.new
          log.debug("start load job polling")
          timer_execute(:polling_bigquery_load_job, @wait_job_interval, &method(:poll))
        end
      end

      def prefer_delayed_commit
        @use_delayed_commit
      end

      # for Fluent::Plugin::Output#implement? method
      def format(tag, time, record)
        super
      end

      def write(chunk)
        job_reference = do_write(chunk)

        until response = writer.fetch_load_job(job_reference)
          sleep @wait_job_interval
        end

        writer.commit_load_job(job_reference.chunk_id_hex, response)
      rescue Fluent::BigQuery::Error => e
        raise if e.retryable?

        @retry_mutex.synchronize do
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
        end

        raise
      end

      def try_write(chunk)
        job_reference = do_write(chunk)
        @polling_mutex.synchronize do
          @polling_targets << job_reference
        end
      rescue Fluent::BigQuery::Error => e
        raise if e.retryable?

        @retry_mutex.synchronize do
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
        end

        raise
      end

      private

      def do_write(chunk)
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

        create_upload_source(chunk) do |upload_source|
          writer.create_load_job(chunk.unique_id, dump_unique_id_hex(chunk.unique_id), project, dataset, table_id, upload_source, schema)
        end
      end

      def poll
        job_reference = @polling_mutex.synchronize do
          @polling_targets.shift
        end
        return unless job_reference

        begin
          response = writer.fetch_load_job(job_reference)
          if response
            writer.commit_load_job(job_reference.chunk_id_hex, response)
            commit_write(job_reference.chunk_id)
            log.debug("commit chunk", chunk: job_reference.chunk_id_hex, **job_reference.as_hash(:job_id, :project_id, :dataset_id, :table_id))
          else
            @polling_mutex.synchronize do
              @polling_targets << job_reference
            end
          end
        rescue Fluent::BigQuery::Error => e
          # RetryableError comes from only `commit_load_job`
          # if error is retryable, takeback chunk and do next `try_flush`
          # if error is not retryable, create custom retry_state and takeback chunk do next `try_flush`
          if e.retryable?
            log.warn("failed to poll load job", error: e, chunk: job_reference.chunk_id_hex, **job_reference.as_hash(:job_id, :project_id, :dataset_id, :table_id))
          else
            log.error("failed to poll load job", error: e, chunk: job_reference.chunk_id_hex, **job_reference.as_hash(:job_id, :project_id, :dataset_id, :table_id))
            @retry_mutex.synchronize do
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
            end
          end

          rollback_write(job_reference.chunk_id)
        rescue => e
          log.error("unexpected error while polling", error: e)
          log.error_backtrace
          rollback_write(job_reference.chunk_id)
        end
      end

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

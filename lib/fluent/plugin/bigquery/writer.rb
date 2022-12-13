module Fluent
  module BigQuery
    class Writer
      def initialize(log, auth_method, **options)
        @auth_method = auth_method
        @scope = "https://www.googleapis.com/auth/bigquery"
        @options = options
        @log = log
        @num_errors_per_chunk = {}
      end

      def client
        @client ||= Google::Apis::BigqueryV2::BigqueryService.new.tap do |cl|
          cl.authorization = get_auth
          cl.client_options.open_timeout_sec = @options[:open_timeout_sec] if @options[:open_timeout_sec]
          cl.client_options.read_timeout_sec = @options[:timeout_sec] if @options[:timeout_sec]
          cl.client_options.send_timeout_sec = @options[:timeout_sec] if @options[:timeout_sec]
        end
      end

      def create_table(project, dataset, table_id, record_schema)
        create_table_retry_limit = 3
        create_table_retry_wait = 1
        create_table_retry_count = 0
        table_id = safe_table_id(table_id)

        begin
          definition = {
            table_reference: {
              table_id: table_id,
            },
            schema: {
              fields: record_schema.to_a,
            }
          }

          definition.merge!(time_partitioning: time_partitioning) if time_partitioning
          definition.merge!(require_partition_filter: require_partition_filter) if require_partition_filter
          definition.merge!(clustering: clustering) if clustering
          client.insert_table(project, dataset, definition, **{})
          log.debug "create table", project_id: project, dataset: dataset, table: table_id
        rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
          message = e.message
          if e.status_code == 409 && /Already Exists:/ =~ message
            log.debug "already created table", project_id: project, dataset: dataset, table: table_id
            # ignore 'Already Exists' error
            return
          end

          log.error "tables.insert API", project_id: project, dataset: dataset, table: table_id, code: e.status_code, message: message

          if create_table_retry_count < create_table_retry_limit
            sleep create_table_retry_wait
            create_table_retry_wait *= 2
            create_table_retry_count += 1
            retry
          else
            raise Fluent::BigQuery::UnRetryableError.new("failed to create table in bigquery", e)
          end
        end
      end

      def fetch_schema(project, dataset, table_id)
        res = client.get_table(project, dataset, table_id)
        schema = Fluent::BigQuery::Helper.deep_stringify_keys(res.schema.to_h[:fields])
        log.debug "Load schema from BigQuery: #{project}:#{dataset}.#{table_id} #{schema}"

        schema
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        message = e.message
        log.error "tables.get API", project_id: project, dataset: dataset, table: table_id, code: e.status_code, message: message
        nil
      end

      def insert_rows(project, dataset, table_id, rows, schema, template_suffix: nil)
        body = {
          rows: rows,
          skip_invalid_rows: @options[:skip_invalid_rows],
          ignore_unknown_values: @options[:ignore_unknown_values],
        }
        body.merge!(template_suffix: template_suffix) if template_suffix

        if @options[:auto_create_table]
          res = insert_all_table_data_with_create_table(project, dataset, table_id, body, schema)
        else
          res = client.insert_all_table_data(project, dataset, table_id, body, **{})
        end
        log.debug "insert rows", project_id: project, dataset: dataset, table: table_id, count: rows.size

        if res.insert_errors && !res.insert_errors.empty?
          log.warn "insert errors", project_id: project, dataset: dataset, table: table_id, insert_errors: res.insert_errors.to_s
          if @options[:allow_retry_insert_errors]
            is_included_any_retryable_insert_error = res.insert_errors.any? do |insert_error|
              insert_error.errors.any? { |error| Fluent::BigQuery::Error.retryable_insert_errors_reason?(error.reason) }
            end
            if is_included_any_retryable_insert_error
              raise Fluent::BigQuery::RetryableError.new("failed to insert into bigquery(insert errors), retry")
            else
              raise Fluent::BigQuery::UnRetryableError.new("failed to insert into bigquery(insert errors), and cannot retry")
            end
          end
        end
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        error_data = { project_id: project, dataset: dataset, table: table_id, code: e.status_code, message: e.message }
        wrapped = Fluent::BigQuery::Error.wrap(e)
        if wrapped.retryable?
          log.warn "tabledata.insertAll API", error_data
        else
          log.error "tabledata.insertAll API", error_data
        end

        raise wrapped
      end

      JobReference = Struct.new(:chunk_id, :chunk_id_hex, :project_id, :dataset_id, :table_id, :job_id) do
        def as_hash(*keys)
          if keys.empty?
            to_h
          else
            to_h.select { |k, _| keys.include?(k) }
          end
        end
      end

      def create_load_job(chunk_id, chunk_id_hex, project, dataset, table_id, upload_source, fields)
        configuration = {
          configuration: {
            load: {
              destination_table: {
                project_id: project,
                dataset_id: dataset,
                table_id: table_id,
              },
              write_disposition: "WRITE_APPEND",
              source_format: source_format,
              ignore_unknown_values: @options[:ignore_unknown_values],
              max_bad_records: @options[:max_bad_records],
            }
          }
        }

        job_id = create_job_id(chunk_id_hex, dataset, table_id, fields.to_a) if @options[:prevent_duplicate_load]
        configuration.merge!({job_reference: {project_id: project, job_id: job_id}}) if job_id

        begin
          # Check table existance
          client.get_table(project, dataset, table_id)
        rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
          if e.status_code == 404 && /Not Found: Table/i =~ e.message
            raise Fluent::BigQuery::UnRetryableError.new("Table is not found") unless @options[:auto_create_table]
            raise Fluent::BigQuery::UnRetryableError.new("Schema is empty") if fields.empty?
            configuration[:configuration][:load].merge!(schema: {fields: fields.to_a})
            configuration[:configuration][:load].merge!(time_partitioning: time_partitioning) if time_partitioning
            configuration[:configuration][:load].merge!(clustering: clustering) if clustering
          end
        end

        res = client.insert_job(
          project,
          configuration,
          upload_source: upload_source,
          content_type: "application/octet-stream",
        )
        JobReference.new(chunk_id, chunk_id_hex, project, dataset, table_id, res.job_reference.job_id)
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        log.error "job.load API", project_id: project, dataset: dataset, table: table_id, code: e.status_code, message: e.message

        if job_id && e.status_code == 409 && e.message =~ /Job/ # duplicate load job
          return JobReference.new(chunk_id, chunk_id_hex, project, dataset, table_id, job_id)
        end

        raise Fluent::BigQuery::Error.wrap(e)
      end

      def fetch_load_job(job_reference)
        project = job_reference.project_id
        job_id = job_reference.job_id
        location = @options[:location]

        res = client.get_job(project, job_id, location: location)
        log.debug "load job fetched", id: job_id, state: res.status.state, **job_reference.as_hash(:project_id, :dataset_id, :table_id)

        if res.status.state == "DONE"
          res
        end
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        e = Fluent::BigQuery::Error.wrap(e) 
        raise e unless e.retryable?
      end

      def commit_load_job(chunk_id_hex, response)
        job_id = response.id
        project = response.configuration.load.destination_table.project_id
        dataset = response.configuration.load.destination_table.dataset_id
        table_id = response.configuration.load.destination_table.table_id

        errors = response.status.errors
        if errors
          errors.each do |e|
            log.error "job.load API (rows)", job_id: job_id, project_id: project, dataset: dataset, table: table_id, message: e.message, reason: e.reason
          end
        end

        error_result = response.status.error_result
        if error_result
          log.error "job.load API (result)", job_id: job_id, project_id: project, dataset: dataset, table: table_id, message: error_result.message, reason: error_result.reason
          if Fluent::BigQuery::Error.retryable_error_reason?(error_result.reason)
            @num_errors_per_chunk[chunk_id_hex] = @num_errors_per_chunk[chunk_id_hex].to_i + 1
            raise Fluent::BigQuery::RetryableError.new("failed to load into bigquery, retry")
          else
            @num_errors_per_chunk.delete(chunk_id_hex)
            raise Fluent::BigQuery::UnRetryableError.new("failed to load into bigquery, and cannot retry")
          end
        end

        # `stats` can be nil if we receive a warning like "Warning: Load job succeeded with data imported, however statistics may be lost due to internal error."
        stats = response.statistics.load
        duration = (response.statistics.end_time - response.statistics.creation_time) / 1000.0
        log.debug "load job finished", id: job_id, state: response.status.state, input_file_bytes: stats&.input_file_bytes, input_files: stats&.input_files, output_bytes: stats&.output_bytes, output_rows: stats&.output_rows, bad_records: stats&.bad_records, duration: duration.round(2), project_id: project, dataset: dataset, table: table_id
        @num_errors_per_chunk.delete(chunk_id_hex)
      end

      private

      def log
        @log
      end

      def get_auth
        case @auth_method
        when :private_key
          get_auth_from_private_key
        when :compute_engine
          get_auth_from_compute_engine
        when :json_key
          get_auth_from_json_key
        when :application_default
          get_auth_from_application_default
        else
          raise ConfigError, "Unknown auth method: #{@auth_method}"
        end
      end

      def get_auth_from_private_key
        require 'google/api_client/auth/key_utils'
        private_key_path = @options[:private_key_path]
        private_key_passphrase = @options[:private_key_passphrase]
        email = @options[:email]

        key = Google::APIClient::KeyUtils.load_from_pkcs12(private_key_path, private_key_passphrase)
        Signet::OAuth2::Client.new(
          token_credential_uri: "https://accounts.google.com/o/oauth2/token",
          audience: "https://accounts.google.com/o/oauth2/token",
          scope: @scope,
          issuer: email,
          signing_key: key
        )
      end

      def get_auth_from_compute_engine
        Google::Auth::GCECredentials.new
      end

      def get_auth_from_json_key
        json_key = @options[:json_key]

        begin
          JSON.parse(json_key)
          key = StringIO.new(json_key)
          Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: key, scope: @scope)
        rescue JSON::ParserError
          key = json_key
          File.open(json_key) do |f|
            Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: f, scope: @scope)
          end
        end
      end

      def get_auth_from_application_default
        Google::Auth.get_application_default([@scope])
      end

      def safe_table_id(table_id)
        table_id.gsub(/\$\d+$/, "")
      end

      def create_job_id(chunk_id_hex, dataset, table, schema)
        job_id_key = "#{chunk_id_hex}#{dataset}#{table}#{schema.to_s}#{@options[:max_bad_records]}#{@options[:ignore_unknown_values]}#{@num_errors_per_chunk[chunk_id_hex]}"
        @log.debug "job_id_key: #{job_id_key}"
        "fluentd_job_" + Digest::SHA1.hexdigest(job_id_key)
      end

      def source_format
        case @options[:source_format]
        when :json
          "NEWLINE_DELIMITED_JSON"
        when :avro
          "AVRO"
        when :csv
          "CSV"
        else
          "NEWLINE_DELIMITED_JSON"
        end
      end

      def time_partitioning
        return @time_partitioning if instance_variable_defined?(:@time_partitioning)

        if @options[:time_partitioning_type]
          @time_partitioning = {
            type: @options[:time_partitioning_type].to_s.upcase,
            field: @options[:time_partitioning_field] ? @options[:time_partitioning_field].to_s : nil,
            expiration_ms: @options[:time_partitioning_expiration] ? @options[:time_partitioning_expiration] * 1000 : nil,
          }.reject { |_, v| v.nil? }
        else
          @time_partitioning
        end
      end

      def require_partition_filter
        return @require_partition_filter if instance_variable_defined?(:@require_partition_filter)

        if @options[:require_partition_filter]
          @require_partition_filter = @options[:require_partition_filter]
        else
          @require_partition_filter
        end
      end

      def clustering
        return @clustering if instance_variable_defined?(:@clustering)

        if @options[:clustering_fields]
          @clustering = {
            fields: @options[:clustering_fields]
          }
        else
          @clustering
        end
      end

      def insert_all_table_data_with_create_table(project, dataset, table_id, body, schema)
        try_count ||= 1
        res = client.insert_all_table_data(project, dataset, table_id, body, **{})
      rescue Google::Apis::ClientError => e
        if e.status_code == 404 && /Not Found: Table/i =~ e.message
          if try_count == 1
            # Table Not Found: Auto Create Table
            create_table(project, dataset, table_id, schema)
          elsif try_count > 60 # timeout in about 300 seconds
            raise "A new table was created but it is not found."
          end

          # Retry to insert several times because the created table is not visible from Streaming insert for a little while
          # cf. https://cloud.google.com/bigquery/troubleshooting-errors#metadata-errors-for-streaming-inserts
          try_count += 1
          sleep 5
          log.debug "Retry to insert rows", project_id: project, dataset: dataset, table: table_id
          retry
        end
        raise
      end
    end
  end
end

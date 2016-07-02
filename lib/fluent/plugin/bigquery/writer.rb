module Fluent
  module BigQuery
    class Writer
      RETRYABLE_ERROR_REASON = %w(backendError internalError rateLimitExceeded tableUnavailable).freeze

      class Error < StandardError
        attr_reader :origin

        def initialize(message, origin = nil)
          @origin = origin
          super(message || origin.message)
        end

        def method_missing(name, *args)
          if @origin
            @origin.send(name, *args)
          else
            super
          end
        end

        def reason
          @origin && @origin.respond_to?(:reason) ? @origin.reason : nil
        end

        def status_code
          @origin && @origin.respond_to?(:status_code) ? @origin.status_code : nil
        end

        def retryable?
          false
        end
      end

      class UnRetryableError < Error; end

      class RetryableError < Error
        def retryable?
          true
        end
      end

      def initialize(log, auth_method, auth_options = {})
        @auth_method = auth_method
        @scope = "https://www.googleapis.com/auth/bigquery"
        @auth_options = auth_options
        @log = log

        @cached_client_expiration = Time.now + 1800
      end

      def client
        return @client if @client && @cached_client_expiration > Time.now

        client = Google::Apis::BigqueryV2::BigqueryService.new.tap do |cl|
          cl.authorization = get_auth
        end

        @cached_client_expiration = Time.now + 1800
        @client = client
      end

      def create_table(project, dataset, table_id, record_schema)
        create_table_retry_limit = 3
        create_table_retry_wait = 1
        create_table_retry_count = 0

        begin
          client.insert_table(project, dataset, {
            table_reference: {
              table_id: table_id,
            },
            schema: {
              fields: record_schema.to_a,
            }
          }, {})
          log.debug "create table", project_id: project, dataset: dataset, table: table_id
          @client = nil
        rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
          @client = nil

          message = e.message
          if e.status_code == 409 && /Already Exists:/ =~ message
            log.debug "already created table", project_id: project, dataset: dataset, table: table_id
            # ignore 'Already Exists' error
            return
          end

          reason = e.respond_to?(:reason) ? e.reason : nil
          log.error "tables.insert API", project_id: project, dataset: dataset, table: table_id, code: e.status_code, message: message, reason: reason

          if RETRYABLE_ERROR_REASON.include?(reason) && create_table_retry_count < create_table_retry_limit
            sleep create_table_retry_wait
            create_table_retry_wait *= 2
            create_table_retry_count += 1
            retry
          else
            raise UnRetryableError.new("failed to create table in bigquery", e)
          end
        end
      end

      def fetch_schema(project, dataset, table_id)
        res = client.get_table(project, dataset, table_id)
        schema = res.schema.fields.as_json
        log.debug "Load schema from BigQuery: #{project}:#{dataset}.#{table_id} #{schema}"

        schema
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        @client = nil
        message = e.message
        log.error "tables.get API", project_id: project, dataset: dataset, table: table_id, code: e.status_code, message: message
        nil
      end

      def insert_rows(project, dataset, table_id, rows, skip_invalid_rows: false, ignore_unknown_values: false, template_suffix: nil, timeout_sec: nil, open_timeout_sec: 60)
        body = {
          rows: rows,
          skip_invalid_rows: skip_invalid_rows,
          ignore_unknown_values: ignore_unknown_values,
        }
        body.merge!(template_suffix: template_suffix) if template_suffix
        client.insert_all_table_data(project, dataset, table_id, body, {
          options: {timeout_sec: timeout_sec, open_timeout_sec: open_timeout_sec}
        })
        log.debug "insert rows", project_id: project, dataset: dataset, table: table_id, count: rows.size
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        @client = nil

        reason = e.respond_to?(:reason) ? e.reason : nil
        log.error "tabledata.insertAll API", project_id: project, dataset: dataset, table: table_id, code: e.status_code, message: e.message, reason: reason

        if RETRYABLE_ERROR_REASON.include?(reason)
          raise RetryableError.new(nil, e)
        else
          raise UnRetryableError.new(nil, e)
        end
      end

      def create_load_job(project, dataset, table_id, upload_source, job_id, fields, ignore_unknown_values: false, max_bad_records: 0, template_suffix: nil, timeout_sec: nil, open_timeout_sec: 60)
        configuration = {
          configuration: {
            load: {
              destination_table: {
                project_id: project,
                dataset_id: dataset,
                table_id: "#{table_id}#{template_suffix}",
              },
              schema: {
                fields: fields.to_a,
              },
              write_disposition: "WRITE_APPEND",
              source_format: "NEWLINE_DELIMITED_JSON",
              ignore_unknown_values: ignore_unknown_values,
              max_bad_records: max_bad_records,
            }
          }
        }
        configuration.merge!({job_reference: {project_id: project, job_id: job_id}}) if job_id

        # If target table is already exist, omit schema configuration.
        # Because schema changing is easier.
        begin
          if template_suffix && client.get_table(project, dataset, "#{table_id}#{template_suffix}")
            configuration[:configuration][:load].delete(:schema)
          end
        rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError
          raise UnRetryableError.new("Schema is empty") if fields.empty?
        end

        res = client.insert_job(
          project,
          configuration,
          {
            upload_source: upload_source,
            content_type: "application/octet-stream",
            options: {
              timeout_sec: timeout_sec,
              open_timeout_sec: open_timeout_sec,
            }
          }
        )
        wait_load_job(project, dataset, res.job_reference.job_id, table_id)
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        @client = nil

        reason = e.respond_to?(:reason) ? e.reason : nil
        log.error "job.load API", project_id: project, dataset: dataset, table: table_id, code: e.status_code, message: e.message, reason: reason

        return wait_load_job(project, dataset, job_id, table_id) if job_id && e.status_code == 409 && e.message =~ /Job/ # duplicate load job

        if RETRYABLE_ERROR_REASON.include?(reason) || e.is_a?(Google::Apis::ServerError)
          raise RetryableError.new(nil, e)
        else
          raise UnRetryableError.new(nil, e)
        end
      end

      def wait_load_job(project, dataset, job_id, table_id)
        wait_interval = 10
        _response = client.get_job(project, job_id)

        until _response.status.state == "DONE"
          log.debug "wait for load job finish", state: _response.status.state, job_id: _response.job_reference.job_id
          sleep wait_interval
          _response = client.get_job(project, _response.job_reference.job_id)
        end

        errors = _response.status.errors
        if errors
          errors.each do |e|
            log.error "job.insert API (rows)", job_id: job_id, project_id: project, dataset: dataset, table: table_id, message: e.message, reason: e.reason
          end
        end

        error_result = _response.status.error_result
        if error_result
          log.error "job.insert API (result)", job_id: job_id, project_id: project, dataset: dataset, table: table_id, message: error_result.message, reason: error_result.reason
          if RETRYABLE_ERROR_REASON.include?(error_result.reason)
            raise RetryableError.new("failed to load into bigquery, retry")
          else
            raise UnRetryableError.new("failed to load into bigquery, and cannot retry")
          end
        end

        log.debug "finish load job", state: _response.status.state
      end

      private

      def log
        @log
      end

      def get_auth
        case @auth_method
        when 'private_key'
          get_auth_from_private_key
        when 'compute_engine'
          get_auth_from_compute_engine
        when 'json_key'
          get_auth_from_json_key
        when 'application_default'
          get_auth_from_application_default
        else
          raise ConfigError, "Unknown auth method: #{@auth_method}"
        end
      end

      def get_auth_from_private_key
        require 'google/api_client/auth/key_utils'
        private_key_path = @auth_options[:private_key_path]
        private_key_passphrase = @auth_options[:private_key_passphrase]
        email = @auth_options[:email]

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
        json_key = @auth_options[:json_key]

        if File.exist?(json_key)
          File.open(json_key) do |f|
            Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: f, scope: @scope)
          end
        else
          key = StringIO.new(json_key)
          Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: key, scope: @scope)
        end
      end

      def get_auth_from_application_default
        Google::Auth.get_application_default([@scope])
      end
    end
  end
end

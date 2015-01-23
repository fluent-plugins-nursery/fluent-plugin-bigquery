require 'json'
require 'google/api_client'
require 'google/api_client/client_secrets'
require 'google/api_client/auth/installed_app'
require 'google/api_client/auth/compute_service_account'

module Fluent::BigQueryPlugin
  class BigQueryClient
    def initialize(attributes = {})
      attributes.each { |name, value| instance_variable_set("@#{name}", value) }
    end

    def create_table(name, schema)
      result =
        access_api(
          api_method: bigquery.tables.insert,
          body_object: {
            'tableReference' => {
               'tableId' => name
            },
            'schema' => {
              'fields' => schema
            }
          }
        )
      handle_error(result) if result.error?
    end

    def insert(table, rows)
      result =
        access_api(
          api_method: bigquery.tabledata.insert_all,
          parameters: {
            'tableId' => table
          },
          body_object: {
            'rows' => rows
          }
        )
      handle_error(result) if result.error?
    end

    def fetch_schema(table)
      result =
        access_api(
          api_method: bigquery.tables.get,
          parameters: {
            'tableId' => table
          }
        )
      handle_error(result) if result.error?
      JSON.parse(result.body)['schema']['fields']
    end

    def load
      # https://developers.google.com/bigquery/loading-data-into-bigquery#loaddatapostrequest
      raise NotImplementedError # TODO
    end

    private

    def access_api(params = {})
      params[:parameters] ||= {}
      params[:parameters]['projectId'] ||= @project
      params[:parameters]['datasetId'] ||= @dataset
      client.execute(params)
    end

    def bigquery
      # TODO: refresh with specified expiration
      @bigquery ||= client.discovered_api('bigquery', 'v2')
    end

    def handle_error(result)
      @client = nil # clear cashed client when errors occur
      error =
        case result.status
        when 404      then NotFound
        when 409      then Conflict
        when 400..499 then ClientError
        when 500..599 then ServerError
        else UnexpectedError
        end
      fail error, result.error_message
    end

    def client
      @client = nil if expired?
      unless @client
        @client = Google::APIClient.new(
          application_name: 'Fluentd BigQuery plugin',
          application_version: Fluent::BigQueryPlugin::VERSION
        )
        authorize_client
        @expiration = Time.now + 1800
      end
      @client
    end

    def expired?
      @expiration && @expiration < Time.now
    end

    def authorize_client
      case @auth_method
      when 'private_key'
        asserter =
          Google::APIClient::JWTAsserter.new(
            @email,
            'https://www.googleapis.com/auth/bigquery',
            Google::APIClient::PKCS12.load_key(@private_key_path, @private_key_passphrase)
          )
        @client.authorization = asserter.authorize
      when 'compute_engine'
        auth = Google::APIClient::ComputeServiceAccount.new
        auth.fetch_access_token!
        @client.authorization = auth
      end
    end

    # def client_oauth # not implemented
    #   raise NotImplementedError, "OAuth needs browser authentication..."
    #
    #   client = Google::APIClient.new(
    #     application_name: 'Example Ruby application',
    #     application_version: '1.0.0'
    #   )
    #   bigquery = client.discovered_api('bigquery', 'v2')
    #   flow = Google::APIClient::InstalledAppFlow.new(
    #     client_id: @client_id
    #     client_secret: @client_secret
    #     scope: ['https://www.googleapis.com/auth/bigquery']
    #   )
    #   client.authorization = flow.authorize # browser authentication !
    #   client
    # end
  end
end
